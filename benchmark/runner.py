#!/usr/bin/env python3
"""Build immutable source copies and run each workload in a fresh process/database."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
BASE_COMMIT = 'f1f7084b51f033ba5b2225203a0e215126d8142b'
PROFILES = {
    'quick': dict(keys=10000, threads=4, rounds=4, repeat=1),
    'standard': dict(keys=100000, threads=8, rounds=8, repeat=3),
    'research': dict(keys=1000000, threads=8, rounds=8, repeat=5),
}

def dump(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')

def run(command, log, timeout, cwd=None, env=None, memory_mib=0):
    start = time.monotonic()
    with log.open('w') as out:
        out.write('COMMAND ' + json.dumps([str(x) for x in command]) + '\n')
        out.flush()
        p = subprocess.Popen([str(x) for x in command], stdout=out, stderr=subprocess.STDOUT,
                             start_new_session=True, cwd=cwd, env=env,
                             preexec_fn=(lambda: resource.setrlimit(resource.RLIMIT_AS, (memory_mib<<20, memory_mib<<20))) if memory_mib else None)
        try:
            code = p.wait(timeout=timeout)
            status = 'pass' if code == 0 else 'crash' if code < 0 else 'fail'
        except subprocess.TimeoutExpired:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            p.wait()
            code, status = p.returncode, 'timeout'
    return dict(status=status, returncode=code, seconds=time.monotonic()-start, log=str(log))

def probe_profiler(name, directory):
    if name == 'none':
        return dict(status='disabled', tool='none')
    executable = shutil.which('heaptrack' if name == 'heaptrack' else 'perf')
    if not executable:
        return dict(status='skipped', tool=name, reason='executable not found')
    version = subprocess.run([executable, '--version'], text=True,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             timeout=10)
    result = dict(status='available', tool=name, executable=executable,
                  version=version.stdout.splitlines()[0] if version.stdout else 'unknown')
    if name.startswith('perf-'):
        check = subprocess.run([executable, 'stat', '-e', 'task-clock', '--', 'true'],
                               text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               timeout=15)
        if check.returncode:
            result.update(status='skipped', reason='perf permission or event unavailable',
                          probe_output=check.stdout[-1000:])
            return result
        if name in ('perf-record', 'perf-sched'):
            with tempfile.TemporaryDirectory(prefix='metadata-profiler-probe-', dir=directory) as temp:
                if name == 'perf-record':
                    command = [executable,'record','-F','99','-g','--call-graph','fp',
                               '-o',str(Path(temp)/'probe.data'),'--','true']
                else:
                    command = [executable,'sched','record','-o',str(Path(temp)/'probe.data'),
                               '--','true']
                check = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, timeout=15)
            if check.returncode:
                result.update(status='skipped', reason='perf recording mode unavailable',
                              probe_output=check.stdout[-1000:])
    elif name == 'heaptrack':
        with tempfile.TemporaryDirectory(prefix='metadata-profiler-probe-', dir=directory) as temp:
            check = subprocess.run([executable,'-o',str(Path(temp)/'probe'),'--','true'],
                                   text=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, timeout=15)
        if check.returncode:
            result.update(status='skipped', reason='heaptrack could not instrument a process',
                          probe_output=check.stdout[-1000:])
    return result

def profiled_command(name, executable, command, work):
    if name == 'none': return command, None
    if name == 'perf-stat':
        output = work/'perf-stat.csv'
        return [executable,'stat','-x,','-o',output,'--',*command], output
    if name == 'perf-record':
        output = work/'perf.data'
        return [executable,'record','-F','199','-g','--call-graph','fp','-o',output,'--',*command], output
    if name == 'perf-sched':
        output = work/'perf-sched.data'
        return [executable,'sched','record','-o',output,'--',*command], output
    output = work/'heaptrack'
    return [executable,'-o',output,'--',*command], output

def collect_profile_report(name, executable, artifact, work, timeout):
    if artifact is None or name in ('none','perf-stat'):
        return None
    if name == 'perf-record':
        command = [executable,'report','--stdio','-i',artifact]
        report = work/'perf-report.txt'
    elif name == 'perf-sched':
        command = [executable,'sched','latency','-i',artifact]
        report = work/'perf-sched-latency.txt'
    else:
        files = sorted(work.glob('heaptrack*'))
        data = next((p for p in files if p.suffix in ('.gz','.zst')), None)
        if data is None: return None
        command = [executable,'--analyze',data]
        report = work/'heaptrack-report.txt'
    result = run(command, work/(report.name+'.log'), timeout)
    if result['status'] == 'pass':
        shutil.copyfile(result['log'], report)
        return str(report)
    return dict(status=result['status'], log=result['log'])

def write_report(out, summary, aggregate):
    lines = ['# Metadata research benchmark report', '',
             f"- Status: `{summary.get('status')}`",
             f"- Source: `{summary.get('source')}`",
             f"- Profile: `{summary.get('profile')}`",
             f"- External profiler: `{summary.get('profiler', {}).get('status', 'disabled')}`",
             '', '## Workload results', '',
             '| Workload | Phase | Keys | Threads | Median ops/s | Median p99 µs | Median drain s |',
             '|---|---|---:|---:|---:|---:|---:|']
    for row in aggregate:
        lines.append('| {case} | {phase} | {keys} | {threads} | {rate:.2f} | {p99} | {drain:.4f} |'.format(
            case=row['case'], phase=row['phase'], keys=row['keys'], threads=row['threads'],
            rate=row['median_ops_per_second'], p99='—' if row['median_p99_us'] is None else f"{row['median_p99_us']:.3f}",
            drain=row['median_drain_seconds']))
    lines += ['', '## Engine counters and metadata I/O', '',
              '| Workload | Phase | Metadata lookups | Stats calls | Batch spill bytes | Change-file bytes | Metadata SST write bytes (phase / drain) | Metadata memory MiB after drain |',
              '|---|---|---:|---:|---:|---:|---:|---:|']
    def cell(row, name):
        value = row.get(name)
        return '—' if value is None else str(int(value))
    for row in aggregate:
        lines.append('| {case} | {phase} | {lookups} | {stats} | {spill} | {change} | {phase_write} / {drain_write} | {memory} |'.format(
            case=row['case'], phase=row['phase'],
            lookups=cell(row, 'median_profile_counter_metadata_lookups'),
            stats=cell(row, 'median_profile_counter_stats_queries'),
            spill=cell(row, 'median_profile_counter_batch_spill_bytes'),
            change=cell(row, 'median_profile_counter_change_file_bytes'),
            phase_write=cell(row, 'median_metadata_sst_write_bytes'),
            drain_write=cell(row, 'median_drain_metadata_sst_write_bytes'),
            memory=('—' if row.get('median_profile_resource_after_drain_metadata_memory_estimate_bytes') is None
                    else f"{row['median_profile_resource_after_drain_metadata_memory_estimate_bytes'] / (1 << 20):.2f}")))
    lines += ['', 'Detailed per-repeat metrics, classified phase/drain I/O, engine counters, and profiler outputs are in `metrics.csv`, `aggregate.json`, and each case directory.', '']
    (out/'report.md').write_text('\n'.join(lines))

def hashes(path):
    return {str(p.relative_to(path)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(path.rglob('*')) if p.is_file() and not p.is_symlink()}

def copy_source(source, dest):
    ignored = {'.git', 'build', 'out', 'third_party', 'doc', '.github', 'teacher-notes',
               'benchmark', '.worktrees', 'docs', '__pycache__'}
    shutil.copytree(source, dest, ignore=lambda _, names: [n for n in names if n in ignored])
    (dest/'third_party').mkdir(exist_ok=True)
    for dep in ('googletest', 'benchmark'):
        target = ROOT/'third_party'/dep
        if not (target/'CMakeLists.txt').exists():
            raise RuntimeError('Missing bundled dependencies; extract the complete course package')
        (dest/'third_party'/dep).symlink_to(target, target_is_directory=True)
    crc_source = source/'third_party'/'crc32c'
    if not (crc_source/'CMakeLists.txt').is_file():
        crc_source = ROOT/'third_party'/'crc32c'
    if (crc_source/'CMakeLists.txt').is_file():
        shutil.copytree(crc_source, dest/'third_party'/'crc32c')

def package_info():
    info = json.loads((HERE/'package.json').read_text())
    if info['baseline_commit'] != BASE_COMMIT:
        raise RuntimeError('Packaged regression baseline does not match runner')
    return info

def regression_sources(dest):
    source = HERE/'regression-source'
    if hashes(source) != package_info()['regression_files']:
        raise RuntimeError('Packaged regression sources are missing or modified')
    shutil.copytree(source, dest)

def profile_delta(before, after):
    if not after:
        return None
    b = json.loads(after) if isinstance(after, str) else after
    a = (json.loads(before) if isinstance(before, str) else before) if before else {}
    counters = {name: value - a.get('counters', {}).get(name, 0)
                for name, value in b.get('counters', {}).items()}
    stages = {}
    for name, end in b.get('stages', {}).items():
        start = a.get('stages', {}).get(name, {})
        stages[name] = {field: end.get(field, 0) - start.get(field, 0)
                        for field in ('events', 'samples', 'sampled_total_ns')}
        end_hist = end.get('histogram_ns_upper_bound', [])
        start_hist = start.get('histogram_ns_upper_bound', [])
        histogram = [max(0, value - (start_hist[i] if i < len(start_hist) else 0))
                     for i, value in enumerate(end_hist)]
        stages[name]['histogram_bucket_counts'] = histogram
        sample_count = sum(histogram)

        def quantile_upper_bound(percentile):
            if sample_count == 0:
                return None
            rank = int(percentile * (sample_count - 1))
            seen = 0
            for bucket, count in enumerate(histogram):
                seen += count
                if seen > rank:
                    return ((1 << (bucket + 1)) - 1
                            if bucket < 31 else (1 << 64) - 1)
            return (1 << 64) - 1

        for percentile in (0.50, 0.95, 0.99):
            suffix = int(percentile * 100)
            stages[name]['p%d_ns_upper_bound' % suffix] = \
                quantile_upper_bound(percentile)
        max_bucket = next((i for i in range(len(histogram) - 1, -1, -1)
                           if histogram[i]), None)
        stages[name]['max_ns_upper_bound'] = (
            ((1 << (max_bucket + 1)) - 1 if max_bucket < 31 else (1 << 64) - 1)
            if max_bucket is not None else None)
    return dict(counters=counters, stages=stages)

def profile_fields(delta):
    """Flatten engine counters and stage timing totals for CSV aggregation."""
    fields = {}
    if not delta:
        return fields
    for name, value in delta.get('counters', {}).items():
        fields['profile_counter_' + name] = value
    for name, values in delta.get('stages', {}).items():
        for field in ('events', 'samples', 'sampled_total_ns',
                      'p50_ns_upper_bound', 'p95_ns_upper_bound',
                      'p99_ns_upper_bound', 'max_ns_upper_bound'):
            fields['profile_stage_' + name + '_' + field] = values.get(field, 0)
    return fields

def profile_resource_fields(snapshot, when):
    if not snapshot:
        return {}
    data = json.loads(snapshot) if isinstance(snapshot, str) else snapshot
    return {'profile_resource_' + when + '_' + name: value
            for name, value in data.get('resources', {}).items()}

def summarize(records, out):
    rows = []
    for r in records:
        for m in r.get('metrics', []):
            row = dict(case=r['case'], seed=r['seed'], keys=r['keys'], threads=r['threads'],
                       repeat=r['repeat'], expired_percent=r['expired_percent'],
                       profiler_requested=r.get('profiler_requested', 'none'),
                       profiler_status=r.get('profiler_status', 'disabled'),
                       peak_rss_kib=r.get('peak_rss_kib'),
                       **r.get('parameters', {}))
            row.update({k:v for k,v in m.items() if k not in ('io','drain_io','metadata_profile_before',
                       'metadata_profile_at_end','metadata_profile_after_drain')})
            row['metadata_profile_delta'] = profile_delta(
                m.get('metadata_profile_before'), m.get('metadata_profile_at_end'))
            row['metadata_drain_profile_delta'] = profile_delta(
                m.get('metadata_profile_at_end'), m.get('metadata_profile_after_drain'))
            row.update(profile_fields(row['metadata_profile_delta']))
            row.update({'drain_' + k: v for k, v in
                        profile_fields(row['metadata_drain_profile_delta']).items()})
            for when, key in (('at_end', 'metadata_profile_at_end'),
                              ('after_drain', 'metadata_profile_after_drain')):
                row.update(profile_resource_fields(m.get(key), when))
            for prefix, io in (('', m.get('io', {})),
                               ('drain_', m.get('drain_io', {}))):
                for category, values in io.items():
                    for key, value in values.items(): row[prefix + category + '_' + key] = value
            if row['metadata_profile_delta'] is not None:
                row['metadata_profile_delta'] = json.dumps(row['metadata_profile_delta'], sort_keys=True)
            if row['metadata_drain_profile_delta'] is not None:
                row['metadata_drain_profile_delta'] = json.dumps(row['metadata_drain_profile_delta'], sort_keys=True)
            rows.append(row)
    if rows:
        with (out/'metrics.csv').open('w') as f:
            fields = list(dict.fromkeys(k for row in rows for k in row))
            w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)
    groups = {}
    for row in rows:
        key = (row['case'], row['phase'], row['keys'], row['threads'],
               row['expired_percent'], row['profiler_requested'],
               row.get('key_bytes'), row.get('batch_size'), row.get('distinct_percent'),
               row.get('distribution'), row.get('metadata_memtable_bytes'),
               row.get('metadata_cache_bytes'), row.get('metadata_workspace_bytes'),
               row.get('write_buffer_bytes'), row.get('query_threads'),
               row.get('warmup_seconds'), row.get('duration_seconds'),
               row.get('profile_sample_rate'), row.get('sync_writes'),
               row.get('metadata_wal_bytes'),
               row.get('metadata_checkpoint_interval_micros'))
        groups.setdefault(key, []).append(row)
    aggregate = []
    for key, vals in sorted(groups.items()):
        p99 = [x['p99_us'] for x in vals if x['p99_us'] is not None]
        median = statistics.median(x['ops_per_second'] for x in vals)
        row = dict(case=key[0], phase=key[1], keys=key[2], threads=key[3],
                   expired_percent=key[4], profiler_requested=key[5], repeats=len(vals),
                   median_ops_per_second=median,
                   min_ops_per_second=min(x['ops_per_second'] for x in vals),
                   max_ops_per_second=max(x['ops_per_second'] for x in vals),
                   ops_per_second_stdev=statistics.stdev(x['ops_per_second'] for x in vals) if len(vals)>1 else None,
                   ops_per_second_cv=(statistics.stdev(x['ops_per_second'] for x in vals)/median if len(vals)>1 and median else None),
                   median_p50_us=statistics.median(x['p50_us'] for x in vals if x['p50_us'] is not None) if any(x['p50_us'] is not None for x in vals) else None,
                   median_p95_us=statistics.median(x['p95_us'] for x in vals if x['p95_us'] is not None) if any(x['p95_us'] is not None for x in vals) else None,
                   median_p99_us=statistics.median(p99) if p99 else None,
                   median_max_us=statistics.median(x['max_us'] for x in vals if x['max_us'] is not None) if any(x['max_us'] is not None for x in vals) else None,
                   median_drain_seconds=statistics.median(x['drain_seconds'] for x in vals),
                   median_peak_rss_kib=statistics.median(x['peak_rss_kib'] for x in vals
                                                        if x.get('peak_rss_kib') is not None)
                   if any(x.get('peak_rss_kib') is not None for x in vals) else None)
        for field in sorted({name for value in vals for name in value
                             if name.startswith('profile_counter_') or
                             name.startswith('profile_stage_') or
                             name.startswith('profile_resource_') or
                             name.startswith('drain_profile_') or
                             any(name.startswith(category + '_') for category in
                                 ('primary_sst', 'metadata_sst', 'metadata_change',
                                  'wal', 'manifest', 'other')) or
                             any(name.startswith('drain_' + category + '_') for category in
                                 ('primary_sst', 'metadata_sst', 'metadata_change',
                                  'wal', 'manifest', 'other'))}):
            samples = [value[field] for value in vals if isinstance(value.get(field), (int, float))]
            if samples:
                row['median_' + field] = statistics.median(samples)
        for field, value in zip(('key_bytes','batch_size','distinct_percent','distribution',
                                 'metadata_memtable_bytes','metadata_cache_bytes',
                                 'metadata_workspace_bytes','write_buffer_bytes','query_threads',
                                 'warmup_seconds','duration_seconds','profile_sample_rate',
                                 'sync_writes','metadata_wal_bytes',
                                 'metadata_checkpoint_interval_micros'),key[6:]):
            if value is not None: row[field] = value
        aggregate.append(row)
    dump(out/'aggregate.json', aggregate)
    return aggregate

def select_cases(available, requested):
    names = requested.split(',') if requested is not None else [
        name for name in available if not name.endswith('_writer')]
    if (not names or len(set(names)) != len(names) or
            any(name not in available or name.endswith('_writer') for name in names)):
        raise ValueError('Unknown, duplicate, or child-only case selected')
    return names


def case_variants(name, cfg, matrix, expired_percent):
    if not matrix or not name.startswith('perf_'):
        return [(cfg['keys'], cfg['threads'], expired_percent)]
    sizes = sorted({max(1, cfg['keys'] // 10), cfg['keys']})
    concurrent = name in ('perf_concurrent', 'perf_stats_under_write',
                          'perf_combined_stats_under_write',
                          'perf_metadata_contention', 'perf_metadata_steady')
    threads = sorted({1, cfg['threads']}) if concurrent else [1]
    ttl_mix = name in ('perf_ttl_mixed', 'perf_combined_stats_under_write',
                       'perf_combined_gc')
    ratios = [0, 50, 100] if ttl_mix else [expired_percent]
    return [(n, t, expiry) for n in sizes for t in threads for expiry in ratios]

def research_parameter_variants(name, base, matrix):
    if not matrix or not name.startswith('perf_metadata_'):
        return [base]
    dimensions = []
    if name == 'perf_metadata_lookup':
        dimensions = [('distribution', ['sequential','uniform','hotspot','zipf']),
                      ('metadata_cache_bytes', [64<<10,1<<20,8<<20]),
                      ('distinct_percent', [10,100])]
    elif name == 'perf_metadata_batch':
        dimensions = [('batch_size', [1,16,256,4096,65536]),
                      ('distinct_percent', [10,100]),
                      ('distribution', ['sequential','zipf'])]
    elif name in ('perf_metadata_contention','perf_metadata_steady'):
        dimensions = [('query_threads', [0,1,4,16])]
        if name == 'perf_metadata_steady':
            dimensions += [('metadata_wal_bytes', [64<<10,16<<20]),
                           ('metadata_checkpoint_interval_micros',
                            [1000,100000,1000000])]
    elif name == 'perf_metadata_maintenance':
        dimensions = [('metadata_memtable_bytes', [64<<10,1<<20,8<<20]),
                      ('metadata_checkpoint_interval_micros',
                       [1000,100000,1000000])]
    elif name == 'perf_metadata_recovery':
        dimensions = [('metadata_workspace_bytes', [64<<10,1<<20,4<<20])]
    variants = [dict(base)]
    for key, values in dimensions:
        for value in values:
            if base.get(key) == value: continue
            item = dict(base); item[key] = value; variants.append(item)
    return variants

def workload_parameters(args, research=False):
    defaults = dict(key_bytes=16, batch_size=64, query_threads=1,
                    distinct_percent=100, distribution='sequential',
                    metadata_memtable_bytes=1 << 20,
                    metadata_cache_bytes=8 << 20,
                    metadata_workspace_bytes=4 << 20,
                    metadata_wal_bytes=16 << 20,
                    metadata_checkpoint_interval_micros=1000000,
                    write_buffer_bytes=256 << 10, warmup_seconds=1,
                    duration_seconds=3, profile_sample_rate=64,
                    sync_writes=args.sync_writes)
    for key in defaults:
        value = getattr(args, key, None)
        if value is not None:
            defaults[key] = value
    return defaults if research else {}

def append_workload_parameters(command, parameters, research):
    if not research:
        return
    names = {
        'key_bytes': '--key-bytes', 'batch_size': '--batch-size',
        'query_threads': '--query-threads',
        'distinct_percent': '--distinct-percent',
        'distribution': '--distribution',
        'metadata_memtable_bytes': '--metadata-memtable-bytes',
        'metadata_wal_bytes': '--metadata-wal-bytes',
        'metadata_checkpoint_interval_micros': '--metadata-checkpoint-interval-micros',
        'metadata_cache_bytes': '--metadata-cache-bytes',
        'metadata_workspace_bytes': '--metadata-workspace-bytes',
        'write_buffer_bytes': '--write-buffer-bytes',
        'warmup_seconds': '--warmup-seconds',
        'duration_seconds': '--duration-seconds',
    }
    for key, flag in names.items():
        command.extend([flag, str(parameters[key])])
    command.append('--sync-writes' if parameters['sync_writes'] else '--async-writes')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--kind', choices=['combined', 'ttl', 'metadata', 'metadata-research', 'baseline'], required=True,
                    help='combined runs both feature suites and their integration scenarios')
    ap.add_argument('--source', type=Path, required=True)
    ap.add_argument('--output', type=Path, required=True)
    ap.add_argument('--profile', choices=PROFILES, default='quick')
    ap.add_argument('--keys', type=int); ap.add_argument('--threads', type=int)
    ap.add_argument('--rounds', type=int); ap.add_argument('--repeat', type=int)
    ap.add_argument('--value-bytes', type=int, default=256)
    ap.add_argument('--seed', type=int, default=7774596)
    ap.add_argument('--key-bytes', type=int)
    ap.add_argument('--batch-size', type=int)
    ap.add_argument('--query-threads', type=int)
    ap.add_argument('--distinct-percent', type=int)
    ap.add_argument('--distribution', choices=['sequential','uniform','hotspot','zipf'])
    ap.add_argument('--metadata-memtable-bytes', type=int)
    ap.add_argument('--metadata-wal-bytes', type=int)
    ap.add_argument('--metadata-checkpoint-interval-micros', type=int)
    ap.add_argument('--metadata-cache-bytes', type=int)
    ap.add_argument('--metadata-workspace-bytes', type=int)
    ap.add_argument('--write-buffer-bytes', type=int)
    ap.add_argument('--warmup-seconds', type=int)
    ap.add_argument('--duration-seconds', type=int)
    ap.add_argument('--profile-sample-rate', type=int, default=64)
    ap.add_argument('--profile-metadata', action=argparse.BooleanOptionalAction, default=None)
    ap.add_argument('--sync-writes', action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument('--profiler', choices=['none','perf-stat','perf-record','perf-sched','heaptrack'], default='none')
    ap.add_argument('--require-profiler', action='store_true')
    ap.add_argument('--expired-percent', type=int, default=50)
    ap.add_argument('--timeout', type=int, default=300, help='per workload seconds; conservative operational limit, not a score')
    ap.add_argument('--build-timeout', type=int, default=300)
    ap.add_argument('--jobs', type=int, default=4)
    ap.add_argument('--teacher-suite', type=Path)
    ap.add_argument('--adapter', type=Path, help='reviewed storage adapter header for a different internal layout')
    ap.add_argument('--cases', help='comma-separated exact case names; default all except subprocess helpers')
    ap.add_argument('--skip-regression', action='store_true')
    ap.add_argument('--memory-mib', type=int, default=0, help='optional workload virtual address-space limit; 0 disables')
    ap.add_argument('--sanitize', action='store_true', help='ASan/UBSan diagnostic build; not comparable to Release timing')
    ap.add_argument('--profiling', action='store_true', help='export per-case profiling JSON/JSONL')
    ap.add_argument('--profiling-sample-interval', type=int, default=1)
    ap.add_argument('--matrix', action='store_true', help='performance at N/10,N and 1,T threads where applicable; TTL expiry 0,50,100')
    args = ap.parse_args()
    cfg = PROFILES[args.profile].copy()
    for key in cfg:
        if getattr(args,key) is not None: cfg[key] = getattr(args,key)
    if args.profiling_sample_interval < 1: ap.error('profiling sample interval must be positive')
    if args.sanitize and args.memory_mib: ap.error('ASan reserves a large virtual address space; do not combine with --memory-mib')
    if min(cfg.values()) < 1 or args.value_bytes < 16 or args.memory_mib < 0 or not 0 <= args.expired_percent <= 100 or min(args.timeout,args.build_timeout,args.jobs)<1:
        ap.error('positive sizes/timeouts/jobs required; value-bytes >=16; expiry in [0,100]')
    if args.require_profiler and args.profiler == 'none':
        ap.error('--require-profiler requires a profiler selection')
    for name in ('key_bytes','batch_size','metadata_memtable_bytes','metadata_cache_bytes',
                 'metadata_workspace_bytes','write_buffer_bytes','profile_sample_rate',
                 'metadata_wal_bytes','metadata_checkpoint_interval_micros',
                 'duration_seconds','warmup_seconds'):
        value = getattr(args, name)
        if value is not None and value < (0 if name == 'warmup_seconds' else 1):
            ap.error('--' + name.replace('_','-') + ' is out of range')
    for name in ('metadata_memtable_bytes','metadata_cache_bytes',
                 'metadata_workspace_bytes','write_buffer_bytes','metadata_wal_bytes'):
        value = getattr(args, name)
        if value is not None and not (64 << 10) <= value <= (1 << 30):
            ap.error('--' + name.replace('_','-') + ' must be between 64 KiB and 1 GiB')
    if args.metadata_checkpoint_interval_micros is not None and not (
            1000 <= args.metadata_checkpoint_interval_micros <= 60_000_000):
        ap.error('--metadata-checkpoint-interval-micros must be between 1000 and 60000000')
    if args.profile_sample_rate > (1 << 20):
        ap.error('--profile-sample-rate must not exceed 1048576')
    if args.query_threads is not None and args.query_threads < 0:
        ap.error('--query-threads must be non-negative')
    if args.distinct_percent is not None and not 1 <= args.distinct_percent <= 100:
        ap.error('--distinct-percent must be in [1,100]')
    src = args.source.resolve(); out = args.output.resolve()
    if not (src/'CMakeLists.txt').is_file(): ap.error('source must be a LevelDB engine directory')
    if out.exists(): ap.error('output must be a new directory (existing results are never overwritten)')
    if out.is_relative_to(src) and not (src == ROOT and out.is_relative_to(ROOT/'build')):
        ap.error('output must be outside the submitted source tree (repository root may use build/)')
    ancestor=out.parent
    while not ancestor.exists():ancestor=ancestor.parent
    free = shutil.disk_usage(ancestor).free
    estimate = cfg['keys'] * (args.value_bytes+128) * 12 + (512<<20)
    if free < estimate: ap.error(f'insufficient free space; conservative estimate {estimate} bytes')
    out.mkdir(parents=True)
    records=[]
    metadata_profile_enabled = (args.profile_metadata if args.profile_metadata is not None
                                else args.kind == 'metadata-research')
    run_regression = not args.skip_regression and args.kind != 'metadata-research'
    summary=dict(schema_version=2, kind=args.kind, profile=args.profile, config=cfg,
                 value_bytes=args.value_bytes, seed=args.seed, source=str(src), records=records,
                 status='running', suite_scope='teacher' if args.teacher_suite else 'public',
                 selected_cases=args.cases, regression_requested=run_regression, sanitize=args.sanitize,
                 memory_limit_mib=args.memory_mib, performance_scoring='ungraded; calibration only',
                 metadata_profiling=dict(enabled=metadata_profile_enabled,
                                         sample_rate=args.profile_sample_rate),
                 profiler=dict(status='pending', tool=args.profiler))
    dump(out/'summary.json',summary)
    try:
        copy_source(src,out/'source')
        regression_sources(out/'regression-source')
        suite = out/'suite'; shutil.copytree(HERE,suite,ignore=shutil.ignore_patterns('__pycache__'))
        adapter = None
        if args.adapter:
            adapter=suite/'common/selected_adapter.h';shutil.copy2(args.adapter,adapter)
        teacher = None
        if args.teacher_suite:
            teacher=out/'teacher-suite';shutil.copytree(args.teacher_suite.resolve(),teacher,ignore=shutil.ignore_patterns('__pycache__'))
        profiler = probe_profiler(args.profiler, out)
        summary['profiler'] = profiler
        if args.require_profiler and profiler['status'] != 'available':
            summary.update(status='profiler_unavailable', error=profiler.get('reason', 'profiler unavailable'))
            return 1
        dump(out/'provenance.json',dict(source_files=hashes(out/'source'),suite_files=hashes(suite),
            teacher_files=hashes(teacher) if teacher else {}, regression_files=hashes(out/'regression-source'),
            baseline_commit=BASE_COMMIT, platform=platform.platform(), python=sys.version,
            cpu_count=os.cpu_count(),cpu_info=Path('/proc/cpuinfo').read_text().splitlines()[:30],
            memory_info=Path('/proc/meminfo').read_text(),
            compiler=subprocess.check_output(['c++','--version'],text=True),
            cmake=subprocess.check_output(['cmake','--version'],text=True),
            submodules=json.dumps(package_info()['dependencies'],sort_keys=True),
            crc32c_source_commit='02e65f4fd3065d27b2e29324800ca6d04df16126',
            metadata_profiling=summary['metadata_profiling'], profiler=profiler))
        build=out/'build'
        build_type = 'RelWithDebInfo' if profiler['status'] == 'available' and args.profiler != 'none' else 'Release'
        configure=['cmake','-S',suite,'-B',build,f'-DCMAKE_BUILD_TYPE={build_type}',
                   f'-DHW_SOURCE={out/"source"}',f'-DHW_KIND={args.kind}',
                   f'-DHW_REGRESSION_SOURCE={out/"regression-source"}',
                   '-DLEVELDB_CRC32C_PROVIDER=bundled']
        if build_type == 'RelWithDebInfo':
            configure += ['-DCMAKE_CXX_FLAGS_RELWITHDEBINFO=-O2 -g -DNDEBUG -fno-omit-frame-pointer',
                          '-DCMAKE_C_FLAGS_RELWITHDEBINFO=-O2 -g -DNDEBUG -fno-omit-frame-pointer']
        if teacher:configure.append(f'-DHW_TEACHER_DIR={teacher}')
        if adapter:configure.append(f'-DHW_STORAGE_ADAPTER={adapter}')
        if args.sanitize:
            configure += ['-DCMAKE_CXX_FLAGS=-fsanitize=address,undefined -fno-omit-frame-pointer -fno-pie',
                          '-DCMAKE_C_FLAGS=-fsanitize=address,undefined -fno-omit-frame-pointer -fno-pie',
                          '-DCMAKE_EXE_LINKER_FLAGS=-fsanitize=address,undefined -no-pie']
        summary['configure']=run(configure,out/'configure.log',args.build_timeout)
        if summary['configure']['status']!='pass':
            summary['status']='configure_failure';return 1
        summary['build']=run(['cmake','--build',build,'--target','homework_bench','-j',args.jobs],out/'build.log',args.build_timeout)
        if summary['build']['status']!='pass':summary['status']='build_failure';return 1
        binary=build/'homework_bench'
        listing_env=os.environ.copy()
        if args.sanitize:
            listing_env.update(ASAN_OPTIONS='detect_leaks=0:halt_on_error=1', UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1')
        available=subprocess.check_output([binary,'--list'],text=True,env=listing_env,timeout=args.timeout).splitlines()
        names=select_cases(available,args.cases)
        summary['available_cases']=[x for x in available if not x.endswith('_writer')]
        summary['executed_cases']=names
        summary['full_suite']=set(names)==set(summary['available_cases'])
        dump(out/'summary.json',summary)
        for name in names:
            variants=case_variants(name,cfg,args.matrix,args.expired_percent)
            for n,t,expiry in variants:
                parameter_variants = research_parameter_variants(
                    name, workload_parameters(args, name.startswith('perf_metadata_')), args.matrix)
                for parameters in parameter_variants:
                    for repeat in range(cfg['repeat']):
                        param_hash = hashlib.sha1(json.dumps(
                            parameters, sort_keys=True).encode()).hexdigest()[:8] if parameters else 'default'
                        label=f'{name}-n{n}-t{t}-e{expiry}-p{param_hash}-r{repeat+1}'
                        work=out/label;work.mkdir()
                        cmd=[binary,'--case',name,'--db',work/'db','--keys',n,'--threads',t,
                             '--rounds',cfg['rounds'],'--value-bytes',args.value_bytes,
                             '--seed',args.seed+repeat,'--expired-percent',expiry]
                        if args.profiling:
                            cmd += ['--profiling', '1', '--profiling-output', work/'profiling',
                                    '--profiling-sample-interval', args.profiling_sample_interval]
                        is_research = name.startswith('perf_metadata_')
                        append_workload_parameters(cmd, parameters, is_research)
                        cmd.extend(['--profile-sample-rate', str(args.profile_sample_rate)])
                        if metadata_profile_enabled: cmd.append('--profile-metadata')
                        else: cmd.append('--no-profile-metadata')
                        print(label,flush=True)
                        test_env=os.environ.copy()
                        if args.sanitize:
                            test_env.update(ASAN_OPTIONS='detect_leaks=0:halt_on_error=1', UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1')
                        executable, artifact = None, None
                        profiled, artifact = (cmd, None)
                        if profiler['status'] == 'available' and args.profiler != 'none':
                            profiled, artifact = profiled_command(args.profiler, profiler['executable'], cmd, work)
                        rec=run(profiled,work/'run.log',args.timeout,cwd=work,env=test_env,memory_mib=args.memory_mib)
                        rec.update(case=name,repeat=repeat+1,seed=args.seed+repeat,keys=n,threads=t,
                                   expired_percent=expiry,parameters=parameters,
                                   profiler_requested=args.profiler,profiler_status=profiler['status'],
                                   profiler_artifact=str(artifact) if artifact else None)
                        payloads=[line[len('HW_RESULT '):] for line in (work/'run.log').read_text(errors='replace').splitlines() if line.startswith('HW_RESULT ')]
                        if rec['status']=='pass':
                            if len(payloads)!=1:rec.update(status='protocol_failure',error='missing or duplicate final result')
                            else:
                                data=json.loads(payloads[0])
                                if data.get('case')!=name or data.get('status')!='pass':rec.update(status='protocol_failure',error='invalid final result')
                                else:rec.update(data)
                        elif payloads:
                            data=json.loads(payloads[-1]);rec['error']=data.get('error','case failed')
                        if rec['status']=='pass' and artifact is not None:
                            report=collect_profile_report(args.profiler,profiler['executable'],artifact,work,args.timeout)
                            rec['profiler_report']=report
                        records.append(rec);dump(out/'summary.json',summary)
                        # Only our own disposable, successfully validated DB is removed.
                        if rec['status']=='pass' and (work/'db').exists():shutil.rmtree(work/'db')
        if run_regression:
            summary['regression_build']=run(['cmake','--build',build,'--target','homework_regression','homework_c_regression','homework_env_regression','-j',args.jobs],out/'regression-build.log',args.build_timeout)
            if summary['regression_build']['status']=='pass':
                env=os.environ.copy()
                if args.sanitize:env.update(ASAN_OPTIONS='detect_leaks=0:halt_on_error=1',UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1')
                env['TEST_TMPDIR']=str(out/'regression-data');Path(env['TEST_TMPDIR']).mkdir()
                reg_runs=[]
                for target in ('homework_regression','homework_c_regression','homework_env_regression'):
                    reg_runs.append(run([build/target,f'--gtest_output=xml:{out/(target+".xml")}'],out/(target+'.log'),max(300,args.timeout),cwd=out,env=env))
                summary['regression']=dict(status='pass' if all(x['status']=='pass' for x in reg_runs) else 'fail', runs=reg_runs)
            else:summary['regression']=dict(status='build_failure')
        summary['status']='pass' if all(r['status']=='pass' for r in records) and summary.get('regression',{}).get('status','pass')=='pass' else 'fail'
        return 0 if summary['status']=='pass' else 1
    except Exception as e:
        summary.update(status='runner_error',error=str(e));return 2
    finally:
        aggregate=summarize(records,out)
        if args.kind == 'metadata-research' and aggregate:
            write_report(out, summary, aggregate)
        dump(out/'summary.json',summary)
        print(json.dumps(dict(status=summary['status'],output=str(out),passed=sum(r['status']=='pass' for r in records),cases=len(records))),flush=True)

if __name__=='__main__':sys.exit(main())
