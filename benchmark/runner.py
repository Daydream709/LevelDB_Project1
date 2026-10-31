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
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
BASE_COMMIT = 'f1f7084b51f033ba5b2225203a0e215126d8142b'
PROFILES = {
    'quick': dict(keys=10000, threads=4, rounds=4, repeat=1),
    'standard': dict(keys=100000, threads=8, rounds=8, repeat=3),
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

def summarize(records, out):
    rows = []
    for r in records:
        for m in r.get('metrics', []):
            rows.append(dict(case=r['case'], seed=r['seed'], keys=r['keys'], threads=r['threads'],
                             repeat=r['repeat'], expired_percent=r['expired_percent'], **m))
    if rows:
        with (out/'metrics.csv').open('w') as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
    groups = {}
    for row in rows:
        key = (row['case'], row['phase'], row['keys'], row['threads'], row['expired_percent'])
        groups.setdefault(key, []).append(row)
    aggregate = []
    for key, vals in sorted(groups.items()):
        aggregate.append(dict(case=key[0], phase=key[1], keys=key[2], threads=key[3], expired_percent=key[4],
                              repeats=len(vals),
                              median_ops_per_second=statistics.median(x['ops_per_second'] for x in vals),
                              min_ops_per_second=min(x['ops_per_second'] for x in vals),
                              max_ops_per_second=max(x['ops_per_second'] for x in vals),
                              median_p99_us=statistics.median(x['p99_us'] for x in vals)))
    dump(out/'aggregate.json', aggregate)

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
                          'perf_combined_stats_under_write')
    threads = sorted({1, cfg['threads']}) if concurrent else [1]
    ttl_mix = name in ('perf_ttl_mixed', 'perf_combined_stats_under_write',
                       'perf_combined_gc')
    ratios = [0, 50, 100] if ttl_mix else [expired_percent]
    return [(n, t, expiry) for n in sizes for t in threads for expiry in ratios]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--kind', choices=['combined', 'ttl', 'metadata', 'baseline'], required=True,
                    help='combined runs both feature suites and their integration scenarios')
    ap.add_argument('--source', type=Path, required=True)
    ap.add_argument('--output', type=Path, required=True)
    ap.add_argument('--profile', choices=PROFILES, default='quick')
    ap.add_argument('--keys', type=int); ap.add_argument('--threads', type=int)
    ap.add_argument('--rounds', type=int); ap.add_argument('--repeat', type=int)
    ap.add_argument('--value-bytes', type=int, default=256)
    ap.add_argument('--seed', type=int, default=7774596)
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
    ap.add_argument('--matrix', action='store_true', help='performance at N/10,N and 1,T threads where applicable; TTL expiry 0,50,100')
    args = ap.parse_args()
    cfg = PROFILES[args.profile].copy()
    for key in cfg:
        if getattr(args,key) is not None: cfg[key] = getattr(args,key)
    if args.sanitize and args.memory_mib: ap.error('ASan reserves a large virtual address space; do not combine with --memory-mib')
    if min(cfg.values()) < 1 or args.value_bytes < 16 or args.memory_mib < 0 or not 0 <= args.expired_percent <= 100 or min(args.timeout,args.build_timeout,args.jobs)<1:
        ap.error('positive sizes/timeouts/jobs required; value-bytes >=16; expiry in [0,100]')
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
    summary=dict(schema_version=1, kind=args.kind, profile=args.profile, config=cfg,
                 value_bytes=args.value_bytes, seed=args.seed, source=str(src), records=records,
                 status='running', suite_scope='teacher' if args.teacher_suite else 'public',
                 selected_cases=args.cases, regression_requested=not args.skip_regression, sanitize=args.sanitize,
                 memory_limit_mib=args.memory_mib, performance_scoring='ungraded; calibration only')
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
        dump(out/'provenance.json',dict(source_files=hashes(out/'source'),suite_files=hashes(suite),
            teacher_files=hashes(teacher) if teacher else {}, regression_files=hashes(out/'regression-source'),
            baseline_commit=BASE_COMMIT, platform=platform.platform(), python=sys.version,
            cpu_count=os.cpu_count(),cpu_info=Path('/proc/cpuinfo').read_text().splitlines()[:30],
            memory_info=Path('/proc/meminfo').read_text(),
            compiler=subprocess.check_output(['c++','--version'],text=True),
            cmake=subprocess.check_output(['cmake','--version'],text=True),
            submodules=json.dumps(package_info()['dependencies'],sort_keys=True)))
        build=out/'build'
        configure=['cmake','-S',suite,'-B',build,'-DCMAKE_BUILD_TYPE=Release',
                   f'-DHW_SOURCE={out/"source"}',f'-DHW_KIND={args.kind}',
                   f'-DHW_REGRESSION_SOURCE={out/"regression-source"}']
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
                for repeat in range(cfg['repeat']):
                    label=f'{name}-n{n}-t{t}-e{expiry}-r{repeat+1}'
                    work=out/label;work.mkdir()
                    cmd=[binary,'--case',name,'--db',work/'db','--keys',n,'--threads',t,
                         '--rounds',cfg['rounds'],'--value-bytes',args.value_bytes,
                         '--seed',args.seed+repeat,'--expired-percent',expiry]
                    print(label,flush=True)
                    test_env=os.environ.copy()
                    if args.sanitize:
                        test_env.update(ASAN_OPTIONS='detect_leaks=0:halt_on_error=1', UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1')
                    rec=run(cmd,work/'run.log',args.timeout,cwd=work,env=test_env,memory_mib=args.memory_mib)
                    rec.update(case=name,repeat=repeat+1,seed=args.seed+repeat,keys=n,threads=t,expired_percent=expiry)
                    payloads=[line[len('HW_RESULT '):] for line in (work/'run.log').read_text(errors='replace').splitlines() if line.startswith('HW_RESULT ')]
                    if rec['status']=='pass':
                        if len(payloads)!=1:rec.update(status='protocol_failure',error='missing or duplicate final result')
                        else:
                            data=json.loads(payloads[0])
                            if data.get('case')!=name or data.get('status')!='pass':rec.update(status='protocol_failure',error='invalid final result')
                            else:rec.update(data)
                    elif payloads:
                        data=json.loads(payloads[-1]);rec['error']=data.get('error','case failed')
                    records.append(rec);dump(out/'summary.json',summary)
                    # Only our own disposable, successfully validated DB is removed.
                    if rec['status']=='pass' and (work/'db').exists():shutil.rmtree(work/'db')
        if not args.skip_regression:
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
        summarize(records,out);dump(out/'summary.json',summary)
        print(json.dumps(dict(status=summary['status'],output=str(out),passed=sum(r['status']=='pass' for r in records),cases=len(records))),flush=True)

if __name__=='__main__':sys.exit(main())
