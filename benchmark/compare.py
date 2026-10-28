#!/usr/bin/env python3
"""Compare successful matched workloads; does not assign performance grades."""
import argparse
import json
from pathlib import Path

FIELDS=('value_bytes','seed','sanitize')
PARAMETERS=('key_bytes','batch_size','distinct_percent','distribution',
            'metadata_memtable_bytes','metadata_cache_bytes',
            'metadata_workspace_bytes','write_buffer_bytes','query_threads',
            'warmup_seconds','duration_seconds','profile_sample_rate','sync_writes',
            'metadata_wal_bytes','metadata_checkpoint_interval_micros')
def compare(base, candidate):
    b=json.loads((base/'summary.json').read_text());c=json.loads((candidate/'summary.json').read_text())
    if b['status']!='pass' or c['status']!='pass':raise ValueError('Only passing runs may be compared')
    if b['kind']=='metadata-research':
        if c['kind']!='metadata-research':
            raise ValueError('Metadata research runs can only be compared with each other')
    elif b['kind']!='baseline':
        raise ValueError('Baseline run must use --kind baseline or --kind metadata-research')
    elif c['kind'] not in ('combined','ttl','metadata'):
        raise ValueError('Candidate must use a feature-enabled benchmark kind')
    if any(b.get(k)!=c.get(k) for k in FIELDS):raise ValueError('Value sizes, seeds or sanitizer modes differ')
    if b.get('metadata_profiling', {}) != c.get('metadata_profiling', {}):
        raise ValueError('Engine metadata profiling settings differ')
    for summary in (b, c):
        profiler = summary.get('profiler', {})
        if profiler.get('tool') not in (None, 'none'):
            raise ValueError('Externally profiled runs are diagnostic and cannot be compared for timing')
    for key in ('platform','compiler','submodules'):
        bp=json.loads((base/'provenance.json').read_text());cp=json.loads((candidate/'provenance.json').read_text())
        if bp[key]!=cp[key]:raise ValueError('Environment mismatch: '+key)
    def index(path):
        indexed = {}
        for x in json.loads((path/'aggregate.json').read_text()):
            parameters = tuple((name, x.get(name)) for name in PARAMETERS if name in x)
            key = (x['case'], x['phase'], x['keys'], x['threads'],
                   x['expired_percent'], parameters)
            indexed[key] = x
        return indexed
    bi,ci=index(base),index(candidate);rows=[]
    for key in sorted(bi.keys()&ci.keys()):
        if not key[0].startswith('perf_'):
            continue
        x,y=bi[key],ci[key]
        if x['repeats']!=y['repeats']:raise ValueError('Repeat counts differ')
        rate=x['median_ops_per_second']
        row=dict(case=key[0],phase=key[1],keys=key[2],threads=key[3],
                 parameters=dict(key[5]),
                 candidate_over_baseline_throughput=y['median_ops_per_second']/rate if rate else None,
                 baseline_ops_per_second=rate,candidate_ops_per_second=y['median_ops_per_second'])
        for metric in ('p50_us','p95_us','p99_us','drain_seconds','peak_rss_kib'):
            before=x.get('median_'+metric);after=y.get('median_'+metric)
            row['baseline_'+metric]=before;row['candidate_'+metric]=after
            row['candidate_over_baseline_'+metric]=after/before if before not in (None,0) and after is not None else None
        for metric in ('metadata_memory_estimate_bytes','metadata_file_count',
                       'metadata_wal_pending_bytes','metadata_wal_size_limit_bytes',
                       'metadata_checkpoint_interval_micros',
                       'metadata_pending_change_bytes','metadata_pending_change_files'):
            key='median_profile_resource_after_drain_'+metric
            before=x.get(key);after=y.get(key)
            row['baseline_'+metric]=before;row['candidate_'+metric]=after
            row['candidate_over_baseline_'+metric]=after/before if before not in (None,0) and after is not None else None
        io_categories=('primary_sst','metadata_sst','metadata_change','wal','manifest','other')
        io_fields=sorted(name for name in x if name.startswith('median_') and
                         (any(name.startswith('median_'+category+'_') for category in io_categories) or
                          any(name.startswith('median_drain_'+category+'_') for category in io_categories)))
        row['io']={name.removeprefix('median_'):{'baseline':x.get(name),'candidate':y.get(name),
                   'ratio':y.get(name)/x[name] if x.get(name) not in (None,0) and y.get(name) is not None else None}
                   for name in io_fields}
        rows.append(row)
    if not rows:raise ValueError('No matching workloads')
    return dict(baseline=str(base),candidate=str(candidate),grade=None,
                note='Ratios are diagnostic; verify machine load and suite coverage before grading.', comparisons=rows)
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('baseline',type=Path);p.add_argument('candidate',type=Path);p.add_argument('--output',type=Path)
    a=p.parse_args();result=compare(a.baseline,a.candidate);text=json.dumps(result,ensure_ascii=False,indent=2)+'\n'
    if a.output:
        with a.output.open('x') as f:f.write(text)
    else:print(text,end='')
