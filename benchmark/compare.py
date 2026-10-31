#!/usr/bin/env python3
"""Compare successful matched workloads; does not assign performance grades."""
import argparse
import json
from pathlib import Path

FIELDS=('value_bytes','seed','sanitize')
def compare(base, candidate):
    b=json.loads((base/'summary.json').read_text());c=json.loads((candidate/'summary.json').read_text())
    if b['status']!='pass' or c['status']!='pass':raise ValueError('Only passing runs may be compared')
    if b['kind']!='baseline':raise ValueError('Baseline run must use --kind baseline')
    if any(b.get(k)!=c.get(k) for k in FIELDS):raise ValueError('Value sizes, seeds or sanitizer modes differ')
    for key in ('platform','compiler','submodules'):
        bp=json.loads((base/'provenance.json').read_text());cp=json.loads((candidate/'provenance.json').read_text())
        if bp[key]!=cp[key]:raise ValueError('Environment mismatch: '+key)
    def index(path):
        return {(x['case'],x['phase'],x['keys'],x['threads'],x['expired_percent']):x
                for x in json.loads((path/'aggregate.json').read_text())}
    bi,ci=index(base),index(candidate);rows=[]
    for key in sorted(bi.keys()&ci.keys()):
        if not key[0].startswith('perf_'):
            continue
        x,y=bi[key],ci[key]
        if x['repeats']!=y['repeats']:raise ValueError('Repeat counts differ')
        rate=x['median_ops_per_second']
        rows.append(dict(case=key[0],phase=key[1],keys=key[2],threads=key[3],
                         candidate_over_baseline_throughput=y['median_ops_per_second']/rate if rate else None,
                         baseline=rate,candidate=y['median_ops_per_second']))
    if not rows:raise ValueError('No matching workloads')
    return dict(baseline=str(base),candidate=str(candidate),grade=None,
                note='Ratios are diagnostic; verify machine load and suite coverage before grading.', comparisons=rows)
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('baseline',type=Path);p.add_argument('candidate',type=Path);p.add_argument('--output',type=Path)
    a=p.parse_args();result=compare(a.baseline,a.candidate);text=json.dumps(result,ensure_ascii=False,indent=2)+'\n'
    if a.output:
        with a.output.open('x') as f:f.write(text)
    else:print(text,end='')
