#!/usr/bin/env python3
"""Summarize unmodified course-runner outputs; never calculates an official score."""
import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


def load(directory):
    rows = json.loads((directory / 'aggregate.json').read_text())
    return {(x['case'], x['phase'], x['keys'], x['threads'], x['expired_percent']): x for x in rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--paired', type=Path)
    parser.add_argument('--micro', type=Path)
    args = parser.parse_args()
    baseline, candidate = load(args.baseline), load(args.candidate)
    if baseline.keys() != candidate.keys():
        raise SystemExit('configuration sets differ')
    args.output.mkdir(parents=True, exist_ok=True)
    paired = json.loads(args.paired.read_text()) if args.paired else []
    rates = defaultdict(list)
    for x in paired:
        if x['status'] != 'pass':
            raise SystemExit('paired run failed')
        for metric in x['metrics']:
            key = (x['case'], metric['phase'], x['keys'], x['threads'], x['expired_percent'])
            rates[(key, x['version'])].append(metric['ops_per_second'])
    def extras(directory):
        grouped = defaultdict(list)
        summary = json.loads((directory / 'summary.json').read_text())
        for run in summary['records']:
            for metric in run.get('metrics', []):
                key = (run['case'], metric['phase'], run['keys'], run['threads'], run['expired_percent'])
                grouped[key].append(dict(peak_rss_kib=run.get('peak_rss_kib', 0),
                    append_bytes=metric.get('append_bytes_during_phase', 0),
                    directory_bytes=metric.get('db_directory_bytes_after_drain', 0)))
        return {key: {field: statistics.median(x[field] for x in samples)
                     for field in samples[0]} for key, samples in grouped.items()}
    baseline_extras, candidate_extras = extras(args.baseline), extras(args.candidate)
    rows = []
    families = defaultdict(list)
    for key, a in sorted(baseline.items()):
        b = candidate[key]
        ratio = b['median_ops_per_second'] / a['median_ops_per_second']
        pair_ratio = None
        if (key, 'baseline') in rates:
            pair_ratio = statistics.median(rates[(key, 'candidate')]) / statistics.median(rates[(key, 'baseline')])
        row = dict(case=key[0], phase=key[1], keys=key[2], threads=key[3], expired_percent=key[4],
                   baseline_ops_s=a['median_ops_per_second'], candidate_ops_s=b['median_ops_per_second'],
                   speedup=ratio, change_percent=(ratio - 1) * 100,
                   baseline_p99_us=a['median_p99_us'], candidate_p99_us=b['median_p99_us'],
                   paired_speedup=pair_ratio,
                   baseline_min_ops_s=a['min_ops_per_second'], baseline_max_ops_s=a['max_ops_per_second'],
                   candidate_min_ops_s=b['min_ops_per_second'], candidate_max_ops_s=b['max_ops_per_second'])
        row.update({'baseline_' + field: value for field, value in baseline_extras[key].items()})
        row.update({'candidate_' + field: value for field, value in candidate_extras[key].items()})
        rows.append(row)
        families[key[0]].append(ratio)
    with (args.output / 'configurations.csv').open('w') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summaries = {name: dict(points=len(values), geometric_speedup=math.exp(statistics.mean(math.log(v) for v in values)),
                           worst_speedup=min(values), best_speedup=max(values)) for name, values in families.items()}
    (args.output / 'comparison.json').write_text(json.dumps(dict(families=summaries, points=rows), indent=2) + '\n')
    lines = ['# SST 局部计数索引性能对照', '',
             '基线：`d59421e`；候选：`feature/sst-local-count-index`。官方 runner 与测试用例未修改。', '',
             'Release / standard，10k、100k，适用场景覆盖 1/8 线程和 0/50/100% 到期比例，每配置 5 次，以下为中位吞吐比。',
             '比值 >1 表示候选更快。家族几何均值仅用于汇总，**不是老师的最终评分公式**。', '',
             f'- 基线原始数据：`{args.baseline}`', f'- 候选原始数据：`{args.candidate}`',
             '- 初始探索基线如与构建重叠，不用于本表；最终基线、候选及交替复测均串行运行。', '',
             '| 家族 | 测试点数 | 几何吞吐比 | 最差 | 最好 |', '|---|---:|---:|---:|---:|']
    for name, x in summaries.items():
        lines.append(f"| {name} | {x['points']} | {x['geometric_speedup']:.3f}× | {x['worst_speedup']:.3f}× | {x['best_speedup']:.3f}× |")
    lines += ['', '## 逐配置结果', '', '| 家族 / 阶段 | N | 线程 | 到期% | 基线 ops/s | 候选 ops/s | 变化 | 交替复测吞吐比 |', '|---|---:|---:|---:|---:|---:|---:|---:|']
    for x in rows:
        pair = '-' if x['paired_speedup'] is None else f"{x['paired_speedup']:.3f}×"
        lines.append(f"| {x['case']} / {x['phase']} | {x['keys']} | {x['threads']} | {x['expired_percent']} | {x['baseline_ops_s']:.2f} | {x['candidate_ops_s']:.2f} | {x['change_percent']:+.1f}% | {pair} |")
    if args.micro:
        micro = [json.loads(line) for line in args.micro.read_text().splitlines() if line.startswith('{')]
        samples = [x for x in micro if 'indexed' in x and x['pass'] < 2]
        lines += ['', '## 独立诊断微基准', '', '下表使用内存文件和无压缩编码，非官方评分；冷热轮次分别为第一、第二轮 4000 次查询，时间关闭 Profiler。主数据与索引查找各使用 4 MiB 有效缓存，5 次重复取中位数。', '',
                  '| key 数 / 每 key 版本数 | SST 增量 | 构建耗时比（候选/基线） | 冷查询加速 | 暖查询加速 | 索引暖轮块读数 | 缓存 charge |', '|---|---:|---:|---:|---:|---:|---:|']
        for shape in sorted({(x['keys'], x['versions']) for x in samples}):
            def med(indexed, phase, field):
                return statistics.median(x[field] for x in samples if (x['keys'], x['versions']) == shape and x['indexed'] == indexed and x['pass'] == phase)
            lines.append(f"| {shape[0]} / {shape[1]} | {(med(1,0,'sst_bytes')/med(0,0,'sst_bytes')-1)*100:+.2f}% | {med(1,0,'build_us')/med(0,0,'build_us'):.3f}× | {med(0,0,'lookup_us')/med(1,0,'lookup_us'):.3f}× | {med(0,1,'lookup_us')/med(1,1,'lookup_us'):.3f}× | {med(1,1,'block_reads'):.0f} | {med(1,1,'cache_charge'):.0f} B |")
        lines += ['', '微基准的优势来自计数结构比完整主数据更紧凑；不能据此推断所有官方写入、扫描或 GC 都加速。第三轮开启 Profiler 的数据只作计数诊断。']
    lines += ['', '## 解释范围', '',
              'SST 计数树需要额外编码、校验、目录和输出字节；普通写入的上界/过滤器已能排除大量新 key，因此并非每次 PUT 都能从索引获益。',
              '存在性提前停止更适合重复写热点；冷索引查找仍需目录和叶子 I/O。GC 在索引构建与归零判断之间交换成本。重开额外读取一个很小的描述符块，不加载整个计数目录。',
              '超过 5% 下降的完整配置追加 5 对交替复测。两个表中的差异提示噪声或真实代价，需要同时查看逐配置结果；不隐瞒回退项，也不预先承诺此方案优于原版本。', '',
              '代码结构、缓存边界、兼容与故障策略见 `SST_LOCAL_COUNTS.md`。完整逐配置 p99、吞吐范围、进程峰值 RSS、阶段追加字节与目录体积见 `configurations.csv`。RSS 是整个进程峰值，不能当成索引自身的内存占用。']
    (args.output / 'REPORT.md').write_text('\n'.join(lines) + '\n')


if __name__ == '__main__':
    main()
