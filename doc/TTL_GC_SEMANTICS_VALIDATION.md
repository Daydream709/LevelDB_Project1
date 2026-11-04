# TTL、快照、全量 GC 和统计语义验证

基线为 `a3896b6`，对照为本次工作区实现；两者使用同一官方运行器、standard 矩阵，每点运行 3 次，表中为吞吐量中位数，变化为新版本相对基线。不同批次执行，结果可能受系统噪声影响。

官方矩阵 168 次运行全部通过；附带原生、C 和环境回归全部通过。独立原生测试 320 通过、2 项因未启用相应压缩库跳过；新增并发 GC 测试两种配置均通过。

`perf_stats_under_write / put_with_stats_queries / 10000 / 1` 的原始基线三次为 119103、98838.4、60084.2 ops/s，波动很大。对两版各五次独立复测后，中位数分别为 61981.2 和 62064.2 ops/s（+0.13%）；因此原矩阵的 -36.88% 不足以证明稳定回退。

| 场景 | 阶段 | 键数 | 线程 | 过期比例 % | 基线 ops/s | 当前 ops/s | 变化 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| perf_combined_gc | ttl_gc_with_statistics | 10000 | 1 | 0 | 471592.00 | 722054.00 | +53.11% |
| perf_combined_gc | ttl_gc_with_statistics | 10000 | 1 | 100 | 376003.00 | 380389.00 | +1.17% |
| perf_combined_gc | ttl_gc_with_statistics | 10000 | 1 | 50 | 374185.00 | 377999.00 | +1.02% |
| perf_combined_gc | ttl_gc_with_statistics | 100000 | 1 | 0 | 1025970.00 | 1011950.00 | -1.37% |
| perf_combined_gc | ttl_gc_with_statistics | 100000 | 1 | 100 | 470473.00 | 468847.00 | -0.35% |
| perf_combined_gc | ttl_gc_with_statistics | 100000 | 1 | 50 | 642672.00 | 638501.00 | -0.65% |
| perf_combined_stats_under_write | stats_under_ttl_write | 10000 | 1 | 0 | 20854700.00 | 19540900.00 | -6.30% |
| perf_combined_stats_under_write | stats_under_ttl_write | 10000 | 1 | 100 | 21064800.00 | 19903200.00 | -5.51% |
| perf_combined_stats_under_write | stats_under_ttl_write | 10000 | 1 | 50 | 20050100.00 | 19334700.00 | -3.57% |
| perf_combined_stats_under_write | stats_under_ttl_write | 10000 | 8 | 0 | 17711800.00 | 17560900.00 | -0.85% |
| perf_combined_stats_under_write | stats_under_ttl_write | 10000 | 8 | 100 | 16964600.00 | 17615300.00 | +3.84% |
| perf_combined_stats_under_write | stats_under_ttl_write | 10000 | 8 | 50 | 17872200.00 | 17720900.00 | -0.85% |
| perf_combined_stats_under_write | stats_under_ttl_write | 100000 | 1 | 0 | 19905300.00 | 20480900.00 | +2.89% |
| perf_combined_stats_under_write | stats_under_ttl_write | 100000 | 1 | 100 | 19277200.00 | 20620100.00 | +6.97% |
| perf_combined_stats_under_write | stats_under_ttl_write | 100000 | 1 | 50 | 20807800.00 | 20802000.00 | -0.03% |
| perf_combined_stats_under_write | stats_under_ttl_write | 100000 | 8 | 0 | 17768400.00 | 17653700.00 | -0.65% |
| perf_combined_stats_under_write | stats_under_ttl_write | 100000 | 8 | 100 | 17989600.00 | 17909000.00 | -0.45% |
| perf_combined_stats_under_write | stats_under_ttl_write | 100000 | 8 | 50 | 17490000.00 | 17731700.00 | +1.38% |
| perf_combined_stats_under_write | ttl_put_with_stats_queries | 10000 | 1 | 0 | 477591.00 | 480497.00 | +0.61% |
| perf_combined_stats_under_write | ttl_put_with_stats_queries | 10000 | 1 | 100 | 485102.00 | 483109.00 | -0.41% |
| perf_combined_stats_under_write | ttl_put_with_stats_queries | 10000 | 1 | 50 | 481898.00 | 483384.00 | +0.31% |
| perf_combined_stats_under_write | ttl_put_with_stats_queries | 10000 | 8 | 0 | 140069.00 | 140615.00 | +0.39% |
| perf_combined_stats_under_write | ttl_put_with_stats_queries | 10000 | 8 | 100 | 138978.00 | 141154.00 | +1.57% |
| perf_combined_stats_under_write | ttl_put_with_stats_queries | 10000 | 8 | 50 | 141086.00 | 140773.00 | -0.22% |
| perf_combined_stats_under_write | ttl_put_with_stats_queries | 100000 | 1 | 0 | 273242.00 | 273050.00 | -0.07% |
| perf_combined_stats_under_write | ttl_put_with_stats_queries | 100000 | 1 | 100 | 320259.00 | 289183.00 | -9.70% |
| perf_combined_stats_under_write | ttl_put_with_stats_queries | 100000 | 1 | 50 | 272548.00 | 272288.00 | -0.10% |
| perf_combined_stats_under_write | ttl_put_with_stats_queries | 100000 | 8 | 0 | 137850.00 | 139662.00 | +1.31% |
| perf_combined_stats_under_write | ttl_put_with_stats_queries | 100000 | 8 | 100 | 138642.00 | 138990.00 | +0.25% |
| perf_combined_stats_under_write | ttl_put_with_stats_queries | 100000 | 8 | 50 | 138602.00 | 138787.00 | +0.13% |
| perf_combined_stats_under_write | ttl_put_without_stats_queries | 10000 | 1 | 0 | 494007.00 | 486821.00 | -1.45% |
| perf_combined_stats_under_write | ttl_put_without_stats_queries | 10000 | 1 | 100 | 491515.00 | 492286.00 | +0.16% |
| perf_combined_stats_under_write | ttl_put_without_stats_queries | 10000 | 1 | 50 | 493722.00 | 493393.00 | -0.07% |
| perf_combined_stats_under_write | ttl_put_without_stats_queries | 10000 | 8 | 0 | 146106.00 | 147354.00 | +0.85% |
| perf_combined_stats_under_write | ttl_put_without_stats_queries | 10000 | 8 | 100 | 145214.00 | 146920.00 | +1.17% |
| perf_combined_stats_under_write | ttl_put_without_stats_queries | 10000 | 8 | 50 | 145772.00 | 147842.00 | +1.42% |
| perf_combined_stats_under_write | ttl_put_without_stats_queries | 100000 | 1 | 0 | 272608.00 | 272872.00 | +0.10% |
| perf_combined_stats_under_write | ttl_put_without_stats_queries | 100000 | 1 | 100 | 282273.00 | 277469.00 | -1.70% |
| perf_combined_stats_under_write | ttl_put_without_stats_queries | 100000 | 1 | 50 | 272426.00 | 271988.00 | -0.16% |
| perf_combined_stats_under_write | ttl_put_without_stats_queries | 100000 | 8 | 0 | 142325.00 | 145564.00 | +2.28% |
| perf_combined_stats_under_write | ttl_put_without_stats_queries | 100000 | 8 | 100 | 145791.00 | 146241.00 | +0.31% |
| perf_combined_stats_under_write | ttl_put_without_stats_queries | 100000 | 8 | 50 | 144771.00 | 145921.00 | +0.79% |
| perf_concurrent | parallel_put | 10000 | 1 | 50 | 492697.00 | 501700.00 | +1.83% |
| perf_concurrent | parallel_put | 10000 | 8 | 50 | 145606.00 | 146308.00 | +0.48% |
| perf_concurrent | parallel_put | 100000 | 1 | 50 | 487705.00 | 492523.00 | +0.99% |
| perf_concurrent | parallel_put | 100000 | 8 | 50 | 144592.00 | 146101.00 | +1.04% |
| perf_read | get_hit | 10000 | 1 | 50 | 1171080.00 | 1213080.00 | +3.59% |
| perf_read | get_hit | 100000 | 1 | 50 | 873631.00 | 913459.00 | +4.56% |
| perf_read | get_miss | 10000 | 1 | 50 | 4938600.00 | 4798020.00 | -2.85% |
| perf_read | get_miss | 100000 | 1 | 50 | 4934900.00 | 4899270.00 | -0.72% |
| perf_reopen | open | 10000 | 1 | 50 | 243.34 | 251.11 | +3.19% |
| perf_reopen | open | 100000 | 1 | 50 | 236.32 | 249.82 | +5.71% |
| perf_scan | scan | 10000 | 1 | 50 | 2675180.00 | 2681590.00 | +0.24% |
| perf_scan | scan | 100000 | 1 | 50 | 2660250.00 | 2656910.00 | -0.13% |
| perf_stats | stats_query | 10000 | 1 | 50 | 1981690.00 | 1962710.00 | -0.96% |
| perf_stats | stats_query | 100000 | 1 | 50 | 1872060.00 | 1856270.00 | -0.84% |
| perf_stats_under_write | put_with_stats_queries | 10000 | 1 | 50 | 98838.40 | 62383.60 | -36.88% |
| perf_stats_under_write | put_with_stats_queries | 10000 | 8 | 50 | 53147.90 | 53167.80 | +0.04% |
| perf_stats_under_write | put_with_stats_queries | 100000 | 1 | 50 | 245380.00 | 229153.00 | -6.61% |
| perf_stats_under_write | put_with_stats_queries | 100000 | 8 | 50 | 137931.00 | 139007.00 | +0.78% |
| perf_stats_under_write | put_without_stats_queries | 10000 | 1 | 50 | 63120.90 | 62324.30 | -1.26% |
| perf_stats_under_write | put_without_stats_queries | 10000 | 8 | 50 | 53061.40 | 52741.10 | -0.60% |
| perf_stats_under_write | put_without_stats_queries | 100000 | 1 | 50 | 229413.00 | 228946.00 | -0.20% |
| perf_stats_under_write | put_without_stats_queries | 100000 | 8 | 50 | 144937.00 | 144721.00 | -0.15% |
| perf_stats_under_write | stats_under_write | 10000 | 1 | 50 | 21393400.00 | 23109700.00 | +8.02% |
| perf_stats_under_write | stats_under_write | 10000 | 8 | 50 | 21650700.00 | 21727600.00 | +0.36% |
| perf_stats_under_write | stats_under_write | 100000 | 1 | 50 | 23069800.00 | 23184500.00 | +0.50% |
| perf_stats_under_write | stats_under_write | 100000 | 8 | 50 | 19637700.00 | 19518600.00 | -0.61% |
| perf_ttl_gc | ttl_full_gc | 10000 | 1 | 50 | 549904.00 | 563371.00 | +2.45% |
| perf_ttl_gc | ttl_full_gc | 100000 | 1 | 50 | 463740.00 | 464928.00 | +0.26% |
| perf_ttl_mixed | ttl_mixed_get | 10000 | 1 | 0 | 1205570.00 | 1216750.00 | +0.93% |
| perf_ttl_mixed | ttl_mixed_get | 10000 | 1 | 100 | 1592340.00 | 1501930.00 | -5.68% |
| perf_ttl_mixed | ttl_mixed_get | 10000 | 1 | 50 | 1345570.00 | 1260850.00 | -6.30% |
| perf_ttl_mixed | ttl_mixed_get | 100000 | 1 | 0 | 958249.00 | 963836.00 | +0.58% |
| perf_ttl_mixed | ttl_mixed_get | 100000 | 1 | 100 | 1215130.00 | 1264020.00 | +4.02% |
| perf_ttl_mixed | ttl_mixed_get | 100000 | 1 | 50 | 1041860.00 | 1011630.00 | -2.90% |
| perf_ttl_write | ttl_put | 10000 | 1 | 50 | 507542.00 | 511961.00 | +0.87% |
| perf_ttl_write | ttl_put | 100000 | 1 | 50 | 484263.00 | 487597.00 | +0.69% |
| perf_write | put | 10000 | 1 | 50 | 518925.00 | 526877.00 | +1.53% |
| perf_write | put | 100000 | 1 | 50 | 497259.00 | 504878.00 | +1.53% |

原始运行文件：`/tmp/merge-benchmark-after-results/`、`/tmp/ttl-semantics-final-results/`；复测：`/tmp/ttl-semantics-baseline-recheck/`、`/tmp/ttl-semantics-current-recheck/`。这些临时目录不保证长期保留。
