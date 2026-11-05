# 三版本官方 Benchmark 性能对比报告

## 版本与方法

- 老师初始版本：`10568369137063605ec7f61ed30349d3ea44dfa0`，其数据库源码与起始提交 `775e7db` 一致。
- 主数据附着初版：2026-11-01 官方矩阵运行保留的源码副本；163 个源码文件与当时 provenance 记录的 SHA-256 全部一致。该版本当时是工作区实现，没有独立提交。
- 最新版：`512309f6ec235d0b47d951b67842ec5aa04b7aa9`。
- 使用老师原始 `benchmark/` 场景和 standard matrix：10k/100k、适用场景 1/8 线程、TTL 过期比例 0%/50%/100%；值 256 字节，rounds=8。
- 每个配置执行三轮，每轮种子依次为 7774596、7774597、7774598；三个版本逐次串行，轮换先后顺序。支持的版本各自使用相同种子与参数。
- 执行用的是老师原始 benchmark 可执行文件及其参数；调度脚本只是把原始运行器每个配置的三次执行按版本交替。各次使用新的数据库目录，成功后仅清理本次测试生成的数据库。
- 独立 Release 构建，三版均启用 Snappy、Zstd；相同机器和依赖。数字是吞吐量 ops/s 的三轮中位数；变化为两版中位数之比减一，不是课程最终分数。
- 老师初始版本没有实现 TTL、元数据和联合接口，只运行支持的普通性能场景，缺项以“—”表示，不当作零。

## 执行结果

- 共 372 次独立场景运行，通过 372 次，失败或超时 0 次。
- 性能阶段与配置：80 个；不完整的三次采样：0 个。
- 老师初始版本支持的场景：`perf_concurrent`, `perf_read`, `perf_reopen`, `perf_scan`, `perf_write`。
- 初始版本未运行的场景：`combined_concurrent`, `combined_contract`, `combined_gc`, `combined_overwrites`, `combined_reopen`, `combined_views`, `metadata_contract`, `metadata_reopen`, `perf_combined_gc`, `perf_combined_stats_under_write`, `perf_stats`, `perf_stats_under_write`, `perf_ttl_gc`, `perf_ttl_mixed`, `perf_ttl_write`, `ttl_contract`, `ttl_views`。

官方运行器默认附带的回归也以三轮轮换顺序执行，结果与上面的 benchmark 场景分别统计：

| 版本 | 原生回归 | C 接口回归 | Env 回归 |
| --- | --- | --- | --- |
| 老师初始版 | 三轮均 211/211 通过 | 三轮通过 | 三轮均 7/7 通过 |
| 主数据附着初版 | 三轮均在 `CorruptionTest.TableFileIndexData` 中断，进程退出码 -11 | 三轮通过 | 三轮均 7/7 通过 |
| 最新版 | 三轮均 211/211 通过 | 三轮通过 | 三轮均 7/7 通过 |

初版原生回归在受损 SST 重开失败后发生段错误，进程中断，因此原始三轮不能算全量通过。为检查其余覆盖，排除这个用例后又补跑三轮，每轮其余 210/210 项通过；这不改变该用例失败的结论。三次失败日志分别位于 `build/three-version-comparison-20261105/regression-runs/r{1,2,3}/primary/homework_regression.log`，补跑日志位于 `regression-followup/`。

2026-11-01 保存的初版评测使用的是未启用 Snappy/Zstd 的构建，本次三版均启用了两种压缩；两次回归条件不同，仅凭当前结果不能确定这是否是失败的根因。为保留历史版本的可比性，本次没有修改附着初版源码。

## 主要发现

- **100k 普通单线程写入**：老师初始版 101,562 ops/s、附着初版 63,997 ops/s、最新版 482,424 ops/s；最新版分别高 375.00% 和 653.82%。
- **100k、8 线程并发写入**：老师初始版 66,632 ops/s、附着初版 47,851 ops/s、最新版 163,066 ops/s；最新版分别高 144.73% 和 240.78%。
- **100k 重开**：老师初始版 112.67 次/s、附着初版 66.13 次/s、最新版 252.80 次/s；最新版分别高 124.38% 和 282.28%。
- **100k、8 线程写入时查询统计**：统计查询阶段从附着初版的 7,566,200 ops/s 增至最新版 19,438,400 ops/s，高 156.91%。老师初始版没有该功能，无法对照。
- **100k TTL GC**：附着初版为 279,024 ops/s，最新版为 4,321,590 ops/s，高 1,448.82%；100k、50% 过期的联合 GC 从 354,877 增至 1,916,900 ops/s，高 440.16%。这是官方场景以配置键数除以该阶段耗时得到的吞吐，不表示每秒处理的真实磁盘设备操作数。
- **存在下降项**：10k、8 线程的 `perf_stats_under_write / put_with_stats_queries` 从附着初版 54,733.60 降至最新版 50,914.00 ops/s（-6.98%）；10k 重开相比老师初始版低 4.34%，10k 扫描低 2.02%。这些差异基于三次中位数，不能排除测试波动。

本次同时跨越了 TTL 语义修复、索引设计和多项性能优化；数字反映整个版本之间的表现差异，不能单独归因于某一项改动。老师初始版没有 TTL 和统计功能，而较新版本具有额外功能，因此公共性能项的对比反映的是各版本实际实现，不表示功能完全相同。

## 全部性能结果

变化列分别是主数据附着初版相对老师初始版本、最新版相对老师初始版本、最新版相对主数据附着初版。`—` 表示该版本没有对应功能或没有结果。

| 场景 / 阶段 | 键数 | 线程 | 过期率 | 老师初始 | 附着初版 | 最新版 | 初版/老师 | 最新/老师 | 最新/初版 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| perf_combined_gc / ttl_gc_with_statistics | 10000 | 1 | 0% | — | 316,939.00 | 659,388.00 | — | — | +108.05% |
| perf_combined_gc / ttl_gc_with_statistics | 10000 | 1 | 100% | — | 278,003.00 | 1,877,690.00 | — | — | +575.42% |
| perf_combined_gc / ttl_gc_with_statistics | 10000 | 1 | 50% | — | 274,626.00 | 763,176.00 | — | — | +177.90% |
| perf_combined_gc / ttl_gc_with_statistics | 100000 | 1 | 0% | — | 461,482.00 | 1,201,120.00 | — | — | +160.27% |
| perf_combined_gc / ttl_gc_with_statistics | 100000 | 1 | 100% | — | 280,561.00 | 4,299,590.00 | — | — | +1432.50% |
| perf_combined_gc / ttl_gc_with_statistics | 100000 | 1 | 50% | — | 354,877.00 | 1,916,900.00 | — | — | +440.16% |
| perf_combined_stats_under_write / stats_under_ttl_write | 10000 | 1 | 0% | — | 10,092,200.00 | 19,791,900.00 | — | — | +96.11% |
| perf_combined_stats_under_write / stats_under_ttl_write | 10000 | 1 | 100% | — | 10,232,200.00 | 19,741,600.00 | — | — | +92.94% |
| perf_combined_stats_under_write / stats_under_ttl_write | 10000 | 1 | 50% | — | 9,928,650.00 | 19,383,400.00 | — | — | +95.23% |
| perf_combined_stats_under_write / stats_under_ttl_write | 10000 | 8 | 0% | — | 6,999,120.00 | 17,213,900.00 | — | — | +145.94% |
| perf_combined_stats_under_write / stats_under_ttl_write | 10000 | 8 | 100% | — | 7,475,910.00 | 16,324,700.00 | — | — | +118.36% |
| perf_combined_stats_under_write / stats_under_ttl_write | 10000 | 8 | 50% | — | 7,221,120.00 | 17,395,500.00 | — | — | +140.90% |
| perf_combined_stats_under_write / stats_under_ttl_write | 100000 | 1 | 0% | — | 7,626,500.00 | 19,310,200.00 | — | — | +153.20% |
| perf_combined_stats_under_write / stats_under_ttl_write | 100000 | 1 | 100% | — | 7,718,790.00 | 19,061,800.00 | — | — | +146.95% |
| perf_combined_stats_under_write / stats_under_ttl_write | 100000 | 1 | 50% | — | 7,900,800.00 | 19,517,700.00 | — | — | +147.03% |
| perf_combined_stats_under_write / stats_under_ttl_write | 100000 | 8 | 0% | — | 7,319,470.00 | 17,088,500.00 | — | — | +133.47% |
| perf_combined_stats_under_write / stats_under_ttl_write | 100000 | 8 | 100% | — | 7,339,910.00 | 17,320,100.00 | — | — | +135.97% |
| perf_combined_stats_under_write / stats_under_ttl_write | 100000 | 8 | 50% | — | 7,429,520.00 | 17,203,800.00 | — | — | +131.56% |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 10000 | 1 | 0% | — | 68,074.90 | 479,689.00 | — | — | +604.65% |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 10000 | 1 | 100% | — | 68,040.40 | 473,818.00 | — | — | +596.38% |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 10000 | 1 | 50% | — | 67,637.00 | 472,172.00 | — | — | +598.10% |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 10000 | 8 | 0% | — | 56,568.50 | 158,870.00 | — | — | +180.85% |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 10000 | 8 | 100% | — | 54,436.20 | 155,107.00 | — | — | +184.93% |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 10000 | 8 | 50% | — | 56,611.30 | 159,445.00 | — | — | +181.65% |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 100000 | 1 | 0% | — | 61,112.70 | 459,967.00 | — | — | +652.65% |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 100000 | 1 | 100% | — | 60,589.60 | 482,117.00 | — | — | +695.71% |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 100000 | 1 | 50% | — | 60,805.90 | 439,557.00 | — | — | +622.89% |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 100000 | 8 | 0% | — | 44,992.20 | 156,403.00 | — | — | +247.62% |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 100000 | 8 | 100% | — | 45,310.70 | 154,828.00 | — | — | +241.70% |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 100000 | 8 | 50% | — | 44,390.80 | 156,330.00 | — | — | +252.17% |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 10000 | 1 | 0% | — | 68,798.40 | 484,948.00 | — | — | +604.88% |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 10000 | 1 | 100% | — | 69,249.60 | 483,207.00 | — | — | +597.78% |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 10000 | 1 | 50% | — | 69,049.70 | 476,541.00 | — | — | +590.14% |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 10000 | 8 | 0% | — | 57,052.30 | 163,399.00 | — | — | +186.40% |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 10000 | 8 | 100% | — | 57,440.30 | 165,081.00 | — | — | +187.40% |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 10000 | 8 | 50% | — | 57,412.40 | 164,058.00 | — | — | +185.75% |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 100000 | 1 | 0% | — | 62,384.10 | 466,239.00 | — | — | +647.37% |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 100000 | 1 | 100% | — | 62,592.50 | 495,001.00 | — | — | +690.83% |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 100000 | 1 | 50% | — | 62,515.20 | 440,008.00 | — | — | +603.84% |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 100000 | 8 | 0% | — | 45,144.20 | 162,999.00 | — | — | +261.06% |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 100000 | 8 | 100% | — | 45,594.30 | 162,541.00 | — | — | +256.49% |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 100000 | 8 | 50% | — | 45,392.70 | 164,066.00 | — | — | +261.44% |
| perf_concurrent / parallel_put | 10000 | 1 | 50% | 115,213.00 | 69,953.70 | 501,160.00 | -39.28% | +334.99% | +616.42% |
| perf_concurrent / parallel_put | 10000 | 8 | 50% | 87,837.30 | 59,082.70 | 165,416.00 | -32.74% | +88.32% | +179.97% |
| perf_concurrent / parallel_put | 100000 | 1 | 50% | 102,516.00 | 64,410.40 | 491,447.00 | -37.17% | +379.39% | +662.99% |
| perf_concurrent / parallel_put | 100000 | 8 | 50% | 66,632.10 | 47,851.40 | 163,066.00 | -28.19% | +144.73% | +240.78% |
| perf_read / get_hit | 10000 | 1 | 50% | 855,678.00 | 808,704.00 | 1,184,140.00 | -5.49% | +38.39% | +46.42% |
| perf_read / get_hit | 100000 | 1 | 50% | 689,408.00 | 643,243.00 | 909,688.00 | -6.70% | +31.95% | +41.42% |
| perf_read / get_miss | 10000 | 1 | 50% | 3,523,570.00 | 2,647,340.00 | 4,821,930.00 | -24.87% | +36.85% | +82.14% |
| perf_read / get_miss | 100000 | 1 | 50% | 3,222,810.00 | 2,514,250.00 | 4,797,060.00 | -21.99% | +48.85% | +90.79% |
| perf_reopen / open | 10000 | 1 | 50% | 115.64 | 98.93 | 110.63 | -14.45% | -4.34% | +11.82% |
| perf_reopen / open | 100000 | 1 | 50% | 112.67 | 66.13 | 252.79 | -41.31% | +124.38% | +282.28% |
| perf_scan / scan | 10000 | 1 | 50% | 2,618,460.00 | 2,342,170.00 | 2,565,580.00 | -10.55% | -2.02% | +9.54% |
| perf_scan / scan | 100000 | 1 | 50% | 2,593,240.00 | 2,329,820.00 | 2,579,180.00 | -10.16% | -0.54% | +10.70% |
| perf_stats / stats_query | 10000 | 1 | 50% | — | 1,879,170.00 | 1,973,050.00 | — | — | +5.00% |
| perf_stats / stats_query | 100000 | 1 | 50% | — | 1,783,440.00 | 1,879,390.00 | — | — | +5.38% |
| perf_stats_under_write / put_with_stats_queries | 10000 | 1 | 50% | — | 64,099.70 | 59,731.20 | — | — | -6.82% |
| perf_stats_under_write / put_with_stats_queries | 10000 | 8 | 50% | — | 54,733.60 | 50,914.00 | — | — | -6.98% |
| perf_stats_under_write / put_with_stats_queries | 100000 | 1 | 50% | — | 60,911.40 | 457,052.00 | — | — | +650.36% |
| perf_stats_under_write / put_with_stats_queries | 100000 | 8 | 50% | — | 45,630.80 | 156,497.00 | — | — | +242.96% |
| perf_stats_under_write / put_without_stats_queries | 10000 | 1 | 50% | — | 64,416.90 | 60,463.10 | — | — | -6.14% |
| perf_stats_under_write / put_without_stats_queries | 10000 | 8 | 50% | — | 55,604.40 | 52,084.00 | — | — | -6.33% |
| perf_stats_under_write / put_without_stats_queries | 100000 | 1 | 50% | — | 62,798.70 | 470,293.00 | — | — | +648.89% |
| perf_stats_under_write / put_without_stats_queries | 100000 | 8 | 50% | — | 45,741.50 | 161,767.00 | — | — | +253.65% |
| perf_stats_under_write / stats_under_write | 10000 | 1 | 50% | — | 11,284,700.00 | 22,678,000.00 | — | — | +100.96% |
| perf_stats_under_write / stats_under_write | 10000 | 8 | 50% | — | 7,954,740.00 | 22,014,500.00 | — | — | +176.75% |
| perf_stats_under_write / stats_under_write | 100000 | 1 | 50% | — | 8,517,080.00 | 22,902,100.00 | — | — | +168.90% |
| perf_stats_under_write / stats_under_write | 100000 | 8 | 50% | — | 7,566,200.00 | 19,438,400.00 | — | — | +156.91% |
| perf_ttl_gc / ttl_full_gc | 10000 | 1 | 50% | — | 280,731.00 | 1,898,270.00 | — | — | +576.19% |
| perf_ttl_gc / ttl_full_gc | 100000 | 1 | 50% | — | 279,024.00 | 4,321,590.00 | — | — | +1448.82% |
| perf_ttl_mixed / ttl_mixed_get | 10000 | 1 | 0% | — | 793,097.00 | 1,211,630.00 | — | — | +52.77% |
| perf_ttl_mixed / ttl_mixed_get | 10000 | 1 | 100% | — | 881,248.00 | 1,579,950.00 | — | — | +79.29% |
| perf_ttl_mixed / ttl_mixed_get | 10000 | 1 | 50% | — | 778,664.00 | 1,341,710.00 | — | — | +72.31% |
| perf_ttl_mixed / ttl_mixed_get | 100000 | 1 | 0% | — | 636,067.00 | 946,786.00 | — | — | +48.85% |
| perf_ttl_mixed / ttl_mixed_get | 100000 | 1 | 100% | — | 684,616.00 | 1,232,870.00 | — | — | +80.08% |
| perf_ttl_mixed / ttl_mixed_get | 100000 | 1 | 50% | — | 649,716.00 | 1,032,970.00 | — | — | +58.99% |
| perf_ttl_write / ttl_put | 10000 | 1 | 50% | — | 70,044.70 | 506,738.00 | — | — | +623.45% |
| perf_ttl_write / ttl_put | 100000 | 1 | 50% | — | 63,323.70 | 470,104.00 | — | — | +642.38% |
| perf_write / put | 10000 | 1 | 50% | 110,381.00 | 69,914.10 | 523,944.00 | -36.66% | +374.67% | +649.41% |
| perf_write / put | 100000 | 1 | 50% | 101,562.00 | 63,997.40 | 482,424.00 | -36.99% | +375.00% | +653.82% |

## 数据与限制

- [每轮原始指标](metrics-raw.csv) 包含吞吐、耗时、p50/p95/p99、采样数和阶段 I/O 等原始字段；[中位数及波动范围](metrics-median.csv) 包含各版本每项指标的最小值、中位数和最大值。
- [各场景峰值 RSS](peak-rss.csv) 为 benchmark 进程的 RSS，包含测试驱动、数据模型与缓存，并非数据库内部独占内存。例如 100k 单线程普通写入的三轮中位数分别为老师初始版 30,540 KiB、附着初版 30,732 KiB、最新版 41,404 KiB。
- 测试执行记录、逐次日志、源码副本、构建日志和版本清单位于 `build/three-version-comparison-20261105/`；该目录通常被 Git 忽略。
- 三次重复能反映本次的波动，但不能排除操作系统调度、缓存及 I/O 状态的影响；对关键异常项应增加配对复测。
- 官方 benchmark 的不同阶段不应不加权直接合成为官方分数；TTL 与统计专有场景无法与没有这些功能的初始版本比较。
- 本报告反映老师公开场景，不代表隐藏测试结果。
