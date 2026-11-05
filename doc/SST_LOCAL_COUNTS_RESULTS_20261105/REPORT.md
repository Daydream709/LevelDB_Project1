# SST 局部计数索引性能对照

基线：`d59421e`；候选：`feature/sst-local-count-index`。官方 runner 与测试用例未修改。

Release / standard，10k、100k，适用场景覆盖 1/8 线程和 0/50/100% 到期比例，每配置 5 次，以下为中位吞吐比。
比值 >1 表示候选更快。家族几何均值仅用于汇总，**不是老师的最终评分公式**。

- 基线原始数据：`/home/ubuntu/leveldb_project1/build/sst-local-count-comparison/baseline-final`
- 候选原始数据：`/home/ubuntu/leveldb_project1/build/sst-local-count-comparison/candidate-final`
- 初始探索基线如与构建重叠，不用于本表；最终基线、候选及交替复测均串行运行。

| 家族 | 测试点数 | 几何吞吐比 | 最差 | 最好 |
|---|---:|---:|---:|---:|
| perf_combined_gc | 6 | 1.014× | 0.772× | 1.361× |
| perf_combined_stats_under_write | 36 | 1.005× | 0.976× | 1.077× |
| perf_concurrent | 4 | 0.992× | 0.989× | 0.996× |
| perf_read | 4 | 1.044× | 1.025× | 1.083× |
| perf_reopen | 2 | 1.005× | 0.977× | 1.033× |
| perf_scan | 2 | 0.989× | 0.988× | 0.990× |
| perf_stats | 2 | 0.996× | 0.995× | 0.998× |
| perf_stats_under_write | 12 | 0.999× | 0.959× | 1.060× |
| perf_ttl_gc | 2 | 0.958× | 0.920× | 0.997× |
| perf_ttl_mixed | 6 | 1.054× | 1.011× | 1.107× |
| perf_ttl_write | 2 | 1.000× | 0.999× | 1.002× |
| perf_write | 2 | 1.014× | 0.991× | 1.037× |

## 逐配置结果

| 家族 / 阶段 | N | 线程 | 到期% | 基线 ops/s | 候选 ops/s | 变化 | 交替复测吞吐比 |
|---|---:|---:|---:|---:|---:|---:|---:|
| perf_combined_gc / ttl_gc_with_statistics | 10000 | 1 | 0 | 834556.00 | 644333.00 | -22.8% | 0.915× |
| perf_combined_gc / ttl_gc_with_statistics | 10000 | 1 | 50 | 813286.00 | 776565.00 | -4.5% | - |
| perf_combined_gc / ttl_gc_with_statistics | 10000 | 1 | 100 | 1619720.00 | 1933710.00 | +19.4% | - |
| perf_combined_gc / ttl_gc_with_statistics | 100000 | 1 | 0 | 784254.00 | 1067400.00 | +36.1% | - |
| perf_combined_gc / ttl_gc_with_statistics | 100000 | 1 | 50 | 2000030.00 | 1834110.00 | -8.3% | 0.925× |
| perf_combined_gc / ttl_gc_with_statistics | 100000 | 1 | 100 | 4402610.00 | 4362680.00 | -0.9% | - |
| perf_combined_stats_under_write / stats_under_ttl_write | 10000 | 1 | 0 | 19356900.00 | 19539000.00 | +0.9% | - |
| perf_combined_stats_under_write / stats_under_ttl_write | 10000 | 1 | 50 | 19581700.00 | 19980400.00 | +2.0% | - |
| perf_combined_stats_under_write / stats_under_ttl_write | 10000 | 1 | 100 | 18743800.00 | 19564800.00 | +4.4% | - |
| perf_combined_stats_under_write / stats_under_ttl_write | 10000 | 8 | 0 | 17603500.00 | 17582800.00 | -0.1% | - |
| perf_combined_stats_under_write / stats_under_ttl_write | 10000 | 8 | 50 | 17609800.00 | 17339500.00 | -1.5% | - |
| perf_combined_stats_under_write / stats_under_ttl_write | 10000 | 8 | 100 | 17531800.00 | 17821300.00 | +1.7% | - |
| perf_combined_stats_under_write / stats_under_ttl_write | 100000 | 1 | 0 | 20279900.00 | 20763300.00 | +2.4% | - |
| perf_combined_stats_under_write / stats_under_ttl_write | 100000 | 1 | 50 | 20408400.00 | 20521800.00 | +0.6% | - |
| perf_combined_stats_under_write / stats_under_ttl_write | 100000 | 1 | 100 | 20418100.00 | 20725700.00 | +1.5% | - |
| perf_combined_stats_under_write / stats_under_ttl_write | 100000 | 8 | 0 | 17538100.00 | 17534900.00 | -0.0% | - |
| perf_combined_stats_under_write / stats_under_ttl_write | 100000 | 8 | 50 | 17498800.00 | 17537000.00 | +0.2% | - |
| perf_combined_stats_under_write / stats_under_ttl_write | 100000 | 8 | 100 | 17393500.00 | 17643000.00 | +1.4% | - |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 10000 | 1 | 0 | 486143.00 | 478053.00 | -1.7% | - |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 10000 | 1 | 50 | 485291.00 | 480398.00 | -1.0% | - |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 10000 | 1 | 100 | 474813.00 | 487203.00 | +2.6% | - |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 10000 | 8 | 0 | 139899.00 | 140965.00 | +0.8% | - |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 10000 | 8 | 50 | 140780.00 | 141177.00 | +0.3% | - |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 10000 | 8 | 100 | 140947.00 | 141085.00 | +0.1% | - |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 100000 | 1 | 0 | 272489.00 | 265832.00 | -2.4% | - |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 100000 | 1 | 50 | 272193.00 | 266223.00 | -2.2% | - |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 100000 | 1 | 100 | 280252.00 | 301814.00 | +7.7% | - |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 100000 | 8 | 0 | 139170.00 | 138691.00 | -0.3% | - |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 100000 | 8 | 50 | 139559.00 | 138462.00 | -0.8% | - |
| perf_combined_stats_under_write / ttl_put_with_stats_queries | 100000 | 8 | 100 | 138977.00 | 138284.00 | -0.5% | - |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 10000 | 1 | 0 | 495375.00 | 495337.00 | -0.0% | - |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 10000 | 1 | 50 | 490793.00 | 495968.00 | +1.1% | - |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 10000 | 1 | 100 | 490707.00 | 498007.00 | +1.5% | - |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 10000 | 8 | 0 | 147193.00 | 147399.00 | +0.1% | - |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 10000 | 8 | 50 | 147870.00 | 147929.00 | +0.0% | - |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 10000 | 8 | 100 | 147590.00 | 147144.00 | -0.3% | - |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 100000 | 1 | 0 | 272519.00 | 266659.00 | -2.2% | - |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 100000 | 1 | 50 | 272768.00 | 266241.00 | -2.4% | - |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 100000 | 1 | 100 | 287032.00 | 299835.00 | +4.5% | - |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 100000 | 8 | 0 | 145521.00 | 145631.00 | +0.1% | - |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 100000 | 8 | 50 | 146369.00 | 145935.00 | -0.3% | - |
| perf_combined_stats_under_write / ttl_put_without_stats_queries | 100000 | 8 | 100 | 145814.00 | 145210.00 | -0.4% | - |
| perf_concurrent / parallel_put | 10000 | 1 | 50 | 505803.00 | 500667.00 | -1.0% | - |
| perf_concurrent / parallel_put | 10000 | 8 | 50 | 147050.00 | 146467.00 | -0.4% | - |
| perf_concurrent / parallel_put | 100000 | 1 | 50 | 505117.00 | 499505.00 | -1.1% | - |
| perf_concurrent / parallel_put | 100000 | 8 | 50 | 145469.00 | 144412.00 | -0.7% | - |
| perf_read / get_hit | 10000 | 1 | 50 | 1166360.00 | 1194950.00 | +2.5% | - |
| perf_read / get_hit | 100000 | 1 | 50 | 849050.00 | 919464.00 | +8.3% | - |
| perf_read / get_miss | 10000 | 1 | 50 | 4799250.00 | 4989830.00 | +4.0% | - |
| perf_read / get_miss | 100000 | 1 | 50 | 4810180.00 | 4958700.00 | +3.1% | - |
| perf_reopen / open | 10000 | 1 | 50 | 240.17 | 248.18 | +3.3% | - |
| perf_reopen / open | 100000 | 1 | 50 | 229.39 | 224.10 | -2.3% | - |
| perf_scan / scan | 10000 | 1 | 50 | 2675380.00 | 2642810.00 | -1.2% | - |
| perf_scan / scan | 100000 | 1 | 50 | 2630800.00 | 2603480.00 | -1.0% | - |
| perf_stats / stats_query | 10000 | 1 | 50 | 1976820.00 | 1972300.00 | -0.2% | - |
| perf_stats / stats_query | 100000 | 1 | 50 | 1868360.00 | 1859350.00 | -0.5% | - |
| perf_stats_under_write / put_with_stats_queries | 10000 | 1 | 50 | 61866.40 | 62015.60 | +0.2% | - |
| perf_stats_under_write / put_with_stats_queries | 10000 | 8 | 50 | 52881.80 | 50956.10 | -3.6% | - |
| perf_stats_under_write / put_with_stats_queries | 100000 | 1 | 50 | 229479.00 | 223949.00 | -2.4% | - |
| perf_stats_under_write / put_with_stats_queries | 100000 | 8 | 50 | 138755.00 | 138396.00 | -0.3% | - |
| perf_stats_under_write / put_without_stats_queries | 10000 | 1 | 50 | 62579.20 | 65265.80 | +4.3% | - |
| perf_stats_under_write / put_without_stats_queries | 10000 | 8 | 50 | 53346.70 | 51142.50 | -4.1% | - |
| perf_stats_under_write / put_without_stats_queries | 100000 | 1 | 50 | 229563.00 | 224267.00 | -2.3% | - |
| perf_stats_under_write / put_without_stats_queries | 100000 | 8 | 50 | 145513.00 | 145114.00 | -0.3% | - |
| perf_stats_under_write / stats_under_write | 10000 | 1 | 50 | 23622500.00 | 23596900.00 | -0.1% | - |
| perf_stats_under_write / stats_under_write | 10000 | 8 | 50 | 22019900.00 | 22201100.00 | +0.8% | - |
| perf_stats_under_write / stats_under_write | 100000 | 1 | 50 | 22170400.00 | 23502500.00 | +6.0% | - |
| perf_stats_under_write / stats_under_write | 100000 | 8 | 50 | 19579800.00 | 19753600.00 | +0.9% | - |
| perf_ttl_gc / ttl_full_gc | 10000 | 1 | 50 | 3065910.00 | 2820740.00 | -8.0% | 0.971× |
| perf_ttl_gc / ttl_full_gc | 100000 | 1 | 50 | 4478160.00 | 4463270.00 | -0.3% | - |
| perf_ttl_mixed / ttl_mixed_get | 10000 | 1 | 0 | 1099780.00 | 1217060.00 | +10.7% | - |
| perf_ttl_mixed / ttl_mixed_get | 10000 | 1 | 50 | 1337920.00 | 1358370.00 | +1.5% | - |
| perf_ttl_mixed / ttl_mixed_get | 10000 | 1 | 100 | 1539220.00 | 1556040.00 | +1.1% | - |
| perf_ttl_mixed / ttl_mixed_get | 100000 | 1 | 0 | 936862.00 | 981688.00 | +4.8% | - |
| perf_ttl_mixed / ttl_mixed_get | 100000 | 1 | 50 | 1016250.00 | 1089850.00 | +7.2% | - |
| perf_ttl_mixed / ttl_mixed_get | 100000 | 1 | 100 | 1164450.00 | 1251100.00 | +7.4% | - |
| perf_ttl_write / ttl_put | 10000 | 1 | 50 | 502328.00 | 501815.00 | -0.1% | - |
| perf_ttl_write / ttl_put | 100000 | 1 | 50 | 486027.00 | 486821.00 | +0.2% | - |
| perf_write / put | 10000 | 1 | 50 | 517459.00 | 512764.00 | -0.9% | - |
| perf_write / put | 100000 | 1 | 50 | 477944.00 | 495751.00 | +3.7% | - |

## 独立诊断微基准

下表使用内存文件和无压缩编码，非官方评分；冷热轮次分别为第一、第二轮 4000 次查询，时间关闭 Profiler。主数据与索引查找各使用 4 MiB 有效缓存，5 次重复取中位数。

| key 数 / 每 key 版本数 | SST 增量 | 构建耗时比（候选/基线） | 冷查询加速 | 暖查询加速 | 索引暖轮块读数 | 缓存 charge |
|---|---:|---:|---:|---:|---:|---:|
| 10000 / 8 | +2.18% | 1.089× | 1.953× | 1.926× | 0 | 267638 B |
| 100000 / 1 | +16.87% | 1.518× | 0.664× | 1.202× | 0 | 2673990 B |

微基准的优势来自计数结构比完整主数据更紧凑；不能据此推断所有官方写入、扫描或 GC 都加速。第三轮开启 Profiler 的数据只作计数诊断。

## 解释范围

SST 计数树需要额外编码、校验、目录和输出字节；普通写入的上界/过滤器已能排除大量新 key，因此并非每次 PUT 都能从索引获益。
存在性提前停止更适合重复写热点；冷索引查找仍需目录和叶子 I/O。GC 在索引构建与归零判断之间交换成本。重开额外读取一个很小的描述符块，不加载整个计数目录。
超过 5% 下降的完整配置追加 5 对交替复测。两个表中的差异提示噪声或真实代价，需要同时查看逐配置结果；不隐瞒回退项，也不预先承诺此方案优于原版本。

代码结构、缓存边界、兼容与故障策略见 `SST_LOCAL_COUNTS.md`。完整逐配置 p99、吞吐范围、进程峰值 RSS、阶段追加字节与目录体积见 `configurations.csv`。RSS 是整个进程峰值，不能当成索引自身的内存占用。

## 正确性与复现核对

- 最终生产代码的原生全量测试：340 项，338 通过、2 跳过；跳过的是环境缺少 Snappy/Zstd 的压缩测试。
- 新增 8 项索引定向测试全部通过；完整原生构建另外启用了 2 项故障注入测试，也全部通过。
- 完整矩阵之后仅补充了实际缓存淘汰的测试断言，重新编译并运行全部 8 项索引定向测试通过；没有改变生产代码。
- 基线与最终候选均为 280/280 官方场景运行通过；最终候选的原始回归、C API 和 Env 回归均通过。
- 基线与候选的官方 suite 和 regression 源码 SHA-256 清单相同；最终生产代码与候选性能快照匹配。
- 日志及机器可读校验结果：`build/sst-local-count-comparison/{native-final.log,targeted-final.log,correctness-final.json}`；本目录也保存 `correctness.json`。
- `baseline`、`candidate`、`baseline-validated` 等早期目录仅为探索记录，本报告使用 `baseline-final` 与 `candidate-final`；最终矩阵和复测期间未并发编译。

## 结论与后续优先级

本轮完成的是持久化局部计数索引及有界查询路径，并非所有官方指标的性能升级。完整矩阵中，读家族几何吞吐比为 1.044×，写入为 1.014×；写入下统计基本持平。正向变化未经专门的正向交替复测，不能据此保证稳定收益。

确认需要优先处理的是联合 GC：10k、0% 到期配置的交替吞吐比为 0.915×（约下降 8.5%），100k、50% 到期为 0.925×（约下降 7.5%）。10k TTL GC 初测下降约 8.0%，交替复测为 0.971×（约下降 2.9%），未持续越过 5% 阈值。完整矩阵中的更大 GC 涨跌不能脱离复测单独解释。

诊断结果揭示代价：100k 单版本无压缩数据的 SST 增大 16.87%，构建耗时约为原来的 1.518 倍；冷启动计数轮吞吐较低，暖轮约快 20.2%。10k、每 key 8 个版本的 SST 只增大 2.18%，构建耗时约增 8.9%，暖轮快约 92.6%。这说明收益依赖重复版本密度和缓存命中，不能把暖计数微基准的提升直接当成数据库整体提升。

下一步优先级：

1. 降低索引构建与编码成本，重点评估常见 count=1 的更紧凑表示、键边界编码，以及索引输出引起的额外主数据 Flush。任何格式调整都需要版本区分和损坏回退测试。
2. 降低冷块首次结构校验和复制成本，并减少缓存中为最大键边界预留的保守容量；保持实际计费和超长 key 的旁路约束。
3. 保留普通写入的存在性提前停止；不要重新用全量重数求和判断 D，也不要为了热查询收益恢复全量逐 key 常驻映射。

建议先处理已确认的 GC 回退，再决定是否合入 Project1。当前改动按计划保留在独立 feature 分支；TTL 的可见性、快照时间和绝对到期时间规则保持原有实现。
