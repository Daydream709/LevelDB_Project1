# 正确性与稳定性强化测试报告

基线：Project1 `ca82bdc`。六个阶段均已执行；覆盖范围和限制见下文。结果不意味着所有未知隐藏测试一定通过。

## 发现：TTL 后台回收无进展时重复重写

固定种子 0 在操作 6200 后等待后台空闲时无法结束。受控重跑日志证明快照释放结束，阻塞在等待后台空闲；数据库日志显示短时间持续生成和删除 SST。复现日志为 `model-seed0-diagnostic.log`、`rewrite-loop-LOG` 与 `rewrite-loop-MANIFEST`。

修复：后台 TTL 任务成功但物理记录总数无变化时，对当前 Version 的自动扫描短暂退避；版本改变、手动维护、MemTable Flush 和普通 Compaction 不受该退避限制。处理时钟达到 UINT64_MAX 的边界。

同一种子在修复后完成 10000 次操作，结果见 `model-seed0-fixed.log`；相关 TTL 调度、缓存及手动 Compaction 回归 14/14 通过，详见 `ttl-cache-regression.log`。

进一步分析：保存的 SST 中存在已被最新永久值遮蔽的过期旧版本。底层重写此前在最新值仍存活时保留了同 key 的所有旧版本，TTL 调度器因旧版本再次选择该文件，形成重写循环。现在在没有快照且 key 位于重写范围内时，只保留仍需保留的最新版本；额外保留无进展调度短退避作为保护。新增 `BottomRewriteReclaimsExpiredVersionHiddenByLiveValue` 在两种 profiling 配置下通过（`bottom-fix-test.log`）。

缩小后的回归已与 `ca82bdc` 独立构建对照：基线在 5 秒后台空闲断言失败（`bottom-baseline-test.log`）；修复后同一测试两个 profiling 配置通过（`bottom-fix-test.log`）。基线测试使用独立源码副本及数据库目录。

## 第一、二阶段进展

底层回收修复后的模型对照：20 个种子、每个 10000 次操作，在普通与启用 profiler 两种配置均通过（`model-bottom-fix-0.log`、`model-bottom-fix-1.log`）。日志记录种子；打开 `LEVELDB_ROBUST_LOG` 可记录每次操作，失败种子的诊断日志已保存。受控并发与多线程 batch/统计测试：默认 CPU 12/12、单 CPU 4/4 通过（`concurrent-default.log`、`concurrent-single-cpu.log`），线程数覆盖 1、2、4、8、16、32。

## 第三阶段：崩溃注入进展

新增独立子进程故障点：`wal_partial`、`wal_flushed`、`wal_synced`、`sst_synced`、`manifest_before_sync`、`manifest_synced`。每个点由子进程发出命中确认，父进程仅终止该子进程，再检查 batch 完整性、TTL、统计和重开。六个点分别在两种 profiling 配置下命中；包含原有 WAL/SST 杀进程测试共 6/6 测试通过，见 `crash-boundaries.log`。

Compaction 与 GC 又补充 6 个可确认命中的故障点：各自的 SST 输出同步、Manifest 同步前、Manifest 同步后；两个 profiling 配置全部命中并通过恢复检查，详见 `crash-compaction-boundaries-r3.log`。`crash-compaction-boundaries.log` 和 `-r2.log` 是故障点未命中的测试构造失败：文件没有重叠而未产生所需的 Compaction 输出；调整重叠数据后命中，不能把前两次当通过。

中间 WAL 校验错误新增跨日志块测试：前一完整事务和下一块事务恢复，损坏事务跳过，统计以实际恢复记录核对；两个 profiling 配置均通过（`wal-middle-corruption-r2.log`）。最初同块后续记录假设失败（`wal-middle-corruption.log`），确认原有 log Reader 在校验失败时丢弃该块余下数据后，改为跨块验证；没有修改数据库恢复策略。崩溃点、已有文件错误、损坏、恢复和 GC 故障组合回归 46/46 通过（`crash-fault-regression.log`）。

## 第四、五阶段

TTL/GC 组合回归 30/30 通过（`ttl-gc-boundaries.log`），包括多个快照时间、迭代器时间、覆盖、到期边界、溢出、重开、GC 被视图阻塞与释放后的物理回收。大 batch、特殊 key、局部索引溢写、损坏和缺失回退、缓存淘汰以及主数据错误传播 22/22 通过（`batch-index-regression.log`）；课程元数据中的四个大 batch、compaction 溢写和自定义比较器用例 4/4 通过（`batch-metadata-course.log`）。单独新增同 key 横跨多个 SST、阻塞中的 immutable MemTable 和活动 MemTable 的测试，详见 `multilayer-count.log`。

## 第六阶段：长期压力

可显式启用 `DISABLED_MixedWorkloadSoak`，通过 `LEVELDB_SOAK_SECONDS` 指定时间；12 秒预检通过。默认 CPU 与单 CPU 各一轮连续 1200 秒均已通过，分别完成 2244088、2230532 次写入，各 120 次 GC、重开、参考模型及物理统计检查；快照扫描分别 115735、110054 次。详情见 `soak-default.log` 和 `soak-single-cpu.log`。每轮检查文件句柄数均为 11，WAL 和 SST 均各 1 个。

默认配置首个至最后检查点 RSS 为 7.07 至 7.94 MiB；单 CPU 且开启 profiling 的配置为 7.22 至 30.84 MiB，后期增长明显放缓。`util/profiler.cc` 按数据库实例 ID 在 `databases` 中保留累积历史，该映射没有实例数上限；有界任务事件缓冲也可能影响 RSS。仅凭本次 RSS 曲线不能定量归因，历史保留语义需要后续明确，不能简单删除历史数据而宣称修复。

原生全量测试 363/363 通过（`native-full.log`，两个 disabled 项为上面的长时间测试）；后来新增的多层受控用例单独 2/2 通过（`multilayer-count-r2.log`）。Snappy 与 Zstd 两个压缩测试实际运行并通过（`compression-regression.log`）。老师 combined 的 10 个正确性场景各 3 次、30/30 通过；完整官方矩阵 280/280 次通过，附带 211 项数据库回归、C 接口回归与 7 项 Env 回归通过。结果见 `official-full.log` 及 `official-summary.json`。

## 性能观察

完整矩阵与基线历史数据的 80 个细分配置均匹配，五次吞吐中位数对照见 `performance-comparison.csv`。历史数据并非交替测量；发现几个异常后，又使用独立的 `ca82bdc` 基线和修复版构建、相同种子与参数进行 5 对交替测试（`performance-paired.jsonl`）。下表变化为吞吐量变化：

| 配置 | 基线 ops/s | 修复后 ops/s | 变化 |
| --- | ---: | ---: | ---: |
| TTL GC，10k | 1,934,460 | 1,890,430 | -2.28% |
| 统计结合 TTL GC，100k、过期比例 0% | 1,042,240 | 1,113,540 | +6.84% |
| 重开，10k | 105.69 | 110.42 | +4.47% |
| 重开，100k | 240.57 | 240.13 | -0.18% |
| 写入并发统计，10k：不查统计的写入 | 61,751 | 60,314 | -2.33% |
| 写入并发统计，10k：查询统计时的写入 | 61,162 | 60,479 | -1.12% |
| 写入并发统计，10k：统计查询 | 23,633,200 | 22,953,300 | -2.88% |

例如，TTL GC 10k 的历史对比是 -31.17%，交替复测为 -2.28%，不应把历史差异直接归因于修复。交替测试样本有限，不能证明所有负载没有回退；尤其当实际工作负载包含修复前的无限后台重写时，两版行为不再可等价比较。

## 局限与重跑

进程被杀死验证的是进程崩溃，不等同断电、文件系统缓存丢失或硬件故障；单 CPU 与默认 CPU 长测同时运行，结果用于正确性和稳定性，不能用于性能比较。长期测试采用 256 个热 key、受控时钟及周期性强制 GC，并不代表所有数据分布。profiling 的实例历史映射无容量上限，长寿命共享 profiler 反复打开数据库存在增长风险；可通过为不同生命周期使用独立 profiler 限制其生存期，完整的有界历史策略需单独设计。尚未进行真实断电和持续多小时、大规模不同 key 的测试。

所有测试日志位于 `build/robustness-20261105/`。长测通过显式指定 `--gtest_also_run_disabled_tests --gtest_filter=*DISABLED_MixedWorkloadSoak/0` 或 `/1` 启用，`LEVELDB_SOAK_SECONDS=1200` 指定 20 分钟。启用索引和双压缩库的构建需要对应 Snappy、Zstd 开发库。
