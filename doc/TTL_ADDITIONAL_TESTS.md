# TTL 补充测试、缺陷修复与最终验证

验证日期：2026-10-28。工作目录：`LevelDB_Project1_GitHub - 副本`。

第一阶段记录见 [TTL 第一阶段实现与验证记录](TTL_VALIDATION.md)。本文件按补充测试、发现缺陷、修复验证和标准评测的顺序保留历史。最终专项验证和标准矩阵均已通过；下文的四次失败是修复前记录。

## 补充的五类测试

最初仅新增或调整测试、测试构建配置和记录，尚未修改数据库功能实现。以下五类验证新增的 22 次执行全部通过：

1. **真实进程异常退出**：独立 `ttl_crash_writer` 进程完成 `sync=true` 批次，父进程收到成功确认后发送 `SIGKILL`，并通过 `waitpid` 回收子进程。分别覆盖 WAL 和已刷盘 SST，检查重开不续期、批次完整性，以及删除和过期新值对旧值的屏蔽。
2. **同一 SST 内的部分范围回收**：覆盖两个端点、范围内永久值、范围外过期值；范围外过期值仍物理存在但逻辑不可见。分别使用字节序和反向比较器。
3. **回收故障注入**：在普通 Compaction 和最底层重写中分别注入 SST Sync、MANIFEST Append 错误。检查故障确实触发、失败不发布统计或文件状态、重开后可见性正确，以及重试原操作后能够成功回收。
4. **批次编码损坏**：遍历 TTL 记录的全部截断位置，覆盖未知 tag、二进制键值、零和 `uint64` 最大截止时间。检查损坏批次被拒绝，前缀操作不会造成部分写入。
5. **多快照保护**：覆盖相同 sequence 不同时间、不同 sequence 的交叉视图。逐步释放快照时仍保护剩余视图，全部释放后能够回收，且回收不推进 sequence。

相关文件为 `db/ttl_metadata_test.cc`、`db/write_batch_test.cc`、新增的 POSIX 辅助程序 `test/ttl_crash_writer.cc`，以及配置辅助程序构建和依赖的 `CMakeLists.txt`。

## 修复前发现的两个回收缺口

当时专项测试共执行 57 次，53 次通过、4 次失败。四次失败来自以下两个场景的性能剖析开启和关闭版本，不是四个独立缺陷，也不是故障注入一致性测试失败。

### 最高非空层未参与回收

`PublicCompactRangeReclaimsSparseExpiredHighestLevel` 写入 16 个永久键和一个 TTL 键，并刷入唯一的 L2 SST。推进时钟后调用 `CompactRange(nullptr, nullptr)`，预期物理记录数为 16，实际仍为 17；逻辑读取已经返回 `NotFound`。直接调用 `TEST_CompactRange(2, ...)` 后，物理记录数可正确降为 16。

### 同一次调用刚刷盘的数据未参与回收

`PublicCompactRangeReclaimsTTLFlushedByTheSameCall` 使用相同数据，但调用前数据只在 MemTable 中。`CompactRange` 自己刷盘后仍留下过期记录，物理记录数为 17。

根因是 `DBImpl::CompactRange` 先确定最高层再刷盘，且合并循环使用 `level < max_level_with_files`：既有最高层未参与重写，同次刷盘产生的新层级也未纳入计算。低过期比例使机会式后台调度没有掩盖问题。

修复前曾建议将最高非底层直接纳入合并。最终方案增加了 TTL 摘要筛选，避免无条件改变普通数据的层级位置。

## 修复前公开评测结果

`combined quick`：10,000 个键、4 个线程、重复一次，22/22 场景通过。固定版本原生回归为 209 项通过、2 项压缩测试跳过，约 118 秒；C API 和环境回归通过，整体 `status=pass`，无超时。

公开评测没有覆盖上述两个新边界，因此该阶段公开评测通过并不代表缺陷已修复。初次 `/tmp` 运行的日志未能留存，这里的结论采用持久目录 `/home/zhou/ttl-validation-20261028/combined` 重跑后的证据。

本地历史证据保存在 `build/ttl-extended-results/`，包括保留四次失败的 `focused.log` 和 `combined/` 下的汇总及回归日志，未被后续结果覆盖。

## 经批准实施的修复

最终修改位于 `db/db_impl.cc` 的 `DBImpl::CompactRange`：

- 先刷盘，失败时记录错误并退出；成功后才确定实际最高数据层。
- 普通数据沿用原有层级合并策略，不无条件将最高层推到下一层。
- 较低层合并结束后，检查最高非底层目标文件的 TTL 摘要。可能存在过期 TTL 或可选摘要无效时追加合并，不用过期比例门槛阻止显式回收。
- 摘要只选择候选文件，实际删除仍遵守快照、sequence 和旧值屏蔽规则；最底层继续使用范围重写路径。

## 修复后的验证

| 验证项目 | 结果 |
| --- | --- |
| TTL、WriteBatch 专项及原生 `ManualCompaction` 兼容性 | 58/58 通过，原四次失败全部通过 |
| 课程 `ttl_test` | 2/2 通过 |
| 课程 `metadata_test` | 3/3 通过 |
| 公开 `combined quick` | 22/22 场景通过，整体 `status=pass` |
| 固定版本原生回归 | 209 项通过，2 项压缩测试跳过 |
| C API、环境回归 | 通过 |
| `git diff --check` | 通过 |

该阶段为 Quick 验证。Linux 评测目录是 `/home/zhou/ttl-validation-20261028/final-fixed-combined`。已复制回副本的本地证据如下：

- `build/ttl-fixed-results/fix-focused-v2.log`
- `build/ttl-fixed-results/final-course-ttl.log`
- `build/ttl-fixed-results/final-course-metadata.log`
- `build/ttl-fixed-results/combined/summary.json`
- `build/ttl-fixed-results/combined/homework_regression.log`
- `build/ttl-fixed-results/combined/homework_c_regression.log`
- `build/ttl-fixed-results/combined/homework_env_regression.log`

## 后续三轮标准性能评测

修复后另行完成 `standard --matrix` 评测，本轮没有再修改功能代码。

```bash
python3 benchmark/runner.py --kind combined --source . \
  --output /home/zhou/ttl-validation-20261028/v1-standard-matrix \
  --profile standard --matrix --jobs 8 --build-timeout 900
```

使用 WSL Ubuntu 22.04、Release 构建，数据库位于 Linux 文件系统。Standard 参数为 100,000 个键、8 个线程、8 轮、每个配置重复三次，value 为 256 字节。矩阵将适用的性能用例扩展至 10,000/100,000 个键、1/8 个线程和 0%/50%/100% 过期比例；不是每个用例都使用全部组合。三次随机种子为 `7774596`、`7774597`、`7774598`。

最终 22 种用例展开为 56 组配置，共 **168/168 次通过**。原生回归 **209 项通过、2 项压缩测试跳过**；C API 和环境回归通过，最终汇总为 `pass`。

以下为 100,000 条、单线程时三次结果的中位数：

| TTL 场景 | 吞吐量中位数（次/秒） | 三轮吞吐量范围（次/秒） |
| --- | ---: | ---: |
| 写入 | 37,661.7 | 37,436.3–45,633.9 |
| 读取，0% 过期 | 630,184 | 624,256–637,564 |
| 读取，50% 过期 | 630,065 | 611,174–702,980 |
| 读取，100% 过期 | 739,613 | 529,277–786,308 |

全部过期记录的 FullGC 耗时中位数为 **0.915944 秒**，范围为 0.826564–0.930146 秒，并验证物理记录清空。该用例目录中的 `e50` 是通用参数，夹具实际使全部记录过期。

这些结果是当前 V1 的本机基线，未与未修改版本进行同配置对照，不能据此推断相对原版的性能提升。部分场景波动明显。运行器部分计时包含数据生成或校验开销；读写延迟每 64 次抽样一次，GC 只测整次回收耗时，不应混为单条记录的尾延迟。

本地报告与原始证据保存在 `build/ttl-standard-results/`：`TTL_STANDARD_REPORT.txt`、`environment.txt`、`runner.log`，以及 `combined/` 下的 `summary.json`、`aggregate.json`、`metrics.csv` 和逐次日志。Linux 完整输出位于 `/home/zhou/ttl-validation-20261028/v1-standard-matrix`。

本文件列出的 `build/` 日志均为本地构建产物，不随代码提交。
