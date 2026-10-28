# TTL 第一阶段实现与验证记录

验证日期：2026-10-28。

本文件保留第一阶段的改动与测试结果，不代表最终版本的全部验证。后续缺陷修复和三轮标准评测见 [TTL 补充测试与修复记录](TTL_ADDITIONAL_TESTS.md)。

## 工作范围

在 `LevelDB_Project1_GitHub - 副本` 中修改和测试，未修改原始 GitHub 克隆目录。仓库已有 TTL 主体实现，本阶段补充批量写入接口、修复范围回收问题，并增加验证。

## 实现改动

- 公开 `WriteBatch::PutWithExpiration()`，支持将 TTL 写入、普通写入和删除放入同一原子批次。截止时间采用 `Env::NowMicros()` 的绝对微秒单位；零表示已过期，不表示永久有效。
- 最底层范围重写只选择与请求范围重叠的 SST 文件。
- 初始化输出文件编号，避免全部记录过期、不产生输出 SST 时使用未初始化值。新增范围清理测试曾复现该问题。
- 新增四个参数化 TTL 测试，在性能剖析开启和关闭时分别执行，共八次。
- 将运行器校验的打包回归源码固定为 LF 换行，以满足逐字节哈希校验；未修改其测试逻辑和断言。

## 第一阶段验证结果

| 验证项目 | 结果 |
| --- | --- |
| WSL Ubuntu 22.04 下的 Debug 构建（CMake、GCC） | 成功 |
| `TTLMetadataTest` | 26/26 通过，包含新增的八次执行 |
| 课程 `ttl_test` | 2/2 通过 |
| 课程 `metadata_test` | 3/3 通过 |
| CTest | 3/3 个测试程序通过 |
| `leveldb_tests` | 245 项通过，2 项跳过 |
| 公开 `combined quick` 工作负载 | 22/22 通过 |
| 同一最终 Release 程序在 Linux `/tmp` 下重跑原生回归 | 209 项通过，2 项压缩测试跳过，约 125 秒 |
| 打包的 C API、环境回归程序 | 通过 |
| `git diff --check` | 通过 |

Quick 配置：10,000 个键、4 个线程、重复一次，随机种子为 `7774596`。

## 首次完整评测超时说明

首次 benchmark 输出位于 `/mnt/d`，原生主回归超过默认的 300 秒超时限制，其中 `SparseMerge` 单项约耗时 202 秒；C API 和环境回归通过。虽然全部 22 个工作负载通过，`build/ttl-validation-final/summary.json` 仍如实保留整体 `fail` 状态，没有修改记录掩盖超时。

随后使用同一个主回归可执行文件，将工作目录和 `TEST_TMPDIR` 放在 `/tmp/leveldb-ttl-fixed-regression` 后重跑，程序成功退出。WSL 下完整评测宜将数据库和输出放在 Linux 文件系统。后续验证改用 Linux 持久目录，避免临时目录清理造成日志丢失。

本阶段的 Quick 验证不属于三轮标准性能评测；后续 Standard 结果另见补充记录。

## 设计选择与 PDF 方案的关系

- 零 TTL 的语义采用课程要求：`ttl=0` 写入立即过期的新版本；普通 `Put` 永不过期。
- 保留仓库既有 SST 元数据摘要和直方图实现。摘要只用于选择回收候选，不能代替实际删除时的安全判断。
- 本次 TTL 改动没有引入新的辅助 LSM，也没有增加 MANIFEST 摘要格式。

## 本地证据

以下路径相对于仓库根目录，属于本地构建输出，不随代码提交：

- `build/ttl-validation-final/summary.json`
- `build/ttl-test-logs/leveldb-ttl-focused.log`
- `build/ttl-test-logs/leveldb-ttl-base.log`
- `build/ttl-test-logs/leveldb-metadata-base.log`
- `build/ttl-test-logs/leveldb-ttl-ctest.log`
- `build/ttl-test-logs/leveldb-ttl-fixed-regression.log`
- `build/ttl-test-logs/leveldb-ttl-fixed-c.log`
- `build/ttl-test-logs/leveldb-ttl-fixed-env.log`
