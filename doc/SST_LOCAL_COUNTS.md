# SST 逐 key 局部计数索引

实现分支：`feature/sst-local-count-index`；起点：`Project1/d59421e`。不自动合并到 Project1。

## 计数语义与提交

`R(k)` 是原始 user key 完整字节串在当前 MemTable、imm 和 Version SST 中的物理记录总数。普通值、删除标记、旧版本和未回收 TTL 均计入。比较器判等但原始字节不同的 key 分别计数。可见性、用户快照和物理时间不参与这个定义。

内存表没有额外逐 key 映射。`MemTable::CountUserKeyRecords` 从原跳表按比较器定位，再按完整字节筛选；`HasUserKeyRecords` 找到第一条记录即停止。两者均返回 Status。`Version` 依据每个文件的比较器范围筛选所有可能来源，包括 L0 重叠和自定义比较器等价边界。

普通写入仅需要确定 D 是否增加，因此 `DBImpl::HasPrimaryRecordsForKey` 依次查 MemTable、imm、SST，命中立即结束。原有主键上界和无假阴性过滤器仍可排除确定不存在的 key。查询仍处于当前状态锁和既有提交顺序内。

Compaction 的 T 变化仍来自实际输入输出记录差量。一般路径通过精确计数判断归零；已完整扫描输入且校验 Version 的底层回收路径，通过排除输入后的存在性判断归零，不构造近似 R。原样 Flush 不改变 T、D。Version、统计检查点和标量的提交协议不变，索引不消耗用户序列号。统计查询仍忽略用户快照，整体返回当前 T、D、S。TTL 编码、到期时间、快照时间和回收规则不变。

## 文件布局

`TableBuilder` 的内部适配器 `LocalCountTableAccess::Enable` 只在主数据输出启用。四条路径是 `db/builder.cc`、`DBImpl` 常规 Compaction、底层重写和 `db/repair.cc`。通用 TableBuilder 和元数据事务临时表不生成该索引。

metaindex 增加 `leveldb.local-counts.v1`，其值为一个 BlockHandle。目标是独立的、小于 64 字节的描述符块：`LCI1`、层高、不同原始键数、物理记录总数、根块位置。描述符与原有 `leveldb.internal-stats.v1` 分开校验。Footer、主数据编码及 Manifest 不变。

`table/local_count_index.{h,cc}` 构建多层树，叶块为 `raw_key -> fixed64 count`，目录块为 `子块最大 key -> BlockHandle`。两者均按 BytewiseComparator 排序，目标块大小 4 KiB；超长键允许超过目标大小。每层只保留一个构建块，目录至少容纳两个条目再分裂，避免超长键导致单子节点无限增高。最多 32 层，溢出报错。通过现有 SST 压缩策略、块 CRC 和 trailer 写入。局部索引写入可能先 flush 当前主数据块；同步推进 filter 的块起点，保持过滤器与数据块偏移一致。

BytewiseComparator 流式汇总相邻原始键。其他 Comparator 使用 `metadata_workspace_size` 限制 map 工作区，按原始字节输出临时有序段。临时段为带 CRC 的顺序记录，不使用加载完整目录的 SST。最多 8 路归并；在线 8 进位合并限制存活段和描述符数量。成功或失败由收集器清理，数据库持锁恢复时清理严格匹配 `<SST文件名>.counts.<数字>.dbtmp` 的孤儿段。单个超长 key、归并当前 key 和树的各层构建块属于必要的瞬时空间，不声称整个进程 RSS 严格等于工作区预算。

## 查找、缓存与回退

打开 SST 只读取小描述符，不预加载计数树目录。`TableCache` 优先用索引计数，叶子缺 key 返回零。缺失、未知格式、校验或结构错误会禁用该 Table 实例的可选索引，整体回退到主数据迭代器。回退验证主数据目录块和数据块 CRC；目录校验在每个表实例内仅执行一次。任何主数据读取错误向上传播，不转换成零。可选索引坏不会单独要求统计重建；真正的检查点/WAL 恢复问题仍由原有机制处理。旧 SST 不强制重写。

`metadata_cache_size` 一半用于局部计数块，另一半分配给既有临时元数据处理的值、块和表缓存。新缓存条目以全局唯一 Table 实例号和块偏移识别；重新打开同一文件或复用文件号不会命中旧实例。缓存保存拥有字节的解压块、一次性结构校验结果和最大键边界。首次检查排序、正计数、目录句柄范围、向前引用和层数；后续复用校验结果。父子边界逐次核验。

缓存 charge 包括块缓冲区容量、验证边界预留容量、对象及共享引用分配。LRU 分 16 片，准入额外要求单条 charge 能放入一个分片，超长块直接读取；否则 LevelDB 的插入时引用可能使单个过大条目留在分片中。每下降一层就释放父块查询引用。缓存自身的 hash/分配器开销、调用栈、主数据块、临时 I/O 缓冲区和正在使用而已淘汰的块不属于 retained cache charge，不能用此值声称总 RSS 上限。

`leveldb.metadata-stats` 的 `memory_bytes` 纳入新缓存 charge，新增 `local_count_cache_bytes` 和 `local_count_cache_budget_bytes`。

## 可观测性与验证

可选 Profiler 新增：`metadata.local_count_index_hit`、`cache_hit`、`cache_miss`、`block_read`、`decoded_bytes`、`cache_admitted_bytes`、`oversize_bypass`、`index_invalid`、`fallback`、`disk_bytes`、`spill_runs`、`merge_runs`（前缀均为 `metadata.local_count_`，fallback 同属此前缀），以及 `metadata.presence_early_stop`。字节计数器是累计流量，当前保留内存看 property；不得混为驻留内存或峰值。

`LocalCountIndexTest` 覆盖多层目录、空键、二进制、超长键、重复版本、删除/TTL、自定义比较器大等价类及多轮溢出、描述符未知版本/损坏、叶子与目录损坏、旧 SST/损坏索引回退、主数据错误传播、溢出、构建失败、临时文件回收、重开缓存身份、冷热缓存与小缓存。原生测试启用此前未默认编入的 `db/fault_injection_test.cc`，同时保留既有 TTL、并发和恢复测试。

`tools/measure_local_counts.cc` 是独立诊断工具，不修改或替代官方 benchmark。它比较同一份记录有/无索引时的 SST 字节数、纯构建 CPU 时间、冷/暖计数查询、块读取和缓存 charge，每种形状重复 5 次。内存源的块读取次数衡量逻辑 I/O，不代表设备延迟；前两轮关闭 Profiler，第三轮开启并输出计数器；第三轮时间只用于诊断，不与官方性能分数直接混用。主数据查询和索引查询均配置 4 MiB 的有效查找缓存。

示例编译（根据构建目录调整路径）：

```sh
c++ -std=c++17 -O3 -pthread -fno-rtti -fno-exceptions \
  -DLEVELDB_PLATFORM_POSIX=1 -I. -Iinclude -Ibuild-release/include \
  -Ithird_party/crc32c/include \
  tools/measure_local_counts.cc build-release/libleveldb.a \
  build-release/third_party/crc32c/libcrc32c.a -o /tmp/measure_local_counts
/tmp/measure_local_counts > local-count-micro.jsonl
```

完整官方对照使用未改动的 runner，Release、standard、矩阵、每配置 5 次。逐配置结果与超过 5% 下降项的复测见本轮性能报告；不能以诊断微基准替代官方全量结果。
