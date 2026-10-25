# 第五部分：项目复盘报告、技术债与大二进阶学习路线

---

## 1. 实战复盘总结

在本次高强度编程实战中，我们从零完成了对经典日志结构存储引擎 `avinassh/py-caskdb` 与 Riak Bitcask 论文的源码级调研、缺陷剖析、独立模块化实现、系统性测试与性能优化：

1. **从“看懂代码”到“发现原项目 Bug”**：
   在第一阶段阅读 `avinassh/py-caskdb` 的 `format.py` 源码时，我们发现其 `encode_kv` 将字符串字符数 `len(key)` 当作 UTF-8 字节数打包进二进制 Header，导致任何中文字符或 Emoji 都会引发严重的解码崩溃。这印证了系统编程中**“字符（Character）不等于字节（Byte）”**的核心铁律。
2. **测试驱动与告警零容忍发现隐藏资源泄漏**：
   在第三阶段执行 `test_mid_file_bit_rot_detected_by_crc32` 时，Python 3.14 抛出了 `ResourceWarning: unclosed file`。通过追踪异常栈，我们定位到当段文件在启动扫描中途因 CRC32 校验失败抛出 `CorruptDataError` 时，`DataSegment` 已打开的文件描述符未在异常路径上关闭。我们在 `engine.py` 中补充了严格的 `try ... except` 资源回收，并用 `-W error` 将所有警告升级为错误进行回归验证。
3. **用实测数据驱动优化，而非凭直觉猜测**：
   在第四阶段基准测试中，我们通过计时数据发现了 `KeyDir.purge_expired` 的 $O(N)$ 全表扫描开销，以及 `close()` 在未修改数据库时重复生成 `.hint` 文件的问题。通过引入 `_ttl_keys` 集合、`_active_dirty` 脏标记与 POSIX `os.pread` 位置读取，热点读吞吐量提升至 **152 万 ops/sec**，冷启动提速 **3.54 倍**。

---

## 2. 当前技术债（Technical Debt）与可扩展方向

| 编号 | 技术债 / 架构局限 | 产生原因 | 后续演进与重构方案 |
| :--- | :--- | :--- | :--- |
| **TD-01** | **全量 Key 驻留内存** | Bitcask 哈希索引模型要求所有活跃 Key 均存储在内存 `KeyDir` 字典中 | 引入分级存储或将冷 Key 索引下沉至磁盘 SSTable + 布隆过滤器（向 LSM-Tree 演进） |
| **TD-02** | **粗粒度引擎级读写锁 (`RLock`)** | 当前 `BitcaskEngine` 使用单个可重入锁保护 `KeyDir` 与活跃段追加写 | 由于只读段使用无状态的 `os.pread`，可将锁拆分为：`KeyDir` 分片读写锁 + 仅针对 `_active_segment` 写入的互斥锁，实现真正的多线程无锁并发读 |
| **TD-03** | **Compaction 期间阻塞写入** | 当前 `compact()` 在持有引擎锁期间完成存活记录迁移与原子切换 | 改为两阶段 Compaction：第一阶段无锁后台扫描不可变段并生成新压缩段；第二阶段仅加短锁切换仍在原段的 KeyEntry |
| **TD-04** | **前缀与范围扫描每次需排序** | `KeyDir` 底层为 Python `dict`（哈希表），`scan_prefix` 需遍历键并排序 | 维护一个同步更新的内存跳表（SkipList）或 `bisect` 有序键数组，支持 $O(\log N + K)$ 原生有序迭代 |

---

## 3. 给计算机专业大二学生的深度学习知识点路线图

结合你在大二上学期正在或即将学习的专业核心课，本项目可以直接作为你深入掌握以下四大核心领域的跳板：

### 领域一：《数据结构与算法》进阶
1. **从哈希表（Bitcask）到 LSM-Tree（LevelDB / RocksDB）**：
   - 思考题：Bitcask 为什么不支持高效的范围查询（Range Scan）且受限于内存容量？
   - 深入方向：学习 **跳表（SkipList）**、**SSTable（Sorted String Table）** 与 **布隆过滤器（Bloom Filter）**，理解 LevelDB 如何用 `MemTable + Immutable MemTable + 多层 SSTable` 解决 Bitcask 的内存瓶颈与范围查询问题。
2. **缓存淘汰算法演进**：
   - 对照本项目的 `src/pybitcask/cache.py`（基于 `OrderedDict` 的 LRU），尝试手写 **LFU（Least Frequently Used）** 与 **W-TinyLFU**，思考全表扫描为什么会导致普通 LRU 发生“缓存污染（Cache Pollution）”。

### 领域二：《操作系统与文件系统》底层原理
1. **缓冲 I/O、Page Cache 与 `fsync` 的真相**：
   - 在本项目中对比 `sync_policy="always"` 与 `sync_policy="os"` 的代码差异，理解 `file.write()`（用户态缓冲区） $\rightarrow$ `file.flush()`（内核态 Page Cache） $\rightarrow$ `os.fsync()`（落盘持久化）三层边界。
2. **系统调用 `lseek + read` vs `pread` vs `mmap`**：
   - 理解为什么多线程共用同一个文件描述符（File Descriptor）时，`lseek + read` 会产生竞态条件（Race Condition），而 `os.pread` 能够在内核态原子完成偏移量读取。

### 领域三：《计算机网络与分布式系统》前瞻
1. **二进制协议设计（Wire & Disk Protocol）**：
   - 掌握定长 Header + 变长 Payload（TLV 模式）、字节序（Little-Endian vs Big-Endian）、CRC32 校验和、位标志（Bitflags）的设计范式，告别只会用 JSON 传数据的阶段。
2. **从单机存储到分布式 KV（大三进阶目标）**：
   - 在 `PyBitcask` 的 HTTP Server 之上，学习 **一致性哈希（Consistent Hashing）** 实现数据分片，或学习 **Raft 共识算法** 实现多副本主从复制，将单机 `PyBitcask` 升级为分布式 KV 集群。

### 领域四：推荐精读的经典资料
1. **论文**：Justin Sheehy & David Smith, *Bitcask: A Log-Structured Hash Table for Fast Key/Value Data* (2010, 仅 6 页，极易读懂)。
2. **书籍**：Martin Kleppmann, 《数据密集型应用系统设计》（*Designing Data-Intensive Applications*, DDIA）**第 3 章：存储与检索（Storage and Retrieval）** —— 该章前半部分正是从零推导 Bitcask、Hint 文件、Compaction，并一路引出 LSM-Tree 与 B-Tree！
