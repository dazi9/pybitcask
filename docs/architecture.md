# 架构设计与核心算法详解 (`PyBitcask`)

本文档详细阐述 `PyBitcask` 的整体系统架构、模块边界、磁盘二进制协议格式、核心数据流与关键算法设计决策。

---

## 1. 系统整体架构图

```text
+-----------------------------------------------------------------------------------+
|                            Client / Access Layer                                  |
|  +-------------------------+  +-----------------------+  +---------------------+  |
|  | Python SDK API          |  | CLI & Interactive     |  | HTTP REST Server    |  |
|  | (BitcaskEngine /        |  | REPL Shell            |  | (ThreadingHTTP      |  |
|  |  DiskStorage / Dict API)|  | (pybitcask.cli)       |  |  Server / JSON API) |  |
|  +------------+------------+  +-----------+-----------+  +----------+----------+  |
+---------------|---------------------------|-------------------------|-------------+
                +---------------------------+-------------------------+
                                            |
                                            v
+-----------------------------------------------------------------------------------+
|                         Core Engine Layer (engine.py)                             |
|  - Thread Synchronization (threading.RLock) & Process Lock (bitcask.lock)         |
|  - CRUD / BatchWrite / JSON Document API / Prefix & Range Scans                   |
|  - Segment Rotation Coordinator & Online Garbage Collector (compact / merge)      |
+-----------------+----------------------------------+------------------------------+
                  |                                  |
                  v                                  v
+-----------------------------------+  +--------------------------------------------+
|   In-Memory Index & Cache Layer   |  |       Storage & Serialization Layer        |
|                                   |  |                                            |
|  +-----------------------------+  |  |  +--------------------------------------+  |
|  | KeyDir (keydir.py)          |  |  |  | Binary Protocol Codec (format.py)    |  |
|  | - dict[str, KeyEntry]       |  |  |  | - Bitcask v2 Record (29B Header+CRC) |  |
|  | - _ttl_keys: set[str] O(T)  |  |  |  | - Hint Record (49B Header+CRC)       |  |
|  | - _dead_bytes_by_file       |  |  |  | - CaskDB v1 Legacy Codec (12B)       |  |
|  +-----------------------------+  |  |  +--------------------------------------+  |
|                                   |  |                     |                      |
|  +-----------------------------+  |  |                     v                      |
|  | LRUCache (cache.py)         |  |  |  +--------------------------------------+  |
|  | - OrderedDict[key, (val,exp)|  |  |  | Segment Manager (segment.py)         |  |
|  | - O(1) get / put / evict    |  |  |  | - Active Segment (Append-Only Write) |  |
|  | - TTL & max_bytes aware     |  |  |  | - Immutable Segments (os.pread Read) |  |
|  +-----------------------------+  |  |  | - Hint Loader & Crash Tail Truncator |  |
|                                   |  |  +--------------------------------------+  |
+-----------------------------------+  +--------------------------------------------+
                                                             |
                                                             v
+-----------------------------------------------------------------------------------+
|                               Disk Storage Layout                                 |
|  bitcask_data/                                                                    |
|  ├── bitcask.lock             (Process PID Lock)                                  |
|  ├── data_00000001.bitcask    (Immutable Segment #1)                              |
|  ├── data_00000001.hint       (Compact Startup Hint Index for Segment #1)         |
|  ├── data_00000002.bitcask    (Immutable Segment #2)                              |
|  ├── data_00000002.hint       (Compact Startup Hint Index for Segment #2)         |
|  └── data_00000003.bitcask    (Active Writable Segment #3)                        |
+-----------------------------------------------------------------------------------+
```

---

## 2. 模块职责与边界划分

| 模块路径 | 核心职责 | 对外暴露接口 / 类 |
| :--- | :--- | :--- |
| `src/pybitcask/exceptions.py` | 定义统一异常继承体系，避免裸抛内置异常 | `BitcaskError`, `CorruptDataError`, `ValidationError`, `ConfigurationError`, `DatabaseClosedError`, `DatabaseLockedError`, `KeyNotFoundError` |
| `src/pybitcask/config.py` | 集中管理引擎配置、边界校验、JSON 配置文件读写与环境变量注入 | `BitcaskConfig` |
| `src/pybitcask/logger.py` | 提供结构化、幂等的日志初始化器，替代原项目硬编码的 `print` | `get_logger` |
| `src/pybitcask/format.py` | 纯函数式二进制序列化与反序列化，无任何磁盘或内存状态依赖 | `encode_record`, `decode_record`, `encode_hint_record`, `decode_hint_record`, `encode_kv`, `decode_kv`, `KeyEntry`, `DecodedRecord`, `HintRecord` |
| `src/pybitcask/keydir.py` | 内存哈希表索引管理、$O(T)$ TTL 过期键追踪、死字节核算与有序扫描 | `KeyDir`, `current_time_ms` |
| `src/pybitcask/cache.py` | 基于 `OrderedDict` 的线程安全 $O(1)$ LRU 热点读缓存 | `LRUCache` |
| `src/pybitcask/segment.py` | 单个日志段文件的顺序追加写、`os.pread` 随机读、尾损坏恢复与 `.hint` 读写 | `DataSegment`, `segment_file_path`, `hint_file_path` |
| `src/pybitcask/engine.py` | 顶层存储引擎协调器：整合缓存、索引、多段轮转、批量写与在线 Compaction | `BitcaskEngine`, `DiskStorage`, `BatchOperation` |
| `src/pybitcask/memory_store.py` | 具备相同 API 契约的纯内存存储实现，用于对照测试 | `MemoryStorage` |
| `src/pybitcask/server.py` | 基于 `ThreadingHTTPServer` 的 RESTful JSON 接口服务 | `BitcaskHTTPServer`, `BitcaskRequestHandler` |
| `src/pybitcask/cli.py` | 命令行参数解析与交互式 Redis 风格 REPL 终端 | `main`, `run_repl`, `build_parser` |

---

## 3. 磁盘二进制协议设计 (`format.py`)

### 3.1 数据记录格式（Bitcask v2 Data Record，29 字节定长 Header）

```text
+------------+---------------+-----------+---------------+--------------+----------------+---------+-----------+
| crc32 (4B) | timestamp(8B) | flags(1B) | expire_at(8B) | key_size(4B) | value_size(4B) |   key   |   value   |
|   uint32   |    uint64     |   uint8   |    uint64     |    uint32    |     uint32     | (UTF-8) |  (bytes)  |
+------------+---------------+-----------+---------------+--------------+----------------+---------+-----------+
|<----------------------------------- 29 Bytes Header ---------------------------------->|<-- Variable Size -->|
```

- **字节序**：统一采用跨平台标准的小端序（Little-Endian, `struct` 格式串 `<IQBQII`）。
- **`crc32` (4B)**：对 `header[4:29] + key_bytes + value_bytes` 计算 IEEE 802.3 CRC32 校验和（`zlib.crc32`），任何单比特翻转（Bit-rot）或截断均可被 100% 检出。
- **`timestamp` (8B)**：毫秒级 Unix 时间戳（相比原项目 4B 秒级时间戳，解决了 2038 年溢出问题与同一秒内高频更新的时序精度问题）。
- **`flags` (1B)**：位标志字段：
  - `0x00` (`FLAG_NORMAL`)：正常写入记录
  - `0x01` (`FLAG_TOMBSTONE`)：删除墓碑记录（`value_size == 0`）
  - `0x02` (`FLAG_HAS_TTL`)：带有过期时间 `expire_at`
- **`expire_at` (8B)**：毫秒级绝对过期时间戳（`0` 表示永不过期）。

### 3.2 Hint 索引文件记录格式（`.hint`，49 字节定长 Header）

```text
+-----------+---------------+----------+---------------+-------------+--------------+---------------+---------------+----------------+---------+
| crc32(4B) | timestamp(8B) | flags(1B)| expire_at(8B) | key_size(4B)| value_pos(8B)| value_size(4B)| record_pos(8B)| record_size(4B)|   key   |
+-----------+---------------+----------+---------------+-------------+--------------+---------------+---------------+----------------+---------+
```

- `.hint` 文件**不存储任何 Value 数据**，直接记录每个活跃 Key 在对应 `.bitcask` 数据文件中的精确偏移量 `value_pos`、`record_pos` 及长度。
- 启动时只需一次 `read_bytes()` 将 `.hint` 读入内存即可瞬间重建 `KeyDir`，无需扫描动辄数十 MB 的历史 Value。

---

## 4. 核心算法与关键设计决策

### 4.1 为什么采用 `Append-Only Log` + `In-Memory KeyDir`？
传统基于 B+ 树的存储引擎在随机写入时需要原地修改磁盘页（引发多次随机磁盘 I/O 与页分裂）。Bitcask 将所有写入（包括新增、更新、删除）转化为**向活跃段文件末尾顺序追加写入（Sequential Append）**，并在内存哈希表 `KeyDir` 中保存 `key -> (file_id, record_pos, value_pos, value_size)`：
- **写入复杂度**：$O(1)$ 内存哈希更新 + 1 次顺序磁盘追加写，吞吐量极高。
- **读取复杂度**：$O(1)$ 内存哈希查找 + 最多 **1 次磁盘寻道（`os.pread`）**（命中 `LRUCache` 时为 **0 次磁盘 I/O**）。

### 4.2 在线垃圾回收算法（Merge / Compaction）
由于更新与删除均采用追加写，旧版本记录与墓碑记录会成为无效空间（Dead Bytes）。`BitcaskEngine.compact()` 的核心流程如下：
1. **封存活跃段**：若当前活跃段包含死字节或存在多个段文件，先调用 `_rotate_active_segment()` 将活跃段转为只读不可变段（Immutable Segment）。
2. **筛选存活记录**：遍历 `KeyDir` 中未过期的所有存活键，筛选出位于待压缩候选段集合中的 `(key, entry)`。
3. **顺序重写与生成 Hint**：从原段读取最新 Value，顺序写入新分配的压缩段文件 `data_<new_id>.bitcask`，并在满 `max_segment_size` 时自动切分；同时为每个新压缩段生成 `.hint` 索引文件。
4. **原子切换索引与清理旧文件**：在引擎锁保护下调用 `key_dir.update_compacted_entry` 将内存指针切换至新段文件，关闭并删除旧的 `.bitcask` 与 `.hint` 文件。

### 4.3 崩溃尾记录自愈算法（Crash Tail Recovery）
当进程在写入过程中遭遇断电或 `SIGKILL` 时，活跃段文件末尾可能残留半条记录（Header 不足 29 字节、Payload 长度不足或末尾记录 CRC32 校验失败）：
- 在启动扫描 `_scan_data_file` 时，若异常发生在**文件末尾最后一条记录**且配置 `recover_tail=True`，引擎会记录告警日志并调用 `os.truncate(path, last_valid_offset)` 将损坏尾部截断至上一条合法记录边界，保证数据库自动恢复可用。
- 若 CRC32 错误发生在**文件中间记录**（说明发生了磁盘静默位翻转 Bit-rot），则严格抛出 `CorruptDataError`，绝不静默吞掉数据损坏。
