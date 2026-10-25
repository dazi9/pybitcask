# PyBitcask — 基于 Bitcask 论文的日志结构键值存储引擎

`PyBitcask` 是基于 Riak **[Bitcask: A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf)** 论文与开源项目 **[avinassh/py-caskdb](https://github.com/avinassh/py-caskdb)** 独立复现并深度扩展的持久化键值存储引擎。

本项目完全使用 **Python 3 标准库（零第三方运行时依赖）** 构建，不仅 100% 兼容 `py-caskdb` 的核心接口、修复了原仓库的多字节 UTF-8 编码截断与缓冲区切片 Bug，还完整实现了原仓库 README 中留作进阶挑战的 **Level 1 / Level 2 / Level 3 核心特性**：
- **IEEE CRC32 数据完整性校验**与**崩溃尾记录自动截断恢复（Torn-Write Recovery）**
- **Tombstone 墓碑标记删除**与 **毫秒级 TTL 自动过期（惰性淘汰 + 主动巡检）**
- **多段文件自动轮转（Segment Rotation）**与**在线垃圾回收压缩（Merge / Compaction）**
- **Hint 索引文件（`.hint`）**与 **`lseek` 跳过 Value 扫描优化**，实现毫秒级冷启动
- **基于 `os.pread` 的单系统调用位置读取**与 **O(1) 线程安全 LRU 读缓存**
- **原子校验批量写入（`batch_write`）**、**有序前缀/范围扫描（`scan_prefix` / `scan_range`）**、**JSON 文档存取**
- **交互式 REPL 命令行工具（`cli.py`）**与**多线程 HTTP REST API 服务端（`server.py`）**

---

## 目录结构

```text
pybitcask/
├── pyproject.toml               # PEP 621 项目打包配置
├── Makefile                     # 一键测试、基准测试、REPL、HTTP 服务启动脚本
├── LICENSE                      # MIT 开源许可证
├── README.md                    # 项目总览与使用教程
├── docs/
│   ├── research_comparison.md   # 第一部分：3个候选开源项目源码调研与选型对比
│   ├── architecture.md          # 第二/五部分：架构图、模块边界、二进制协议与核心算法
│   ├── benchmark_report.md      # 第四部分：复杂度分析、性能瓶颈定位与优化前后实测报告
│   └── retrospective.md         # 第五部分：项目复盘、技术债、TODO 与大二进阶学习指南
├── src/
│   └── pybitcask/
│       ├── __init__.py          # 公共 API 导出
│       ├── py.typed             # PEP 561 类型检查标记
│       ├── exceptions.py        # 领域异常体系
│       ├── config.py            # 配置管理、校验、JSON 与环境变量加载
│       ├── logger.py            # 结构化日志模块
│       ├── format.py            # Bitcask v2 二进制协议、Hint 协议、CaskDB v1 兼容编解码
│       ├── keydir.py            # 内存哈希索引 KeyDir、TTL 集合追踪、死字节统计与范围扫描
│       ├── cache.py             # O(1) 线程安全 LRU 读缓存
│       ├── segment.py           # 段文件追加写、os.pread 位置读、尾损坏恢复与 .hint 生成
│       ├── engine.py            # 核心存储引擎 BitcaskEngine、分段轮转、Compaction 垃圾回收
│       ├── memory_store.py      # 内存存储引擎（用于单元测试与基准对照）
│       ├── server.py            # 多线程 HTTP REST/JSON 服务端
│       └── cli.py               # 命令行工具与交互式 REPL Shell
├── tests/                       # 52 项单元、集成、异常边界、并发与安全测试（含崩溃一致性回归 6 项）
│   ├── test_format.py
│   ├── test_keydir_and_cache.py
│   ├── test_segment.py
│   ├── test_engine.py
│   ├── test_server_and_cli.py
│   └── test_security_and_edge.py
└── benchmarks/
    └── run_benchmark.py         # 可重复执行的写入、启动、读取与 Compaction 基准测试
```

---

## 环境要求与安装教程

- **运行环境**：Python `>= 3.10`（已在 Python `3.14.4` Linux/WSL 环境下完整验证）
- **外部依赖**：无（100% Python 标准库）

### 方式一：直接通过 `PYTHONPATH` 运行（免安装）
```bash
cd /mnt/c/Users/18045/pybitcask
PYTHONPATH=src python3 -W error -m unittest discover -s tests -v
```

### 方式二：使用 Makefile 快捷指令
```bash
make test     # 运行全部 52 个单元测试、集成测试与边界安全测试
make bench    # 运行性能基准测试并输出 JSON 报告
make repl     # 启动交互式 Bitcask REPL Shell
make serve    # 启动 HTTP REST API 服务器 (默认 127.0.0.1:7379)
```

---

## 快速上手示例

### 1. Python SDK 编程接口

```python
from pybitcask import BitcaskEngine, BitcaskConfig, BatchOperation

# 配置多段存储目录、段大小上限、LRU 缓存容量与校验策略
config = BitcaskConfig(
    db_path="./my_bitcask_db",
    max_segment_size=4 * 1024 * 1024,  # 4 MiB 自动切分新段
    sync_policy="os",                  # 可选: 'always' | 'batch' | 'os'
    verify_checksum=True,              # 读取时校验 IEEE CRC32
    use_hint_files=True,               # 生成并加载 .hint 索引加速启动
    cache_capacity=1024,               # LRU 热点读缓存
)

with BitcaskEngine(config=config) as db:
    # 1. 基础读写与字典风格 API（完整支持中文与 Emoji UTF-8）
    db.set("student:1001", "张三 - 计算机科学与技术")
    db["student:1002"] = "李四 - 软件工程"
    print(db.get("student:1001"))      # 输出: 张三 - 计算机科学与技术

    # 2. 带 TTL（毫秒过期时间）的临时键值
    db.set("session:token_abc", "active", ttl_ms=5000)

    # 3. JSON 文档存取
    db.set_json("course:cs201", {"title": "数据结构与算法", "credits": 4})
    print(db.get_json("course:cs201"))

    # 4. 原子校验批量写入
    db.batch_write([
        BatchOperation("put", "score:1001", "95"),
        BatchOperation("put", "score:1002", "92"),
        BatchOperation("delete", "student:1002"),
    ])

    # 5. 前缀扫描与范围扫描
    print(db.scan_prefix("score:"))    # [('score:1001', '95'), ('score:1002', '92')]

    # 6. 在线 Merge / Compaction 垃圾回收（清理旧版本与墓碑记录）
    compact_stats = db.compact()
    print("Compaction Result:", compact_stats)
```

### 2. 命令行与交互式 REPL Shell

```bash
# 单次命令执行
PYTHONPATH=src python3 -m pybitcask.cli --db ./my_db set course:1 "Operating Systems"
PYTHONPATH=src python3 -m pybitcask.cli --db ./my_db get course:1
PYTHONPATH=src python3 -m pybitcask.cli --db ./my_db stats

# 交互式 Shell
PYTHONPATH=src python3 -m pybitcask.cli --db ./my_db shell
bitcask> SET user:1 "Alice"
OK
bitcask> GET user:1
Alice
bitcask> STATS
bitcask> COMPACT
bitcask> EXIT
```

### 3. HTTP REST API 服务

```bash
# 启动服务
PYTHONPATH=src python3 -m pybitcask.cli --db ./my_db serve --host 127.0.0.1 --port 7379

# 写入键值
curl -X PUT http://127.0.0.1:7379/kv/user:101 \
     -H "Content-Type: application/json" \
     -d '{"value": "Bob", "ttl_ms": 60000}'

# 读取键值
curl http://127.0.0.1:7379/kv/user:101

# 触发在线 Compaction 与查询引擎状态
curl -X POST http://127.0.0.1:7379/compact
curl http://127.0.0.1:7379/stats
```

---

## 与参考项目 `avinassh/py-caskdb` 的核心差异与许可证说明

| 维度 | `avinassh/py-caskdb` (原仓库) | `PyBitcask` (本项目复现与增强) |
| :--- | :--- | :--- |
| **开源许可证** | MIT License | MIT License（保留原项目致谢与兼容性） |
| **多字节 UTF-8 支持** | **存在 Bug**：`encode_kv` 使用 `len(str)` 字符数作为字节长度，中文/Emoji 解码直接抛出 `UnicodeDecodeError` | **已修复**：严格按 UTF-8 编码后的 `bytes` 长度写入 Header，并增加测试覆盖 |
| **数据完整性校验** | 无校验，磁盘位翻转无法检测 | 每条数据记录与 Hint 记录均包含 **4B IEEE CRC32 校验和** |
| **崩溃尾记录恢复** | 无，末尾半条记录会导致启动崩溃 | 支持 `recover_tail=True`，自动检测并 `os.truncate` 截断崩溃残留尾记录 |
| **删除机制 (`delete`)** | 无 `delete` 接口（测试中用设为空字符串代替，Key 永久驻留内存） | 完整实现 **Tombstone（墓碑标志 `0x01`）** 追写与内存移除，重启后墓碑生效 |
| **TTL 自动过期** | 不支持 | 支持毫秒级 `expire_at`，结合惰性淘汰与 `O(T)` TTL 集合主动清理 |
| **启动加载机制** | 全量读取所有 Value 并 `print` 到终端，大文件启动极慢 | 支持 **`.hint` 索引文件直读（提速 3.5x+）** 与 **`lseek` 跳过 Value 扫描** |
| **读取系统调用** | `file.seek()` + `file.read()`（两次操作且改变文件偏移） | 使用 POSIX **`os.pread()`** 单系统调用位置读取 + **O(1) LRU 缓存** |
| **多文件与垃圾回收** | 单文件无限膨胀，历史旧值无法回收 | 支持 `max_segment_size` 自动分段轮转 + **`compact()` 在线垃圾回收（实测回收 92.5% 空间）** |
| **外围工具生态** | 仅库代码 | 提供 CLI 子命令、交互式 REPL Shell、多线程 HTTP REST 服务端 |

---

## 已知限制与后续开发 TODO 清单

### 已知限制 (Known Limitations)
1. **全键内存约束（Bitcask 架构固有特性）**：`KeyDir` 必须在内存中维护所有活跃 Key 的元数据，因此数据库可容纳的 Key 总数受限于机器物理内存大小（但 Value 总量可远超内存大小）。
2. **范围扫描复杂度**：由于底层主索引为哈希表，前缀扫描（`scan_prefix`）与范围扫描（`scan_range`）需要对活跃 Key 集合进行过滤与排序（时间复杂度 O(N + K log K)），不如 B+ 树或 LSM-Tree（跳表 + SSTable）原生有序高效。
3. **单写多读模型**：写操作通过引擎级可重入锁（`RLock`）串行化追加到活跃段文件，跨进程并发依赖文件锁互斥，不支持多进程同时写入同一目录。

### 后续继续开发 TODO 清单
- [ ] **TODO 1（索引有序化）**：在 `KeyDir` 中引入跳表（SkipList）或 B-Tree 二级有序键索引，使 `scan_range` / `scan_prefix` 达到 O(log N + K) 复杂度。
- [ ] **TODO 2（内存映射读取）**：为只读归档段（Immutable Segments）增加可选的 `mmap` 映射读取通道，减少内核态到用户态的缓冲区拷贝。
- [ ] **TODO 3（后台异步 Compaction 线程）**：支持后台守护线程在低负载窗口自动执行分段式增量 Merge，降低大段合并时的写抖动。
- [ ] **TODO 4（二进制 Value 压缩）**：利用标准库 `zlib` / `lzma` 在 Record Flags 中增加 `FLAG_COMPRESSED = 0x04`，对大于 1 KiB 的文本/JSON Value 自动进行透明压缩存储。
- [ ] **TODO 5（Redis RESP 协议兼容层）**：在 `server.py` 基础上增加基于 `asyncio` 的 RESP 协议端口，支持直接使用 `redis-cli` 客户端连接操作。
