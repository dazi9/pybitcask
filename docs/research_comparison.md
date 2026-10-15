# 第一部分：候选开源项目源码调研与选型对比报告

本报告记录了在当前工作环境中对 GitHub 上 3 个适合计算机专业大二学生复现的经典中等难度开源项目的真实拉取、目录核验、源码级剖析与最终选型决策。所有结论均基于已下载至本地 `/tmp/opencode/ref-repos/` 的真实仓库源码与测试执行结果。

---

## 1. 候选项目核验总表

| 维度 | 候选项目 A：`avinassh/py-caskdb` | 候选项目 B：`bartdegoede/python-searchengine` | 候选项目 C：`karpathy/micrograd` |
| :--- | :--- | :--- | :--- |
| **GitHub 仓库链接** | `https://github.com/avinassh/py-caskdb` | `https://github.com/bartdegoede/python-searchengine` | `https://github.com/karpathy/micrograd` |
| **Commit SHA (实测核验)** | `b617fe2080f5bf648551f68286047232201b2650` | `c76ebb6b4b9cc48bdf8854f7f8a8ece4313bc8cd` | `master` HEAD |
| **GitHub Stars** | ~1,365 | ~420 | ~17,913 |
| **开源许可证** | MIT License (`LICENSE`) | MIT License (`LICENSE.txt`) | MIT License (`LICENSE`) |
| **核心技术栈** | Python 3 标准库 (`struct`, `os`, `time`, `typing`, `unittest`) | Python 3.10+, `PyStemmer`, `numpy`, `datasets`, `sentence-transformers` | Python 3, `random`, `math`（测试依赖 `torch`） |
| **核心代码规模** | 3 个核心模块共 ~400 行 (`format.py`, `disk_store.py`, `memory_store.py`) | 6 个核心模块共 ~350 行 (`analysis.py`, `index.py`, `vector_index.py` 等) | 2 个核心模块共 ~154 行 (`engine.py` 94 行, `nn.py` 60 行) |
| **对应大二核心课程** | 数据结构（哈希表、LRU）、操作系统（文件系统、`fsync`、崩溃一致性）、数据库系统原理 | 数据结构（倒排索引、集合运算）、信息检索（TF-IDF、余弦相似度） | 高等数学/线性代数（链式法则、计算图）、机器学习导论（反向传播、MLP） |

---

## 2. 各项目目录结构、架构、核心算法与源码深度分析

### 候选项目 A：`avinassh/py-caskdb`（磁盘日志结构键值存储引擎）

#### (1) 目录结构（实测核验）
```text
py-caskdb/
├── LICENSE
├── Makefile
├── README.md
├── hints.md
├── setup.cfg
├── src/caskdb/
│   ├── __init__.py
│   ├── format.py          # 187 行：定长 12B Header (<LLL) 与 KV 字节流编解码
│   ├── disk_store.py      # 205 行：DiskStorage 磁盘追加写、随机 Seek 读与启动扫描
│   ├── memory_store.py    # 12 行：MemoryStorage 内存字典包装
│   └── example.py         # 38 行：基础读写示例
└── tests/
    ├── test_format.py     # 104 行：Header 与 KV 编码往返测试
    ├── test_disk_store.py # 126 行：持久化与字典接口测试
    └── test_memory_store.py
```

#### (2) 核心算法与数据流
- **写入路径 (`DiskStorage.set`)**：获取当前秒级时间戳 $\rightarrow$ `encode_kv` 使用 `struct.pack("<LLL", timestamp, key_size, value_size)` 打包 12 字节定长小端序头部并拼接 Key/Value 字节 $\rightarrow$ 追加写入文件末尾（`write` + `flush` + `os.fsync`） $\rightarrow$ 在内存哈希表 `key_dir: dict[str, KeyEntry]` 中记录 `(timestamp, position, total_size)` 并推进 `write_position`。
- **读取路径 (`DiskStorage.get`)**：在内存 `key_dir` 中以 $O(1)$ 查找 `KeyEntry` $\rightarrow$ 调用 `self.file.seek(kv.position, 0)` 进行单次磁盘寻道 $\rightarrow$ 读取 `kv.total_size` 字节并通过 `decode_kv` 解包返回 Value。
- **启动恢复 (`DiskStorage._init_key_dir`)**：顺序读取 12 字节头部 $\rightarrow$ 解出 `key_size` 和 `value_size` $\rightarrow$ 顺序读取 Key 和 Value 并重建 `key_dir`。

#### (3) 源码审查中发现的真实缺陷（已在本地复现验证）
1. **多字节 UTF-8 严重 Bug (`src/caskdb/format.py` 第 136 行)**：
   ```python
   header: bytes = encode_header(timestamp, len(key), len(value))
   data: bytes = b"".join([str.encode(key), str.encode(value)])
   ```
   原代码将 Python 字符串的**字符个数** `len(key)` 而非 UTF-8 编码后的**字节长度** `len(key.encode("utf-8"))` 写入了二进制 Header！当存入中文 `encode_kv(1700000000, "姓名", "张三")` 时，`key_size` 被错误记为 `2`（实际为 6 字节），调用 `decode_kv` 时在字节偏移 2 处截断，直接抛出 `UnicodeDecodeError: 'utf-8' codec can't decode bytes in position 0-1`。
2. **`decode_kv` 尾部切片未受 `value_size` 约束 (`format.py` 第 162 行)**：
   `value_bytes = data[HEADER_SIZE + key_size :]` 直接切片到缓冲区末尾，忽略了头部解出的 `value_size`。
3. **缺少真正的删除（Tombstone）、CRC32 校验、Hint 索引与 Compaction 垃圾回收**：原仓库将这些全部列在 README 的 `What next? (Level 1 / Level 2 / Level 3)` 挑战中尚未实现。

---

### 候选项目 B：`bartdegoede/python-searchengine`（全文与向量检索引擎）

#### (1) 目录结构（实测核验）
```text
python-searchengine/
├── LICENSE.txt
├── README.md
├── pyproject.toml
├── download.py / load.py / run.py / run_semantic.py
├── search/
│   ├── analysis.py        # 分词、小写化、去标点、停用词过滤、PyStemmer 词干提取
│   ├── documents.py       # Abstract 数据类与词频 Counter 统计
│   ├── index.py           # 倒排索引 Index、AND/OR 集合交并查询、TF-IDF 排序
│   ├── vector_index.py    # 基于 NumPy 归一化矩阵乘法 (@) 与 argpartition 的向量检索
│   ├── embeddings.py      # SentenceTransformer 向量编码包装
│   └── timing.py          # 耗时装饰器
└── tests/
    ├── test_analysis.py
    ├── test_index.py
    ├── test_embeddings.py
    └── test_vector_index.py
```

#### (2) 核心算法与源码分析
- **文本分析管线 (`analysis.py`)**：`tokenize` $\rightarrow$ `lowercase_filter` $\rightarrow$ `punctuation_filter` $\rightarrow$ `stopword_filter` $\rightarrow$ `stem_filter`。
- **倒排索引与 TF-IDF (`index.py`)**：维护 `index: dict[str, set[int]]`。查询时对各词项文档集合求交集（`set.intersection(*results)`）或并集（`set.union(*results)`），并按 $\sum_{t \in q} \text{TF}(t, d) \times \log_{10}(N / \text{DF}(t))$ 排序。
- **源码缺陷与环境限制**：
  1. 当查询词为空或全部由停用词组成（如 `search("the and")`) 时，`analyzed_query` 为空列表 `[]`，`set.intersection(*[])` 会直接抛出 `TypeError: descriptor 'intersection' of 'set' object needs an argument`。
  2. 在 `OR` 模式下若某个查询词未出现在任何文档中，`self.document_frequency(token)` 为 `0`，`inverse_document_frequency` 计算 `len(self.documents) / 0` 会触发 `ZeroDivisionError`。
  3. 强依赖 C 扩展库 `PyStemmer`、`numpy`、`datasets`、`sentence-transformers`，且默认脚本需下载 20GB Wikipedia 数据集。

---

### 候选项目 C：`karpathy/micrograd`（标量自动求导与微型神经网络引擎）

#### (1) 目录结构（实测核验）
```text
micrograd/
├── LICENSE
├── README.md
├── setup.py
├── micrograd/
│   ├── __init__.py
│   ├── engine.py          # 94 行：Value 标量节点、前向运算符重载与拓扑排序反向传播
│   └── nn.py              # 60 行：Module、Neuron、Layer、MLP
└── test/
    └── test_engine.py     # 67 行：与 PyTorch 对照梯度正确性
```

#### (2) 核心算法与源码分析
- **动态计算图与反向传播 (`engine.py`)**：每个 `Value` 节点保存标量 `data`、梯度 `grad`、前驱集合 `_prev` 和局部链式法则闭包 `_backward`。调用 `backward()` 时先对有向无环图（DAG）执行 DFS 拓扑排序，再按逆拓扑序依次调用 `v._backward()` 累积梯度。
- **局限性**：总代码量仅 154 行，纯内存标量数学计算，不涉及二进制序列化、文件 I/O、并发控制、崩溃恢复、配置管理等系统工程模块，且测试套件强依赖安装 `torch`。

---

## 3. 最终选型决策与理由

**选定复现项目：候选项目 A —— `avinassh/py-caskdb`（结合 Riak Bitcask 论文构建完整版 `PyBitcask` 存储引擎）**

### 选型理由：
1. **完美契合大二核心课程体系**：将《数据结构》（哈希表 `KeyDir`、双向链表/有序字典 `LRUCache`、前缀与范围扫描）与《操作系统/计算机系统基础》（顺序追加写 I/O、`os.fsync` 持久化、POSIX `os.pread`、文件分段轮转、崩溃截断恢复、文件锁与多线程同步）深度融合。
2. **零外部依赖，100% 当前环境可运行、可测试、可基准压测**：完全基于 Python 3.14 标准库即可构建从底层二进制协议到顶层 CLI / HTTP Server 的完整闭环。
3. **具备极高的工程复现与进阶拓展价值**：不仅能修复原仓库真实存在的 UTF-8 编码 Bug，还能把原仓库未实现的 Bitcask 论文核心机制（CRC32 校验、Tombstone 删除、TTL 过期、Hint 索引文件、Multi-Segment 轮转、Compaction 垃圾回收、LRU 读缓存、批量原子写）全部高质量落地。
