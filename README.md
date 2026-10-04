# 双缓冲（乒乓缓冲）切换组件

无锁的单写者/多读者双缓冲实现：写者只写私有缓冲，读者读已发布缓冲，
切换是一次原子引用交换。纯 Java 17，无第三方运行时依赖。

## 快速开始

```bash
mvn -q verify   # 编译 + 全部 15 个测试（含并发一致性与性能对比）
```

```java
// 经典乒乓：除活跃缓冲外保留 1 代（最多同时存在 2 个缓冲）
DoubleBuffer<List<String>> db = new DoubleBuffer<>(ArrayList::new, 1, List::clear);

// 写：批量修改私有缓冲，一次提交（一次切换）
db.writeBatch(list -> {
    list.add("a");
    list.add("b");
});

// 读：无锁，读到的是稳定快照
int size = db.read(List::size);

BufferStats s = db.stats(); // 切换次数 / 延迟回收次数 / 活跃读者数 / 存活代数
```

## 工作原理

- **两个角色**：`active` 槽位通过 `volatile` 引用发布；写者从「退休槽位池」
  取一个读者已清空的槽位（或按需新建）作为私有暂存缓冲，`commit()` 时一次
  原子交换完成切换。新读者立刻看到新数据；正在读旧缓冲的读者不受影响。
- **回收协议（hazard-pointer 风格）**：每个槽位有原子读者计数与 `mutating`
  标志。读者先认领（计数 +1）再校验（仍是 active 且未被写者标记）；写者先
  标记 `mutating` 再复核计数。两对 store→load 构成 Dekker 式握手，保证
  至少一方能看到对方，缓冲绝不会在读者持有期间被改写——因此读者不可能
  读到写了一半的缓冲（有专门的撕裂读测试证明）。
- **代数保留**：`retainedGenerations = k` 表示除活跃缓冲外最多再保留 k 代
  （总存活 k+1 个）。k=1 即经典乒乓。慢读者占住旧代时，写者可先用空闲代
  或新分配（不超过上限）；当代数耗尽，写者才等待最老一代的读者排空后复用，
  并计入 `delayedRecycleCount`。槽位只复用不销毁，稳态下零分配。
- **批量写入**：`writeBatch` 把一批修改放进私有暂存缓冲，返回时一次性提交，
  一批修改只产生一次切换；批内异常则整批丢弃，旧快照保持可见。
  `writeBatch` 内部串行化多写者；`beginWrite()/commit()` 是单写者协议。
- **写模型**：默认复用缓冲保留上上次写入的内容（原地增量模型）；构造时传入
  `resetter`（如 `List::clear`）则每批从干净缓冲开始（全量快照模型）。

## 与读写锁的取舍（内存换吞吐）

| 维度 | 双缓冲（本组件） | ReentrantReadWriteLock |
| --- | --- | --- |
| 读路径 | 无锁：2 次原子计数 + 2 次 volatile 读 | 读锁获取/释放（共享态 CAS） |
| 写路径 | 一次原子交换即发布；写时不阻塞读者 | 写锁互斥，写期间所有读者阻塞 |
| 读写互相阻塞 | 几乎不阻塞（仅代数耗尽时写者等读者排空） | 写等读、读等写，互斥明显 |
| 内存 | k+1 份缓冲常驻（内存换吞吐） | 1 份数据 |
| 一致性 | 读者看到切换前/后的完整快照之一 | 读者看到锁保护下的当前值 |
| 适用 | 读多写少、快照语义、写可批量 | 写频繁、内存敏感、需要读最新值 |

一句话：双缓冲用 **k+1 份内存** 换来 **读写互不阻塞**；读写锁省内存但写
操作会卡住所有读者，读者也会拖住写者。

## 性能对比（测试实测）

`PerformanceComparisonTest` 在 8 线程下对同一负载（4 个 long 的求和/自增）
对比两种方案，每次测量 700ms（本机 JDK 17 实测，数值随机器波动）：

| 读:写 | 读写锁吞吐 | 双缓冲吞吐 | 加速比 |
| --- | --- | --- | --- |
| 99:1 | ~8.3M op/s | ~47M op/s | **~5.6x** |
| 90:10 | ~6.3M op/s | ~18.3M op/s | **~2.9x** |
| 50:50 | ~5.5M op/s | ~7.3M op/s | **~1.3x** |
| 10:90 | ~6.2M op/s | ~5.6M op/s | ~0.9x（锁略优） |

结论：读占比越高双缓冲优势越大（读路径无锁、写不阻塞读）；写占绝对主导
时两者都退化为串行写，双缓冲多一次交换开销，读写锁略优。

## 测试覆盖（15 个用例）

- `WriteReadIsolationTest`：写读隔离——2 万次提交 + 4 读者高压下无一撕裂读；
  慢读者迫使回收等待时不变量仍成立。
- `SwitchSemanticsTest`：切换即时生效；在读旧缓冲的读者读完前不受切换影响；
  活跃读者数统计准确。
- `RecycleWaitTest`：代数耗尽时写者阻塞、读者排空后恢复（`delayedRecycleCount`
  递增）；多保留代数可避免等待；无慢读者时缓冲复用不泄漏（恒为 2 代）。
- `BatchCommitTest`：批中状态对外不可见、一批一次切换、失败批整批丢弃、
  多写者并发批提交不丢切换。
- `PerformanceComparisonTest`：上表；同时校验两种实现各自的结果一致性。

## API 一览

- `new DoubleBuffer<>(factory, retainedGenerations[, resetter])`
- 写：`writeBatch(Consumer<T>)`（推荐）；`beginWrite()` / `commit()` / `abortWrite()`
- 读：`read(Function<T,R>)` / `readVoid(Consumer<T>)`
- 统计：`stats()` → `BufferStats{switchCount, delayedRecycleCount, activeReaders, liveGenerations}`
