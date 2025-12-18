# TinyKV Lab 1 专题报告：BadgerDB 存储引擎实现

## 一、 模块目标与背景

在 TinyKV 项目中，`StandAloneStorage` 模块是作为底层键值存储的直接封装。它不涉及分布式特性，而是提供了一个单机的、可靠的 KV 存储接口。本报告将详细阐述 `StandAloneStorage` 的实现及其与 BadgerDB 数据库的交互。

**BadgerDB 介绍：** BadgerDB 是一个用 Go 语言编写的、高性能的、嵌入式键值存储数据库。它以其优秀的性能和简洁的 API 而闻名。

**核心挑战：** BadgerDB 本身并不直接支持“列族 (Column Family, CF)”的概念。然而，TinyKV 的上层事务模型（如 Percolator）依赖于列族来组织数据。因此，我们需要在 BadgerDB 之上封装一层，通过编码技巧（例如在 Key 前面加上代表列族的前缀）来模拟列族。项目中的 `engine_util` 模块正是为解决此问题而设计的。

## 二、 `StandAloneStorage` 实现详解

`StandAloneStorage` 模块主要实现了 `Reader` 和 `Write` 两个核心方法，分别用于提供数据读取和写入的能力。

### 2.1 `Reader` 方法实现

`Reader` 方法用于创建并返回一个 `StorageReader` 接口的实现，用于进行数据读取。

*   **`storage.go` - `Reader` 方法代码：**

    ```go
    func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
        // ADD: 创建只读事务并返回BadgerReader，用于提供一致性快照的读取接口
        // NewTransaction(false) 创建一个只读事务，确保读取的一致性快照
        return NewBadgerReader(s.db.NewTransaction(false)), nil
    }
    ```

*   **实现讲解**：
    *   **目的**：提供一个一致性的数据视图。在并发环境中，确保读操作不会受到正在进行的写操作的干扰，即获得一个**一致性快照**。
    *   **`s.db.NewTransaction(false)`**：这是 BadgerDB 提供的关键 API。它用于创建一个新的事务。
        *   参数 `false` 明确指示这是一个**只读事务**。与读写事务相比，只读事务具有更好的性能，因为它不需要处理锁和冲突解决。
        *   创建只读事务后，它会获得数据库在创建时刻的一个**一致性快照**。这意味着在该事务的整个生命周期内，无论底层数据库发生多少写操作，该事务读取到的数据视图都是固定不变的，从而保证了读取的一致性。
    *   **`NewBadgerReader(...)`**：`BadgerReader` 是 TinyKV 项目对 BadgerDB 事务的进一步封装。它实现了 `StorageReader` 接口，并内部持有这个 `badger.Txn` 实例。`BadgerReader` 提供的 `GetCF` (按列族获取键值) 和 `IterCF` (按列族迭代键值) 等方法，会利用这个传入的 `badger.Txn` 来执行实际的键值操作。

### 2.2 `Write` 方法实现

`Write` 方法负责将一系列的修改操作（`Put` 或 `Delete`）批量应用到底层 BadgerDB 存储。

*   **`storage.go` - `Write` 方法代码：**

    ```go
    func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
        for _, m := range batch {
            switch data := m.Data.(type) {
            case storage.Put:
                // ADD: 使用engine_util.PutCF封装BadgerDB的写入操作，支持列族模拟
                // 通过列族前缀编码实现多列族存储，确保数据按列族组织
                engine_util.PutCF(s.db, data.Cf, data.Key, data.Value)
            case storage.Delete:
                // ADD: 使用engine_util.DeleteCF封装BadgerDB的删除操作，支持列族模拟
                // 通过列族前缀编码定位并删除指定列族中的键值对
                engine_util.DeleteCF(s.db, data.Cf, data.Key)
            }
        }
        return nil
    }
    ```

*   **实现讲解**：
    *   **目的**：高效地执行批量键值对的写入和删除操作。
    *   **`batch []storage.Modify`**：`Write` 方法接收一个 `Modify` 结构体切片，每个 `Modify` 结构体通过 `Data` 字段（`interface{}` 类型）封装了具体的 `Put` 或 `Delete` 操作。这种设计允许在一个批次中混合不同类型的操作。
    *   **`switch data := m.Data.(type)`**：Go 语言的 `type switch` 语句被用于判断 `m.Data` 字段的具体类型。这是一种处理 `interface{}` 类型的标准且优雅的做法，可以根据实际的操作类型（`storage.Put` 或 `storage.Delete`）执行相应的逻辑。
    *   **`engine_util.PutCF` 和 `engine_util.DeleteCF`**：这是 `StandAloneStorage` 实现“列族”模拟的核心。
        *   由于 BadgerDB 原生不支持列族，`engine_util` 模块提供了一层封装。
        *   `PutCF` 和 `DeleteCF` 内部会根据传入的列族 `Cf` 和原始 `Key`，将它们编码成一个新的、唯一的 Key（例如，在原始 Key 前面添加 `CF_Prefix`），然后使用这个新 Key 在 BadgerDB 中进行实际的存储操作。
        *   这两个函数内部最终会调用 `s.db.Update(...)` 方法。`s.db.Update` 是 BadgerDB 推荐的写入方式，它会自动处理事务的创建、数据修改的提交或在出现错误时回滚，大大简化了外部代码的复杂性。

## 三、 总结

通过 `StandAloneStorage` 的实现，我们成功地基于 BadgerDB 构建了一个功能完备的单机 KV 存储引擎。它不仅提供了基本的读写能力，还通过 `engine_util` 模块巧妙地模拟了列族概念，为上层应用提供了必要的抽象。这个模块为后续集成 Raft 分布式一致性算法奠定了坚实的底层存储基础。

