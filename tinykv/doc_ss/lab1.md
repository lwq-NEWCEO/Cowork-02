# LAB 1 存储与日志层

## 设计

在本章中，我们将讨论分布式事务数据库系统的设计。架构概览如下：

![overview](imgs/overview.png)

众所周知，事务系统必须确保 **`ACID`** 属性在大多数情况下都能得到保证。如何确保这些属性？

### 首先确保 `持久性`

在分布式环境中，为了满足**可用性**和**可靠性**要求，关键是提高**事务日志**的可用性和可靠性。正如 `AWS Aurora` 所说的那样，“**日志即数据库**”，如果日志能够可靠地持久化，那么**`持久性`**就能得到保证。

在像 MySQL 这样的单机数据库中，这是通过在将结果返回给客户端之前，将 InnoDB 重做日志持久化到磁盘来实现的。显然，如果单节点发生故障，日志可能会丢失。为了更可靠地持久化日志，需要副本，因此引入了分布式系统。
困难之处在于如何确保不同副本中的日志完全相同。通常，我们将使用一些共识算法将日志复制到不同的节点，`CAP` 理论告诉我们，如果使用更多的副本，可用性将会提高，而丢失日志的可能性将会降低。 

在 `tinykv` 的架构中，`Raft` 被用作共识算法，用于将日志复制到副本节点，代码模块称为 `raftStore`。 

![raft](imgs/raft.png)

`raftStore` 入请求都将由它处理。存储引擎的难点在于使其尽可能快，同时减少所需的资源将负责将事务日志或提交日志复制到不同的节点。在大多数复制成员成功接受日志后，它们被认为是 `已提交` 的，并且可以响应客户端继续写入过程。这保证了一旦事务被提交，那么它的所有写入内容至少在大多数节点上都持久化到磁盘，因此事务 `ACID` 的 **`持久性`** 在分布式环境中得到保证。

仅有提交日志，无法提供读取和写入请求服务。存储引擎用于应用或重放这些日志，然后它将服务这些请求。
在 TiKV 中，[rocksdb](https://docs.pingcap.com/zh/tidb/stable/rocksdb-overview) 用于构建存储引擎层。在 tinykv 中，使用了类似的存储引擎 [badger](https://github.com/dgraph-io/badger)，所有读取和写。


### 如何实现 `原子性`

在分布式事务架构的传统解决方案中，使用诸如 `两阶段提交` 之类的特殊协议来确保事务处理的原子性。问题是如何确保事务处理在故障转移后能够**正确**地继续进行，因为在传统的 2PC 处理中，如果协调器不可用，整个过程将会停滞。

在 `tinykv` 中，`raftStore` 已经确保日志将被复制到副本，并且故障转移更容易，因为如果 raft 组的大多数节点存活，事务状态始终可以恢复。也就是说，无论协调器或参与者是否发生故障，2PC 处理都可以继续，因为新当选的领导者将始终具有相同的事务状态。

在 tinysql/tinykv 中，[percolator](https://research.google/pubs/pub36726/) 协议被用作分布式事务协议，它类似于传统的 2PC 方式，但仍然存在一些差异。主要的区别之一是，协调器或调度器不需要在本地持久化事务状态，所有事务状态都持久化在参与者节点中。在 tinysql/tinykv 集群中，`tinysql` 节点充当事务协调器，而所有 `tinykv` 节点都是参与者。在接下来的实验中，我们将基于 `tinysql` 和 `tinykv` 的现有框架来实现 percolator 协议。

### 并发与隔离

为了获得更好的性能，事务引擎需要处理许多并发请求，如何并发处理它们并确保结果合理？隔离级别被定义来描述性能和并发约束之间的权衡。如果您不熟悉事务隔离级别和相关概念，可以参考[文档](https://pingcap.com/blog-cn/take-you-through-the-isolation-level-of-tidb-1)。

在 tinysql/tinykv 集群中，我们将实现一种强隔离约束，称为“快照隔离”或“可重复读”。在分布式环境中，使用全局时间戳排序分配器对所有并发事务进行排序，并且每个事务将具有唯一的 `start_ts`，这意味着它使用的快照。此时间戳排序分配器位于 tinysql/tinykv 集群中的 `tinyscheudler` 服务器中。要了解有关集群的调度服务的更多信息，此[文档](https://pingcap.com/blog-cn/placement-driver)可能会有所帮助。

### 支持 SQL 事务

为了构建一个完整的分布式事务数据库，我们将为事务语法添加 SQL 支持，例如 `BEGIN`、`COMMIT`、`ROLLBACK`。通常使用的写入语句（如 `INSERT`、`DELETE`）将被实现，以使用分布式事务将数据写入存储层。`SELECT` 结果也将保留上述部分中描述的事务属性。事务层能够正确处理读写和写写冲突。

## LAB1

在本实验中，我们将熟悉 `tinykv` 中的整个框架，并完成 `raftStore` 和 `storeEngine` 的实现。如上所述，`raftStore` 将处理所有提交日志，并将它们复制到不同 raft 组中的不同节点。在 tinykv 中，一个 raft 组被命名为 `Region`，每个区域都有其服务的键范围。引导阶段之后将有一个区域，并且该区域将来可能会拆分为更多区域，然后 `raftStore` 中不同的 raft 组（或我们称之为“区域”）将负责不同的键范围，并且 `multi-raft` 或 `multiple-regions` 将独立处理客户端请求。现在，您可以简单地认为只有一个 raft 组或一个区域正在处理请求。
此[文档](https://docs.pingcap.com/zh/tidb/stable/tikv-overview)可能有助于理解 `raftStore` 架构。


### 代码

#### `raftStore` 抽象

在 `kv/storage/storage.go` 中，存在 `raftStore` 的接口或抽象。
```
// Storage represents the internal-facing server part of TinyKV, it handles sending and receiving from other
// TinyKV nodes. As part of that responsibility, it also reads and writes data to disk (or semi-permanent memory).
type Storage interface {
	Start(client scheduler_client.Client) error
	Stop() error
	Write(ctx *kvrpcpb.Context, batch []Modify) error
	Reader(ctx *kvrpcpb.Context) (StorageReader, error)
	Client() scheduler_client.Client
}
```
`Write` 接口将被事务引擎用于持久化写入日志。如果它返回 ok，则表示日志已成功持久化并由存储引擎应用。这里需要两个步骤：首先是在大多数节点上持久化日志，其次是在存储引擎中应用这些写入。

为了简单起见，我们将跳过 raft 日志共识步骤，首先只考虑单机存储引擎。在此之后，我们将熟悉存储引擎接口，这非常有用，因为 `raftStore` 也将使用相同的存储引擎来持久化日志。

#### 实现 `StandAloneStorage` 的核心接口

尝试实现 `kv/storage/standalone_storage/standalone_storage.go` 中缺失的代码，这些代码部分标记为：

`// YOUR CODE HERE (lab1).`

完成这些部分后，运行 `make lab1P0` 命令来检查是否所有测试用例都通过。需要注意的事项：
- 由于 [badger](https://github.com/dgraph-io/badger) 被用作存储引擎，因此可以在其文档和存储库中找到常见的用法。
- `badger` 存储引擎不支持 [`列族`](https://en.wikipedia.org/wiki/Standard_column_family)。`percolator` 事务模型需要列族，在 `tinykv` 中，列族相关的实用程序已经包装在 `kv/util/engine_util/util.go` 中。当处理 `storage.Modify` 时，写入存储引擎的键应该使用 `KeyWithCF` 函数进行编码，考虑到其预期的列族。在 `tinykv` 中，有两种类型的 `Modify`，请查看 `kv/storage/modify.go` 以获取更多信息。
- `scheduler_client.Client` 不会被 `standAloneServer` 使用，因此可以跳过。
- 可以考虑 `badger` 提供的 `txn` 功能和相关的读/写接口。查看 `BadgerReader` 以获取更多信息。
- 某些测试用例可能有助于理解存储接口的用法。


#### 实现 `RaftStorage` 的核心接口

在 `StandAloneStorage` 中，日志引擎层被忽略，所有读取和写入请求都由存储引擎直接处理。在本章中，我们将尝试构建如上所述的日志引擎。在 `standalone_storage` 中，单个 badger 实例被用作存储引擎。
在 `raftStore` 中将有两个 `badger` 实例，第一个用作存储引擎或状态机，就像 `standalone_storage` 一样，第二个将由 `raftStore` 中的日志引擎用于持久化 raft 日志。在 `kv/util/engine_util/engines.go` 中，您可以找到 `Kv` 和 `Raft` 结构成员，`Kv` 实例用作存储引擎，`Raft` 由 raft 日志引擎使用。

`raftStore` 的工作流程是：

![raftStore](imgs/raftstore.png)

有一些重要的概念和抽象，其中一些已经在上面提到过，列表如下：
- `RawNode`。raft 实例的包装器，`Step`、`Ready` 和 `Advance` 接口由上层使用以驱动 raft 进程。`RawNode` 及其内部 raft 实例不负责实际发送消息和持久化日志，它们将设置在结果 `Ready` 结构中，上层将处理 ready 结果以完成实际工作。
- `Ready`。ready 是 raft 实例的输出，期望由上层处理，例如将消息发送到其他节点并将信息持久化到日志引擎。有关 `Ready` 的更多信息，请参见 `kv/raft/rawnode.go` 中的注释。

上面的概念或抽象是关于 raft 实例的。以下概念构建在 raft 实例或 `RawNode` 之上。
- `Region`。区域是一个 raft 组，负责处理与特定键范围相关的读/写请求。
- `Peer`。peer 是 raft 组或区域的成员，默认情况下使用 3 个副本，一个区域将有 3 个不同的 peer。一个 peer 内部将有一个 `RawNode`，其中包含一个 raft 实例。
- `raftWorker`。worker 用于处理路由到不同区域领导者或区域 peer 的所有客户端请求。
- `peerMsgHandler`。委托用于处理特定领导者 peer 的客户端请求。
- `applyWorker`。在提交 proposed 请求和相关日志后，相应的 apply 请求将路由到 `applyWorker`，然后这些日志将应用于状态机，该状态机是 tinykv 中的 `badger` 存储引擎。


将它们放在一起，消息流可以分为两个阶段：

- **日志共识阶段**。客户端请求通过回调发送到路由器，`raftWorker` 将使用相应的 peer 消息处理程序来处理请求。
- **日志应用阶段**。在 raft 组提交日志后，apply 请求将发送到 apply 路由器，`applyWorker` 将处理 apply 请求，最后调用回调，然后允许将结果响应给客户端。

可能有助于理解 `raftStore` 的文档：
- [raftStore 代码分析](https://pingcap.com/blog-cn/tikv-source-code-reading-17)
- [tikv 源代码阅读 raft propose](https://pingcap.com/blog-cn/tikv-source-code-reading-2)
- [tikv 源代码阅读 raft commit/apply](https://pingcap.com/blog-cn/tikv-source-code-reading-18)

尝试在以下位置实现缺失的代码：
- `kv/raftstore/peer_msg_handler.go`，`proposeRaftCommand` 方法，它是读/写请求 proposing 的核心部分。
- `kv/raftstore/peer.go`，`HandleRaftReady` 方法，它是 raft ready 处理的核心部分。
- `kv/raftstore/peer_storage.go`，`SaveReadyState` 方法，它是状态和日志持久性的核心部分。
- `kv/raftstore/peer_storage.go`，`Append` 方法，它将来自 raft ready 的日志附加到日志引擎。


这些代码部分的入口点标记为：

`// YOUR CODE HERE (lab1).`

要完成的代码部分标记为

`// Hintx: xxxxx`

周围有一些有用的注释和指导。


由于 `raftStore` 非常复杂，因此测试分为 4 个部分，`Makefile` 文件中有更多信息，测试顺序为：
- `make lab1P1a`。这是关于 `raftStore` 逻辑的基本测试。
- `make lab1P1b` 带有故障注入。这是关于 `raftStore` 逻辑的基本测试。
- `make lab1P2a`。这是关于 `raftStore` 的持久性测试。
- `make lab1P2b` 带有故障注入。这是关于 `raftStore` 的持久性测试。
- `make lab1P3a`。这是关于 `raftStore` 的快照相关测试。
- `make lab1P3b` 带有故障注入。这是关于 `raftStore` 的快照相关测试。
- `make lab1P4a`。这是关于 `raftStore` 的配置更改测试。
- `make lab1P4b` 带有故障注入。这是关于 `raftStore` 的配置更改测试。

需要注意的事项：
- 当测试失败时，`/tmp/test-raftstore-xxx` 中将存在一些垃圾目录或文件，可以通过手动删除它们，或者可以使用 `make clean` 命令来执行清理工作。
- 测试可能会消耗大量内存，最好使用具有 RAM(>= 16 GB) 的开发机器，如果由于 OOM 导致测试无法一起运行，请尝试使用 `go test -v ./kv/test_raftstore -run test_name` 等命令逐个运行它们。
- 尝试在运行测试之前设置更大的打开文件限制，例如 `ulimit -n 8192`。
- raft 包提供 raft 实现。它被包装到 `RawNode` 中，`Step` 和 `Ready` 是使用 `RawNode` 的核心接口。
- `RaftStorage` 中将有不同类型的工作人员，最重要的工作人员是 `raftWorker` 和 `applyWorker`。
- 尝试理解整个消息处理：一个新的输入客户端请求并使用结果响应客户端。由于有不同的工作人员并且 raft 共识需要几个步骤，因此客户端请求可能会转发给不同的工作人员，并且回调也将用于将结果通知给调用者。
