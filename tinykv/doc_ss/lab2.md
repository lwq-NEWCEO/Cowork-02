# 实验二 事务层 - TinyKV 部分

## 设计思路

在实验一中，我们完成了 Raft 日志引擎和存储引擎的开发。借助 Raft 日志引擎，事务日志能够被“可靠地”持久化，且故障转移后集群状态可以恢复。本章我们将探讨**分布式事务层**的设计与实现。Raft 日志引擎已提供了持久性（Durability）和状态恢复保障，而事务层需要保证原子性（Atomicity）以及并发场景下的正确性（即隔离性 Isolation）。

我们采用 Percolator 协议来保证原子性，并通过全局时间戳排序来对并发事务的执行进行序列化。基于这些机制，我们可以为客户端提供一种强隔离级别（通常称为快照隔离 Snapshot Isolation 或可重复读 Repeatable Read）。Percolator 协议同时在 tinysql 和 tinykv 服务器中实现，全局事务时间戳的分配则由 tinyscheduler 服务器完成，所有逻辑时间戳均严格单调递增。本实验中，我们将实现 tinykv 侧的 Percolator 协议部分，即分布式事务的参与者逻辑。

## 实现 TinyKV 中的 Percolator 协议

事务处理流程如下：

![transaction_overview](imgs/transaction_overview.PNG)

用户向 tinysql 服务器发送写入类请求（例如 Insert），请求经过解析和执行后，用户数据从行格式转换为键值对格式。
随后事务模块负责将这些键值对提交到存储引擎中。由于不同的键可能分布在不同的 Region（这些 Region 可能分散在不同的
tinykv 服务器上），事务引擎必须保证提交过程最终**要么完全成功**，**要么完全不生效**——这正是 Percolator 协议要解决的核心问题。

考虑一个包含多个键的分布式事务提交过程：首先会从待提交的键中选择一个作为**主键（Primary Key）**，事务的最终状态仅由该主键的提交状态决定。换句话说，只有当主键提交成功时，整个事务才被视为提交成功。提交过程分为两个阶段：预写（Prewrite）阶段和提交（Commit）阶段。

### 预写阶段（Prewrite Phase）

键值对准备完成后，事务进入预写阶段。在此阶段，所有待提交的键会被预写到对应 Region Leader 所在的不同 tinykv 服务器上。每个预写请求由对应的 tinykv 服务器处理，并为每个键在存储引擎的锁列族（lock column family）中写入预写锁（Prewrite Lock）。如果任意一个键的预写过程失败，整个提交流程会立即终止，同时清理已写入的预写锁。

### 提交阶段（Commit Phase）

若所有键的预写过程均成功，事务进入提交阶段。此阶段首先提交主键：在存储引擎的写列族（write column family）中写入一条写记录（Write Record），并释放锁列族中主键对应的预写锁。一旦主键提交成功，整个事务即被视为提交完成，并向客户端返回成功响应。其余的键（称为次键 Secondary Keys）则在后台异步提交。

上述流程覆盖了正常场景，但未考虑**故障恢复**。在分布式环境中，故障可能发生在任意节点：例如 tinysql 服务器可能在两阶段提交完成前崩溃，tinykv 服务器也可能出现故障。当新的 Region Leader 被选举出来并开始处理请求时，我们该如何**恢复未完成的事务并保证数据正确性**？

### 回滚记录（Rollback Record）

一旦事务确定失败，其遗留的锁需要被清理，同时在存储引擎中写入一条回滚记录，以防止未来可能的预写或提交操作（考虑到网络延迟等因素）。因此，若事务主键上存在回滚记录，则该事务的状态被判定为“已回滚”，且永远无法提交，最终必然失败。

### 检查事务状态（Check The Transaction Status）

如前所述，事务的最终状态仅由其主键（或主键锁）的状态决定。因此，当某个事务的状态无法确定时，需要检查其主键（或主键锁）的状态：
- 若主键存在提交记录或回滚记录，则可明确判定事务已提交或已回滚；
- 若主键锁仍存在且未过期，则事务可能仍在提交过程中；
- 若既无锁记录，也无提交/回滚记录，则事务状态不确定——可选择等待，或写入回滚记录阻止后续提交，从而将事务判定为回滚。

两阶段提交中的每个预写锁都包含一个存活时间（Time To Live，TTL）字段。若锁的 TTL 过期，则该锁可被 `CheckTxnStatus` 等并发命令回滚，此后该事务最终必然失败。

### 处理冲突与故障恢复（Process Conflicts And Do Recovery）

不同的事务协调器可能部署在不同的 tinysql 服务器上，读写请求之间可能发生冲突。例如：事务 txn1 已为键 k1 写入预写锁，而另一个读事务 txn2 试图读取 k1，此时 txn2 应如何处理？

由于锁记录的存在，读写请求无法继续执行，存在以下几种可能：
- 该锁所属的事务已提交：可直接提交该锁，阻塞的请求即可继续处理该键；
- 该锁所属的事务已回滚：可直接回滚该锁，阻塞的请求即可继续处理该键；
- 该锁所属的事务仍在执行中：阻塞的请求需等待持有锁的事务完成。

这种冲突处理逻辑在 tinysql/tinykv 集群中被称为“解析（Resolve）”。当请求被其他事务的预写锁阻塞时，解析流程会被触发，用于判定锁的状态并协助提交或回滚该锁，使被阻塞的请求得以继续执行。解析操作隐式地完成了**事务恢复**：例如，若事务协调器完成预写锁写入后 tinysql 服务器崩溃，这些遗留的锁会被来自其他 tinysql 服务器的并发事务请求解析处理。

## 实验二任务

我们需要在 tinykv 服务器中实现上述接口及其处理逻辑。

### 代码结构

#### 命令抽象（Command Abstraction）

在 `kv/transaction/commands/command.go` 中定义了所有事务命令的接口：

```go
// Command 是一个抽象接口，涵盖了从接收 gRPC 请求到返回响应的完整处理流程。
type Command interface {
	Context() *kvrpcpb.Context
	StartTs() uint64
	// WillWrite 返回该命令可能写入的所有键列表。若为只读命令则返回 nil。
	WillWrite() [][]byte
	// Read 执行命令的只读逻辑。仅在 WillWrite 返回 nil 时调用。
	// 若命令需要写入数据库，则应返回非空的待写入键列表。
	Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error)
	// PrepareWrites 用于在 MVCC 事务中构建写入操作。
	// 命令也可通过 txn 执行非事务性的读写操作。
	// 若返回时未修改 txn，则表示无需执行事务操作。
	PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error)
}


```

The `WillWrite` generate the write content need to be written for this request, the `Read` will execute the read requests needed by this command. `PrepareWrites` is used
to build the actual write contents for this command, it's the core part for write command processing. As each transaction may have its unique identifier, the `start_ts` which is the allocated global timestamp, the `StartTs` is
used to return this value of the current command.

Try to understand the whole process of the client requests processing(transaction command processing and `raftStore` log commit/apply). The transaction command results in some write mutations, these mutations will be converted into raft command requests and sent to the `raftStore`, after propose, commit and apply processing in the `raftStore`, the transaction command is considered successful and results are sent back to the clients.

![raftStore](imgs/raftstore.png)


#### Implement the `Get` Command

`KvGet` is used by the point get execution fetching the value for a specific key, try to implement the missing code in `kv/transaction/commands/get.go`, 
These code parts are marked with:

```
// YOUR CODE HERE (lab2).
```

#### Implement the `Prewrite` and `Commit` Commands

These are the two most important interfaces of our transaction engine, try to implement the missing code in `kv/transaction/commands/prewrite.go` and `kv/transaction/commands/commit.go`.  These code parts are marked with:

```
// YOUR CODE HERE (lab2).
```

Things to note:
- There could be duplicate requests as these commands are sent through rpc requests from tinysql server.
- `StartTS` is the only identifier of one transaction.
- `CommitTS` is the expected commit timestamp for one record.
- Consider the read write conflicts processing.

After finish these two parts, run `make lab2P1` to check if all the tests are passsed.

#### Implement the `Rollback` and `CheckTxnStatus` Commands

`Rollback` is used unlock the key and put the `Rollback Record` for a key. `CheckTxnStatus` is used to query the primary key lock status for a specific transaction. try to implement the missing code in `kv/transaction/commands/rollback.go` and `kv/transaction/commands/checkTxn.go`, These code parts are marked with:

```
// YOUR CODE HERE (lab2).
```
After finish these parts, run `make lab2P2` to check if all the tests are passed. Things to note:
- Consider the situation query lock dose not exist.
- There could be duplicate requests as these commands are sent through rpc requests from tinysql server.
- There are three different response actions in the `CheckTxnStatusResponse`. `Action_TTLExpireRollback` means the target lock is rolled back as it has expired and `Action_LockNotExistRollback` means the target lock does not exist and the rollback record is written.


#### Implement the `ResolveLock` Command

`Resolve` is used to commit or rollback lock if its related transaction status is certain.Try to implement the missing code in `kv/transaction/commands/resolve.go`, 
These code parts are marked with:

```
// YOUR CODE HERE (lab2).
```

After finish these parts, run `make lab2P3` to check if all the tests are passed. Things to note:
- The transaction status has been decided in the input request parameters.
- The `rollbackKey` and `commitKey` could be helpful.

After all the commands and tests are finished, run `make lab2P4` to check if tests in another suite are passed. In the next lab we'll try to implement the transaction coordinator part for percolator protocol in the `tinysql` server, and all these commands will be used.
