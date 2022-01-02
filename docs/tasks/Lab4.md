#### TASK #1 - LOCK MANAGER
To ensure correct interleaving of transactions' operations, the DBMS will use a lock manager (LM) to control when transactions are allowed to access data items. The basic idea of a LM is that it maintains an internal data structure about the locks currently held by active transactions. Transactions then issue lock requests to the LM before they are allowed to access a data item. The LM will either grant the lock to the calling transaction, block that transaction, or abort it.

In your implementation, there will be a global LM for the entire system (similar to your buffer pool manager). The TableHeap and Executor classes will use your LM to acquire locks on tuple records (by record id RID) whenever a transaction wants to access/modify a tuple.
This task requires you to implement a tuple-level LM that supports the three common isolation levels : READ_UNCOMMITED, READ_COMMITTED, and REPEATABLE_READ. The Lock Manager should grant or release locks according to a transaction's isolation level. Please refer to the lecture slides about the details.

In the repository, we are providing you with a Transaction context handle (include/concurrency/transaction.h) with an isolation level attribute (i.e., READ_UNCOMMITED, READ_COMMITTED, and REPEATABLE_READ) and information about its acquired locks. The LM will need to check the isolation level of transaction and expose correct behavior on lock/unlock requests. Any failed lock operation should lead to an ABORTED transaction state (implicit abort) and throw an exception. The transaction manager would further catch this exception and rollback write operations executed by the transaction.

We recommend you to read this article to refresh your C++ concurrency knowledge. More detailed documentation is available here.

> REQUIREMENTS AND HINTS
> The only file you need to modify for this task is the LockManager class (concurrency/lock_manager.cpp and concurrency/lock_manager.h). You will need to implement the following functions:

> LockShared(Transaction, RID): Transaction txn tries to take a shared lock on record id rid. This should be blocked on waiting and should return true when granted. Return false if transaction is rolled back (aborts).
> LockExclusive(Transaction, RID): Transaction txn tries to take an exclusive lock on record id rid. This should be blocked on waiting and should return true when granted. Return false if transaction is rolled back (aborts).
> LockUpgrade(Transaction, RID): Transaction txn tries to upgrade a shared to exclusive lock on record id rid. This should be blocked on waiting and should return true when granted. Return false if transaction is rolled back (aborts). This should also abort the transaction and return false if another transaction is already waiting to upgrade their lock.
> Unlock(Transaction, RID): Unlock the record identified by the given record id that is held by the transaction.

The specific locking mechanism taken by the lock manager depends on the transaction isolation level. You should first take a look at the transaction.h and lock_manager.h to become familiar with the API and member variables we provide. We also recommend to review the isolation level concepts since the implementation of these functions shall be compatible with the isolation level of the transaction that is making the lock/unlock requests. You have the freedom of adding any necessary data structures in lock_manager.h. You should consult with Chapters 15.1-15.2 in the textbook and isolation level concepts covered in lectures to make sure your implementation satisfies the requirement.

##### ADVICE FROM THE TAS
+ While your Lock Manager needs to use deadlock prevention, we recommend implementing your lock manager first without any deadlock handling and then adding the prevention mechanism after you verify it correctly locks and unlocks when no deadlocks occur.
+ You will need some way to keep track of which transactions are waiting on a lock. Take a look at LockRequestQueue class in lock_manager.h.
+ What does LockUpgrade translate to in terms of operations on the LockRequestQueue?
+ You will need some way to notify transactions that are waiting when they may be up to grab the lock. We recommend using std::condition_variable
+ Although some isolation levels are achieved by ensuring the properties of strict two phase locking, your implementation of lock manager is only required to ensure properties of two phase locking. The concept of strict 2PL will be achieved through the logic in your executors and transaction manager. Take a look at Commit and Abort methods there.
+ You should also maintain state of transaction. For example, the states of transaction may be changed from GROWING phase to SHRINKING phase due to unlock operation (Hint: Look at the methods in transaction.h)
+ You should also keep track of the shared/exclusive lock acquired by a transaction using shared_lock_set_ and exclusive_lock_set_ so that when the TransactionManager wants to commit/abort a transaction, the LM can release them properly.
+ Setting a transaction's state to ABORTED implicitly aborts it, but it is not explicitly aborted until TransactionManager::Abort is called. You should read through this function to understand what it does, and how your lock manager is used in the abort process.

#### TASK #2 - DEADLOCK PREVENTION

If your lock manager is told to use prevention, your lock manager should use the WOUND-WAIT algorithm for deciding which transactions to abort.

When acquiring a lock, you will need to look at the corresponding LockRequestQueue to see which transactions it would be waiting for.

##### ADVICE FROM THE TAS
+ Read through the slides carefully on how Wound-Wait is implemented.
+ When you abort a transaction, make sure to properly set its state using SetState
+ If a transaction is upgrading (waiting to acquire an X-lock), it can still be aborted. You must handle this correctly.
+ A waits for graph draws edges when a transaction is waiting for another transaction. Remember that if multiple transactions hold a shared lock, a single transaction may be waiting on multiple transactions.
+ When a transaction is aborted, make sure to set the transaction's state to ABORTED and throw an exception in your lock manager. The transaction manager will take care of the explicit abort and rollback changes.
+ A transaction waiting for a lock may be aborted by another thread due to the wound-wait prevention policy. You must have a way to notify waiting transactions that they've been aborted.


#### TASK #3 - CONCURRENT QUERY EXECUTION

During concurrent query execution, executors are required to lock/unlock tuples appropriately to achieve the isolation level specified in the corresponding transaction. To simplify this task, you can ignore concurrent index execution and just focus on table tuples.

You will need to update the Next() methods of some executors (sequential scans, inserts, updates, deletes, nested loop joins, and aggregations) implemented in Project 3. Note that transactions would abort when lock/unlock fails. Although there is no requirement of concurrent index execution, we still need to undo all previous write operations on both table tuples and indexes appropriately on transaction abort. To achieve this, you will need to maintain the write sets in transactions, which is required by the Abort() method of transaction manager.

You should not assume that a transaction only consists of one query. Specifically, this means a tuple might be accessed by different queries more than once in a transaction. Think about how you should handle this under different isolation levels.

More specifically, you will need to add support for concurrent query execution in the following executors:

+ src/execution/seq_scan_executor.cpp
+ src/execution/insert_executor.cpp
+ src/execution/update_executor.cpp
+ src/execution/delete_executor.cpp

#### TESTING

You can test the individual components of this assignment using our testing framework. We use GTest for unit test cases. You can compile and run each test individually from the command-line:
```bash
cd build
make lock_manager_test
make transaction_test
./test/lock_manager_test
./test/transaction_test
```
Important: These tests are only a subset of the all the tests that we will use to evaluate and grade your project. You should write additional test cases on your own to check the complete functionality of your implementation.