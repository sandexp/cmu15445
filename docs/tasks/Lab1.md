#### TASK #1 - LRU REPLACEMENT POLICY
This component is responsible for tracking page usage in the buffer pool. You will implement a new sub-class called LRUReplacer in src/include/buffer/lru_replacer.h and its corresponding implementation file in src/buffer/lru_replacer.cpp. LRUReplacer extends the abstract Replacer class (src/include/buffer/replacer.h), which contains the function specifications.

The maximum number of pages for the LRUReplacer is the same as the size of the buffer pool since it contains placeholders for all of the frames in the BufferPoolManager. However, at any given moment not all the frames are considered to be in the LRUReplacer. The LRUReplacer is initialized to have no frames in it. Then, only the newly unpinned ones will be considered to be in the LRUReplacer.

You will need to implement the LRU policy discussed in the class. You will need to implement the following methods:

```c++
Victim(frame_id_t*) : Remove the object that was accessed least recently compared to all the other elements being tracked by the Replacer, store its contents in the output parameter and return True. If the Replacer is empty return False.
Pin(frame_id_t) : This method should be called after a page is pinned to a frame in the BufferPoolManager. It should remove the frame containing the pinned page from the LRUReplacer.
Unpin(frame_id_t) : This method should be called when the pin_count of a page becomes 0. This method should add the frame containing the unpinned page to the LRUReplacer.
Size() : This method returns the number of frames that are currently in the LRUReplacer.
```
The implementation details are up to you. You are allowed to use built-in STL containers. You can assume that you will not run out of memory, but you must make sure that the operations are thread-safe.

Lastly, in this project you are expected to implement only the LRU replacement policy. You don't have to implement the clock replacement policy, even if there is a corresponding file for it.

#### TASK #2 - BUFFER POOL MANAGER INSTANCE
Next, you need to implement the buffer pool manager in your system (BufferPoolManagerInstance). The BufferPoolManagerInstance is responsible for fetching database pages from the DiskManager and storing them in memory. The BufferPoolManagerInstance can also write dirty pages out to disk when it is either explicitly instructed to do so or when it needs to evict a page to make space for a new page.

To make sure that your implementation works correctly with the rest of the system, we will provide you with some of the functions already filled in. You will also not need to implement the code that actually reads and writes data to disk (this is called the DiskManager in our implementation). We will provide that functionality for you.

All in-memory pages in the system are represented by Page objects. The BufferPoolManagerInstance does not need to understand the contents of these pages. But it is important for you as the system developer to understand that Page objects are just containers for memory in the buffer pool and thus are not specific to a unique page. That is, each Page object contains a block of memory that the DiskManager will use as a location to copy the contents of a physical page that it reads from disk. The BufferPoolManagerInstance will reuse the same Page object to store data as it moves back and forth to disk. This means that the same Page object may contain a different physical page throughout the life of the system. The Page object's identifer (page_id) keeps track of what physical page it contains; if a Page object does not contain a physical page, then its page_id must be set to INVALID_PAGE_ID.

Each Page object also maintains a counter for the number of threads that have "pinned" that page. Your BufferPoolManagerInstance is not allowed to free a Page that is pinned. Each Page object also keeps track of whether it is dirty or not. It is your job to record whether a page was modified before it is unpinned. Your BufferPoolManagerInstance must write the contents of a dirty Page back to disk before that object can be reused.

Your BufferPoolManagerInstance implementation will use the LRUReplacer class that you created in the previous steps of this assignment. It will use the LRUReplacer to keep track of when Page objects are accessed so that it can decide which one to evict when it must free a frame to make room for copying a new physical page from disk.

You will need to implement the following functions defined in the header file (src/include/buffer/buffer_pool_manager_instance.h) in the source file (src/buffer/buffer_pool_manager_instance.cpp):
```c++
FetchPgImp(page_id)
UnpinPgImp(page_id, is_dirty)
FlushPgImp(page_id)
NewPgImp(page_id)
DeletePgImp(page_id)
FlushAllPagesImpl()
```
For FetchPgImp, you should return NULL if no page is available in the free list and all other pages are currently pinned. FlushPgImp should flush a page regardless of its pin status.

For UnpinPgImp, the is_dirty parameter keeps track of whether a page was modified while it was pinned.

Refer to the function documentation for details on how to implement these functions. Don't touch the non-impl versions, we need those to grade your code.

Note: Pin and Unpin within the contexts of the LRUReplacer and the BufferPoolManagerInstance have inverse meanings. Within the context of the LRUReplacer, pinning a page implies that we shouldn't evict the page because it is in use. This means we should remove it from the LRUReplacer. On the other hand, pinning a page in the BufferPoolManagerInstance implies that we want to use a page, and that it should not be removed from the buffer pool.

#### TASK #3 - PARALLEL BUFFER POOL MANAGER
As you probably noticed in the previous task, the single Buffer Pool Manager Instance needs to take latches in order to be thread safe. This can cause a lot of contention as every thread fights over a single latch when interacting with the buffer pool. One potential solution is to have multiple buffer pools in your system, each with it's own latch.

ParallelBufferPoolManager is a class that holds multiple BufferPoolManagerInstances. For every operation the ParallelBufferPoolManager picks a single BufferPoolManagerInstance and delegates to that instance.

We use the given page id to determine which specific BufferPoolManagerInstance to use. If we have num_instances many BufferPoolManagerInstances, then we need some way to map the given page id to a number in the range [0, num_instances). For this project we will be using the modulo operator, page_id mod num_instances will map the given page_id to the correct range.

When the ParallelBufferPoolManager is first instantiated it should have a starting index of 0. Every time you create a new page you will try every BufferPoolManagerInstance, starting at the starting index, until one is successful. Then increase the starting index by one.

Make sure that when you create the individual BufferPoolManagerInstances you use the constructor that takes uint32_t num_instances and uint32_t instance_index so that page ids are created correctly.

You will need to implement the following functions defined in the header file (src/include/buffer/parallel_buffer_pool_manager.h) in the source file (src/buffer/parallel_buffer_pool_manager.cpp):

```c++
ParallelBufferPoolManager(num_instances, pool_size, disk_manager, log_manager)
~ParallelBufferPoolManager()
GetPoolSize()
GetBufferPoolManager(page_id)
FetchPgImp(page_id)
UnpinPgImp(page_id, is_dirty)
FlushPgImp(page_id)
NewPgImp(page_id)
DeletePgImp(page_id)
FlushAllPagesImpl()
```

#### TESTING
You can test the individual components of this assigment using our testing framework. We use GTest for unit test cases. There are three separate files that contain tests for each component:

LRUReplacer: test/buffer/lru_replacer_test.cpp
BufferPoolManagerInstance: test/buffer/buffer_pool_manager_instance_test.cpp
ParallelBufferPoolManager: test/buffer/parallel_buffer_pool_manager_test.cpp
You can compile and run each test individually from the command-line:

```bash
$ mkdir build
$ cd build
$ make lru_replacer_test
$ ./test/lru_replacer_test
```
You can also run make check-tests to run ALL of the test cases. Note that some tests are disabled as you have not implemented future projects. You can disable tests in GTest by adding a DISABLED_ prefix to the test name.

Important: These tests are only a subset of the all the tests that we will use to evaluate and grade your project. You should write additional test cases on your own to check the complete functionality of your implementation.