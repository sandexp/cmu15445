#### TASK #1 - PAGE LAYOUTS
Your hash table is meant to be accessed through the DBMS's BufferPoolManager. This means that you cannot allocate memory to store information. Everything must be stored in disk pages so that they can read/written from the DiskManager. If you create a hash table, write its pages to disk, and then restart the DBMS, you should be able to load back the hash table from disk after restarting.

To support reading/writing hash table buckets on top of pages, you will implement two Page classes to store the data of your hash table. This is meant to teach you how to allocate memory from the BufferPoolManager as pages.
##### HASH TABLE DIRECTORY PAGE
This class holds all of the meta-data for the hash table. It is divided into the fields as shown by the table below:
```c++
Variable Name	Size	Description
page_id_	4 bytes	Self Page Id
lsn_	4 bytes	Log sequence number (Used in Project 4)
global_depth_	4 bytes	Global depth of the directory
local_depths_	512 bytes	Array of local depths for each bucket (uint8)
bucket_page_ids_	2048 bytes	Array of bucket page_id_t
The bucket_page_ids_ array maps bucket ids to page_id_t ids. The ith element in bucket_page_ids_ is the page_id for the ith bucket.
```
You must implement your Hash Table Directory Page in the designated files. You are only allowed to modify the directory file (src/include/storage/page/hash_table_directory_page.h) and its corresponding source file (src/storage/page/hash_table_directory_page.cpp). The Directory and Bucket pages are fully separate from the LinearProbeHashTable's Header and Block pages, so please make sure that you are editing the correct files.
##### Hash Table Bucket Page
The Hash Table Bucket Page holds three arrays:

occupied_ : The ith bit of occupied_ is 1 if the ith index of array_ has ever been occupied.
readable_ : The ith bit of readable_ is 1 if the ith index of array_ holds a readable value.
array_ : The array that holds the key-value pairs.
The number of slots available in a Hash Table Bucket Page depends on the types of the keys and values being stored. You only need to support fixed-length keys and values. The size of keys/values will be the same within a single hash table instance, but you cannot assume that they will be the same for all instances (e.g., hash table #1 can have 32-bit keys and hash table #2 can have 64-bit keys).

You must implement your Hash Table Bucket Page in the designated files. You are only allowed to modify the header file (src/include/storage/page/hash_table_bucket_page.h) and its corresponding source file (src/storage/page/hash_table_bucket_page.cpp). The Directory and Bucket pages are fully separate from the LinearProbeHashTable's Header and Block pages, so please make sure that you are editing the correct files.

Each Hash Table Directory/Bucket page corresponds to the content (i.e., the byte array data_) of a memory page fetched by buffer pool. Every time you try to read or write a page, you need to first fetch the page from buffer pool using its unique page_id, then reinterpret cast to either a directory or a bucket page, and unpin the page after any writing or reading operations.

We have provided various helpers, or documentation suggesting helper functions. The only functions you must implement are as follows.

Bucket Page: - Insert - Remove - IsOccupied - IsReadable - KeyAt - ValueAt

Directory Page: - GetGlobalDepth - IncrGlobalDepth - SetLocalDepth - SetBucketPageId - GetBucketPageId

You are free to design and implement additional new functions as you see fit. However, you must be careful to watch for name collisions. These should be rare, but would arise in Gradescope as a compiler error.

#### TASK #2 - HASH TABLE IMPLEMENTATION
You will implement a hash table that uses the extendible hashing scheme. It needs to support insertions (Insert), point search (GetValue), and deletions (Remove). There are many helper functions either implemented or documented the extendible hash table's header and cpp files. Your only strict API requirement is adhering to Insert, GetValue, and Remove. You also must leave the VerifyIntegrity function as it is. Please feel free to design and implement additional functions as you see fit.

Your hash table must support both unique and non-unique keys. Duplicate values for the same key are not allowed. This means that (key_0, value_0) and (key_0, value_1) can exist in the same hash table, but not (key_0, value_0) and (key_0, value_0). The Insert method only returns false if it tries to insert an existing key-value pair.

Your hash table implementation must hide the details of the key/value type and associated comparator, like this:

```c++
template <typename KeyType, typename ValueType, typename KeyComparator>
class ExtendibleHashTable {
// ---
};
```
These classes are already implemented for you:

+ KeyType: 
  
    The type of each key in the hash table. This will only be GenericKey, the actual size of GenericKey is specified and instantiated with a template argument and depends on the data type of indexed attribute.
+ ValueType: 
    
    The type of each value in the hash table. This will only be 64-bit RID.
+ KeyComparator: 
    
    The class used to compare whether two KeyType instances are less/greater-than each other. These will be included in the KeyType implementation files. Variables with the KeyComparator type are essentially functions; for instance, given two keys KeyType key1 and KeyType key2, and a key comparator KeyComparator cmp, you can compare the keys via cmp(key1, key2).
Note that you can equality-test ValueType instances simply using the == operator.

##### EXTENDIBLE HASHING IMPLEMENTATION DETAILS
This implementation requires that you implement bucket splitting/merging and directory growing/shrinking. Some implementations of extendible hashing skip the merging of buckets as it can cause thrashing in certain scenarios. We implement it here to provide a full understanding of the data structure and provide more opportunities for learning how to manage latches, locks, page operations (fetch/pin/delete/etc).

+ DIRECTORY INDEXING
  
    When inserting into your hash index, you will want to use the least-significant bits for indexing into the directory. Of course, it is possible to use the most-significant bits correctly, but using the least-significant bits makes the directory expansion operation much simpler.


+ SPLITTING BUCKETS
  
    You must split a bucket if there is no room for insertion. You can ostensibly split as soon as the bucket becomes full, if you find that easier. However, the reference solution splits only when an insertion would overflow a page. Hence, you may find that the provided API is more amenable to this approach. As always, you are welcome to factor your own internal API.

+ MERGING BUCKETS

    Merging must be attempted when a bucket becomes empty. There are ways to merge more aggressively by checking the occupancy of buckets and their split images, but these expensive checks and extra merges can increase thrashing.

To keep things relatively simple, we provide the following rules for merging:

Only empty buckets can be merged.
Buckets can only be merged with their split image if their split image has the same local depth.
Buckets can only be merged if their local depth is greater than 0.
If you are confused about a "split image,” please review the algorithm and code documentation. The concept falls out quite naturally.
+ DIRECTORY GROWING

    There are no fancy rules for this. You either have to grow the directory, or you don't.

+ DIRECTORY SHRINKING
    
    Only shrink the directory if the local depth of every bucket is strictly less than the global depth of the directory. You may see other tests for the shrinking of the directory, but this one is trivial since we keep the local depths in the directory page.
  
##### PERFORMANCE
An important performance detail is to only take write locks and latches when they are needed. Always taking write locks will inevitably timeout on Gradescope.

In addition, one potential optimization is to factor your own scans types over the bucket pages, which can avoid repeated scans in certain cases. You will find that checking many things about the bucket page often involves a full scan, so you can potentially collect all this information in one pass.


#### TASK #3 - CONCURRENCY CONTROL
Up to this this point you could assume that your hash table only supported single-threaded execution. In this last task, you will modify your implementation so that it supports multiple threads reading/writing the table at the same time.

You will need to have latches on each bucket so that when one thread is writing to a bucket other threads are not reading or modifying that index as well. You should also allow multiple readers to be reading the same bucket at the same time.

You will need to latch the whole hash table when you need to split or merge buckets, and when global depth changes.

##### REQUIREMENTS AND HINTS

LATCHES
There are two latches to be aware of in this project. The first is table_latch_ in extendible_hash_table.h, which takes latches on the extendible hash table. This comes from the RWLatch class in src/include/common/rwlatch.h. As you can see in the code, it is backed by std::mutex. The second is the built-in page latching functionality in src/include/storage/page.h. This is what you must use to protect your bucket pages. Note that to take a read-lock on the table_latch_ you call RLock from RWLatch.h, but to take a read-lock on a bucket page you must reinterpret_cast<Page *> to a page pointer, and call the RLatch method from page.h.

We suggest you look at the extendible hash table class, look at its members, and analyze exactly which latches will allow which behavior. We also suggest that you do the same for the bucket pages.

Project 4 will explore concurrency control through locking with the LockManager in LockManager.h. You do not need the LockManager at all for this project.

+ TRANSACTION POINTER

    You can simply pass nullptr as the Transaction pointer argument when it is required. This Transaction object comes from src/include/concurrency/transaction.h. It provides methods to store the page on which you have acquired latch while traversing through the Hash Table. You do not need to do this to pass the tests.
  

#### TESTING
You can test the individual components of this assigment using our testing framework. We use GTest for unit test cases.
```bash
cd build
make -j hash_table_test
./test/hash_table_test
```
We encourage you to use gdb to debug your project if you are having problems. Here is a nice reference for useful commands in gdb. Make sure you've ran cmake with debug flags on so gdb has debugging symbols to use.