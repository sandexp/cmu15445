//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.h
//
// Identification: src/include/container/hash/extendible_hash_table.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <queue>
#include <string>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "concurrency/transaction.h"
#include "container/hash/hash_function.h"
#include "storage/page/hash_table_bucket_page.h"
#include "storage/page/hash_table_directory_page.h"

namespace bustub {

#define HASH_TABLE_TYPE ExtendibleHashTable<KeyType, ValueType, KeyComparator>

/**
 * Implementation of extendible hash table that is backed by a buffer pool
 * manager. Non-unique keys are supported. Supports insert and delete. The
 * table grows/shrinks dynamically as buckets become full/empty.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
class ExtendibleHashTable {
 public:
  /**
   * Creates a new ExtendibleHashTable.
   *
   * @param buffer_pool_manager buffer pool manager to be used
   * @param comparator comparator for keys
   * @param hash_fn the hash function
   */
  explicit ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                               const KeyComparator &comparator, HashFunction<KeyType> hash_fn);

  /**
   * Inserts a key-value pair into the hash table.
   *
   * @param transaction the current transaction
   * @param key the key to create
   * @param value the value to be associated with the key
   * @return true if insert succeeded, false otherwise
   */
  bool Insert(Transaction *transaction, const KeyType &key, const ValueType &value);

  /**
   * Deletes the associated value for the given key.
   *
   * @param transaction the current transaction
   * @param key the key to delete
   * @param value the value to delete
   * @return true if remove succeeded, false otherwise
   */
  bool Remove(Transaction *transaction, const KeyType &key, const ValueType &value);

  /**
   * Performs a point query on the hash table.
   *
   * @param transaction the current transaction
   * @param key the key to look up
   * @param[out] result the value(s) associated with a given key
   * @return the value(s) associated with the given key
   */
  bool GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result);

  /**
   * Returns the global depth.  Do not touch.
   */
  uint32_t GetGlobalDepth();

  /**
   * Helper function to verify the integrity of the extendible hash table's directory.  Do not touch.
   */
  void VerifyIntegrity();

  /**
   * Test grow of dictionary. This is just for test.
   */
  bool MockGrowDictionary();

  /**
   * Mock metadata for grow and shrink test, just for test, can not be used for operation
   * @param global_depth mocked global depth
   * @param local_depths mocked local depth
   * @param bucket_pages mocked bucket pages
   */
  HashTableDirectoryPage *MockMetadata(uint32_t global_depth, std::vector<uint32_t> local_depths,
                                       std::vector<page_id_t> bucket_pages);

  /**
   * Mock Split Process.
   */
  void MockSplitProcess(uint32_t split_bucket_index);

  /**
   * Mock Shrink Directory.
   * @param global_depth global depth
   * @param local_depth local depths
   * @param pages pages info
   */
  void MockShrinkDirectory(uint32_t global_depth, const std::vector<uint32_t> &local_depth,
                           const std::vector<page_id_t> &pages);

  /**
   * Mock merge process, on occur when one bucket page is empty.
   * Assert {@code empty_index} is empty bucket page.
   * @param dir_page dir page
   * @param empty_index empty bucket index
   */
  void MockMergeProcess(HashTableDirectoryPage *dir_page, uint32_t empty_index);

  /**
   * Mock Split Process.
   */
  uint32_t GetKeyBucket(KeyType key);

  /**
   * Print metadata of ext hash table.
   */
  void PrintHashTableMetadata();

  /**
   * Get directory page, just for test.
   * @return directory page
   */
  HashTableDirectoryPage *GetDirectoryPage();

  /**
   * Print buckets of current pages.
   */
  void PrintBuckets(HashTableDirectoryPage *dir_page);

  /**
   * Print Buffer Pool Usage. Just for test.
   */
  void PrintBufferPoolUsage();

 private:
  /**
   * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
   * for extendible hashing.
   *
   * @param key the key to hash
   * @return the downcasted 32-bit hash
   */
  inline uint32_t Hash(KeyType key);

  /**
   * KeyToDirectoryIndex - maps a key to a directory index
   *
   * In Extendible Hashing we map a key to a directory index
   * using the following hash + mask function.
   *
   * DirectoryIndex = Hash(key) & GLOBAL_DEPTH_MASK
   *
   * where GLOBAL_DEPTH_MASK is a mask with exactly GLOBAL_DEPTH 1's from LSB
   * upwards.  For example, global depth 3 corresponds to 0x00000007 in a 32-bit
   * representation.
   *
   * @param key the key to use for lookup
   * @param dir_page to use for lookup of global depth
   * @return the directory index
   */
  inline uint32_t KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page);

  /**
   * Get the bucket page_id corresponding to a key.
   *
   * @param key the key for lookup
   * @param dir_page a pointer to the hash table's directory page
   * @return the bucket page_id corresponding to the input key
   */
  inline uint32_t KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page);

  /**
   * Fetches the directory page from the buffer pool manager.
   *
   * @return a pointer to the directory page
   */
  HashTableDirectoryPage *FetchDirectoryPage();

  /**
   * Fetches the a bucket page from the buffer pool manager using the bucket's page_id.
   *
   * @param bucket_page_id the page_id to fetch
   * @return a pointer to a bucket page
   */
  HASH_TABLE_BUCKET_TYPE *FetchBucketPage(page_id_t bucket_page_id);

  /**
   * Performs insertion with an optional bucket splitting.
   *
   * @param transaction a pointer to the current transaction
   * @param key the key to insert
   * @param value the value to insert
   * @return whether or not the insertion was successful
   */
  bool SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value);

  /**
   * Optionally merges an empty bucket into it's pair.  This is called by Remove,
   * if Remove makes a bucket empty.
   *
   * There are three conditions under which we skip the merge:
   * 1. The bucket is no longer empty.
   * 2. The bucket has local depth 0.
   * 3. The bucket's local depth doesn't match its split image's local depth.
   *
   * @param transaction a pointer to the current transaction
   * @param key the key that was removed
   * @param value the value that was removed
   */
  void Merge(Transaction *transaction, const KeyType &key, const ValueType &value);

  /**
   * Sub-Merge of bucket ,this method is used to solved nested merge problem when empty_bucket and its split image are
   * both empty
   * @param transaction txn
   * @param empty_index empty index of dir
   */
  void SubMerge(Transaction *transaction, uint32_t empty_index);

  /**
   * Get page from bucket page, this is a assist function to fetch w/r latches.
   * @param bucketPage bucket page
   * @return underlying page
   */
  Page *GetPage(HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucketPage);

  /**
   * Split page and reallocate local depth info and bucket info of given directory.
   * @param dir_page target directory page
   * @param split_bucket_index original split page
   * @return split image index to retrieve split image bucket
   */
  uint32_t SplitPage(HashTableDirectoryPage *dir_page, uint32_t split_bucket_index);

  /**
   * Shuffle data between {@code from_page_id} and {@code to_page_id}.
   * @param from_page_id data source page id
   * @param to_page_id data destination page id
   * @param from_dir_index data sources index in dir
   * @param to_dir_index desc index in dir
   * @param comparator key comparator
   * @return true if completed, false if errors occur
   */
  bool Shuffle(HashTableDirectoryPage *dir_page, uint32_t from_index, uint32_t to_index, uint32_t mask);

  // member variables
  page_id_t directory_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;

  // Readers includes inserts and removes, writers are splits and merges
  ReaderWriterLatch table_latch_;
  HashFunction<KeyType> hash_fn_;
};

}  // namespace bustub
