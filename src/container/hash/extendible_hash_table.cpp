//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <buffer/buffer_pool_manager_instance.h>
#include <string>
#include <utility>
#include <vector>

#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  page_id_t page_id;
  buffer_pool_manager_->NewPage(&page_id);
  directory_page_id_ = page_id;

  // init zero bucket
  page_id_t zero_page;
  buffer_pool_manager_->NewPage(&zero_page);
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();

  // init dir page
  dir_page->SetBucketPageId(0, zero_page);
  dir_page->SetPageId(directory_page_id_);
  dir_page->SetLocalDepth(0, 0);

  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->UnpinPage(zero_page, true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_id = Hash(key);
  int global_mask = dir_page->GetGlobalDepthMask();
  return bucket_id & global_mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_id = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(bucket_id);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  HashTableDirectoryPage *res = reinterpret_cast<HashTableDirectoryPage *>(page);
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  return reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(page);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t dir_index = KeyToDirectoryIndex(key, dir_page);
  page_id_t page_id = dir_page->GetBucketPageId(dir_index);

  HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucket_page = FetchBucketPage(page_id);
  bool res = bucket_page->GetValue(key, comparator_, result);
  Page *page = GetPage(bucket_page);

  // release pin of dir_page and bucket_page
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  assert(buffer_pool_manager_->UnpinPage(page->GetPageId(), false));
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_page_id = KeyToPageId(key, dir_page);
  HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucket_page = FetchBucketPage(bucket_page_id);
  Page *page = GetPage(bucket_page);

  // we made dir_page and bucket_page pin incr 1, so we must decr 1
  if (bucket_page->IsFull()) {
    assert(buffer_pool_manager_->UnpinPage(page->GetPageId(), false));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    return SplitInsert(transaction, key, value);
  }
  assert(buffer_pool_manager_->UnpinPage(page->GetPageId(), true));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  return bucket_page->Insert(key, value, comparator_);
}

/**
 * Split Insert Logic
 * @param key inserted key
 * @param value inserted value
 * @return true if insert successfully, false if failed
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t split_bucket_index = KeyToDirectoryIndex(key, dir_page);

  // judge if grow dir is necessary
  if (dir_page->GetLocalDepth(split_bucket_index) == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
    dir_page->Grow();
  }

  uint32_t split_image_index = SplitPage(dir_page, split_bucket_index);

  uint32_t mask = (1 << dir_page->GetLocalDepth(split_bucket_index)) - 1;
  // shuffle data between split-bucket-page and split-image-page
  Shuffle(dir_page, split_bucket_index, split_image_index, mask);

  // unpin modified page
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  // split task over
  // insert kv pair
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_page_id = KeyToPageId(key, dir_page);
  HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucket_page = FetchBucketPage(bucket_page_id);

  bool flag = bucket_page->Remove(key, value, comparator_);

  if (bucket_page->IsEmpty()) {
    // bucket of bucket_id pin will be released in SubMerge method
    uint32_t bucket_id = KeyToDirectoryIndex(key, dir_page);
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    SubMerge(transaction, bucket_id);
  } else {
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  }
  return flag;
}

/*****************************************************************************
 * MERGE ABORTED
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::SubMerge(Transaction *transaction, uint32_t empty_index) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();

  if (dir_page->GetLocalDepth(empty_index) <= 0) {
    // LOG_INFO("Bucket %d Depth is Zero, Can not be merged.", bucket_id);
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    return;
  }

  if (dir_page->GetLocalDepth(empty_index) >= dir_page->Size()) {
    // LOG_INFO("Bucket %d is Beyond The Arrange of Global Depth.", dir_page->Size());
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    return;
  }

  // get split image and when split image depth is equal to this bucket, we will merge it
  uint32_t split_bucket_id = dir_page->GetSplitImageIndex(empty_index);

  if (dir_page->GetLocalDepth(split_bucket_id) != dir_page->GetLocalDepth(empty_index)) {
    // LOG_INFO("Split Image Depth: %d Is Not Equal to Current Bucket Id: %d .", split_bucket_id, bucket_id);
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    return;
  }

  dir_page->SetBucketPageId(empty_index, dir_page->GetBucketPageId(split_bucket_id));

  // get page id of empty bucket and split image
  page_id_t empty_bucket_page_id = dir_page->GetBucketPageId(empty_index);
  page_id_t split_image_page_id = dir_page->GetBucketPageId(split_bucket_id);

  HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucket_page = FetchBucketPage(empty_bucket_page_id);

  // fail fast if empty bucket is not empty
  if (!bucket_page->IsEmpty()) {
    assert(buffer_pool_manager_->UnpinPage(empty_bucket_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    return;
  }

  // decline local path and compact current bucket to split image
  uint32_t target_local_depth = dir_page->GetLocalDepth(empty_index) - 1;
  dir_page->SetLocalDepth(split_bucket_id, target_local_depth);
  dir_page->SetLocalDepth(empty_index, target_local_depth);
  assert(dir_page->GetLocalDepth(split_bucket_id) == dir_page->GetLocalDepth(empty_index));

  assert(buffer_pool_manager_->UnpinPage(empty_bucket_page_id, false));
  assert(buffer_pool_manager_->DeletePage(empty_bucket_page_id));

  for (size_t i = 0; i < dir_page->Size(); ++i) {
    if (dir_page->GetBucketPageId(i) == empty_bucket_page_id || dir_page->GetBucketPageId(i) == split_image_page_id) {
      dir_page->SetBucketPageId(i, split_image_page_id);
      dir_page->SetLocalDepth(i, target_local_depth);
    }
  }

  // if split image page is also empty page,then we will continue to sub-merge it
  HashTableBucketPage<KeyType, ValueType, KeyComparator> *split_image_page = FetchBucketPage(split_image_page_id);
  assert(buffer_pool_manager_->UnpinPage(split_image_page_id, false));

  // solve both current bucket and split image bucket are empty
  if (split_image_page->IsEmpty()) {
    // LOG_INFO("Trigger Recursive Merge.");
    SubMerge(transaction, std::min(empty_index, split_bucket_id));
  }

  // do possible dir shrink
  while (dir_page->CanShrink()) {
    // if global depth shrink, we need to shrink underlying storage
    dir_page->DecrGlobalDepth();
  }
  // flush update of dir page into disk
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
}

/*****************************************************************************
 * HELPER FUNCTIONS - SUPPORT MAIN LOGIC
 *****************************************************************************/

/**
 * Get page from bucket page, this is a helper function to fetch a w/r latches for a bucket page.
 * @param bucketPage bucket page
 * @return target page
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::GetPage(HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucketPage) {
  Page *page = reinterpret_cast<Page *>(bucketPage);
  return page;
}

/**
 * Split page and reallocate local depth and bucket page mapping info of directory page.
 * 1. Assume you have already grew given dictionary, and local depth incr can not trigger grow operation.
 * 2. Assume {@code split_bucket_index} bucket page is full, and next insertion will split
 * @param dir_page target directory page
 * @param split_bucket_index split bucket index
 * @return split image index to retrieve split image bucket page
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::SplitPage(HashTableDirectoryPage *dir_page, uint32_t split_bucket_index) {
  page_id_t split_page_id = dir_page->GetBucketPageId(split_bucket_index);
  dir_page->IncrLocalDepth(split_bucket_index);
  uint32_t diff = 1 << dir_page->GetLocalDepth(split_bucket_index);

  // find all index which bucket page is current split_bucket_index's bucket page, and we will incr local depth
  for (size_t i = split_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    dir_page->SetBucketPageId(i, split_page_id);
  }

  for (size_t i = split_bucket_index; i >= 0; i -= diff) {
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    dir_page->SetBucketPageId(i, split_page_id);
    if (i < diff) {
      break;
    }
  }

  // get split image bucket page
  uint32_t split_image_index = dir_page->GetSplitImageIndex(split_bucket_index);
  // assign a page for split image page
  page_id_t split_image_page_id;
  buffer_pool_manager_->NewPage(&split_image_page_id);
  // set local depth of split image page
  dir_page->SetLocalDepth(split_image_index, dir_page->GetLocalDepth(split_bucket_index));
  dir_page->SetBucketPageId(split_image_index, split_image_page_id);

  for (size_t i = split_image_index; i < dir_page->Size(); i += diff) {
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_image_index));
    dir_page->SetBucketPageId(i, split_image_page_id);
  }

  for (size_t i = split_image_index; i >= 0; i -= diff) {
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_image_index));
    dir_page->SetBucketPageId(i, split_image_page_id);
    if (i < diff) {
      break;
    }
  }

  return split_image_index;
}

/**
 * Shuffle data between {@code from_page_id} and {@code to_page_id}.
 * @param from_page_id data source page id
 * @param to_page_id data destination page id
 * @param mask mask for shuffle
 * @param dir_page dir page
 * @return true if completed, false if errors occur
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Shuffle(HashTableDirectoryPage *dir_page, uint32_t from_index, uint32_t to_index, uint32_t mask) {
  page_id_t from_page_id = dir_page->GetBucketPageId(from_index);
  page_id_t to_page_id = dir_page->GetBucketPageId(to_index);

  HashTableBucketPage<KeyType, ValueType, KeyComparator> *from_bucket_page = FetchBucketPage(from_page_id);
  HashTableBucketPage<KeyType, ValueType, KeyComparator> *to_bucket_page = FetchBucketPage(to_page_id);

  // since creation of split image has caused pin count incr in process of split page, so here we need to decr 1, before
  // other operation
  assert(buffer_pool_manager_->UnpinPage(to_page_id, false));

  // storage for origin data
  std::vector<KeyType> keys;
  std::vector<ValueType> values;
  uint32_t size = from_bucket_page->Size();
  for (size_t i = 0; i < size; ++i) {
    keys.push_back(from_bucket_page->KeyAt(i));
    values.push_back(from_bucket_page->ValueAt(i));
  }

  // reset from page
  from_bucket_page->ReSet();

  // reallocate data to two page
  for (size_t i = 0; i < size; ++i) {
    uint32_t target_index = Hash(keys[i]) & mask;
    assert(target_index == from_index || target_index == to_index);
    if (target_index == from_index) {
      from_bucket_page->Insert(keys[i], values[i], comparator_);
    } else {
      to_bucket_page->Insert(keys[i], values[i], comparator_);
    }
  }
  // release pin count
  assert(buffer_pool_manager_->UnpinPage(from_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(to_page_id, true));
  return true;
}

/*****************************************************************************
 * TEST FUNCTION AREA - THESE FUNCTIONS IS JUST FOR TESTS
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::MockGrowDictionary() {
  try {
    HashTableDirectoryPage *dir_page = FetchDirectoryPage();
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    dir_page->IncrGlobalDepth();
    dir_page->Grow();
    return true;
  } catch (Exception exception) {
    // LOG_INFO("GROW DICTIONARY FAILED. REASON IS %s. ", exception.what());
    return false;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::MockMetadata(uint32_t global_depth, std::vector<uint32_t> local_depths,
                                                      std::vector<page_id_t> bucket_pages) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  assert(local_depths.size() == bucket_pages.size());
  uint32_t size = local_depths.size();
  dir_page->MockGlobalDepth(global_depth);
  for (size_t i = 0; i < size; ++i) {
    dir_page->SetBucketPageId(i, bucket_pages[i]);
    dir_page->SetLocalDepth(i, local_depths[i]);
  }
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::MockSplitProcess(uint32_t split_bucket_index) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  assert(split_bucket_index < dir_page->Size());
  uint32_t split_image_index = SplitPage(dir_page, split_bucket_index);
  page_id_t max_page_id = 0;
  for (size_t i = 0; i < dir_page->Size(); ++i) {
    if (i != split_image_index) {
      max_page_id = max_page_id < dir_page->GetBucketPageId(i) ? dir_page->GetBucketPageId(i) : max_page_id;
    }
  }
  // reset page id to next allocated page id instead of allocation of bpm
  dir_page->SetBucketPageId(split_image_index, max_page_id + 1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::MockShrinkDirectory(uint32_t global_depth, const std::vector<uint32_t> &local_depth,
                                          const std::vector<page_id_t> &pages) {
  HashTableDirectoryPage *dir_page = MockMetadata(global_depth, local_depth, pages);
  // LOG_DEBUG("Before Shrink...");
  dir_page->PrintDirectory();
  // LOG_DEBUG("After Shrink...");
  dir_page->Shrink();
  dir_page->PrintDirectory();
}

/**
 * Assert you have get dir page by {@code FetchDirectoryPage()}.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::MockMergeProcess(HashTableDirectoryPage *dir_page, uint32_t empty_index) {
  assert(dir_page->GetLocalDepth(empty_index) > 0);
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  uint32_t split_index = dir_page->GetSplitImageIndex(empty_index);
  assert(dir_page->GetLocalDepth(empty_index) == dir_page->GetLocalDepth(split_index));
  HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucket_page =
      FetchBucketPage(dir_page->GetBucketPageId(empty_index));
  assert(bucket_page->IsEmpty());
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetBucketPageId(empty_index), false));

  page_id_t empty_page_id = dir_page->GetBucketPageId(empty_index);
  page_id_t split_image_page_id = dir_page->GetBucketPageId(split_index);
  dir_page->SetBucketPageId(empty_index, split_image_page_id);
  dir_page->DecrLocalDepth(empty_index);
  dir_page->DecrLocalDepth(split_index);

  for (size_t i = 0; i < dir_page->Size(); ++i) {
    if (dir_page->GetBucketPageId(i) == split_image_page_id || dir_page->GetBucketPageId(i) == empty_page_id) {
      dir_page->SetBucketPageId(i, split_image_page_id);
      dir_page->SetBucketPageId(i, dir_page->GetLocalDepth(empty_index));
    }
  }

  while (dir_page->CanShrink()) {
    dir_page->Shrink();
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetKeyBucket(KeyType key) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  uint32_t dir_index = KeyToDirectoryIndex(key, dir_page);
  return dir_index;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::PrintHashTableMetadata() {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  // LOG_DEBUG("======== DIRECTORY (global_depth_: %u) ========", dir_page->GetGlobalDepth());
  // LOG_DEBUG("| bucket_idx | page_id | local_depth | bucket_size |");
  for (uint32_t idx = 0; idx < static_cast<uint32_t>(0x1 << dir_page->GetGlobalDepth()); idx++) {
    HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucket_page =
        FetchBucketPage(dir_page->GetBucketPageId(idx));
    Page *page = GetPage(bucket_page);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    // LOG_DEBUG("|      %u     |     %u     |     %u     |     %u     |", idx, dir_page->GetBucketPageId(idx),
    // dir_page->GetLocalDepth(idx), bucket_page->Size());
  }
  // LOG_DEBUG("================ END DIRECTORY ================");
}

/**
 * Attention:
 * This is just for test. And you need to {@code unpin} method to decr pin count in test code.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::GetDirectoryPage() {
  return FetchDirectoryPage();
}

/**
 * Assert given dir page is get by {@code FetchDirectoryPage()}. So at first we need to decline pin count.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::PrintBuckets(HashTableDirectoryPage *dir_page) {
  uint32_t size = 1 << dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  for (size_t i = 0; i < size; ++i) {
    // LOG_DEBUG("==================== %d th Buckets Info ==========================", i);
    page_id_t bucket_page_id = dir_page->GetBucketPageId(i);
    // LOG_DEBUG("| Index = %d | PageId = %d |", i, bucket_page_id);
    HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucket_page = FetchBucketPage(bucket_page_id);
    Page *page = GetPage(bucket_page);
    assert(buffer_pool_manager_->UnpinPage(page->GetPageId(), false));
    bucket_page->PrintBucket();
    // LOG_DEBUG("===================   Buckets End   ===========================");
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::PrintBufferPoolUsage() {
  BufferPoolManagerInstance *bpmi = dynamic_cast<BufferPoolManagerInstance *>(buffer_pool_manager_);
  bpmi->PrintBufferPoolPages();
}

/*****************************************************************************
 * GET GLOBAL DEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
