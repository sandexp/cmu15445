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

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  buffer_pool_manager_=buffer_pool_manager;
  comparator_=comparator;
  hash_fn_=hash_fn;
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
  uint32_t bucket_id=Hash(key);
  int global_mask=dir_page->GetGlobalDepthMask();
  return bucket_id & global_mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_id=KeyToDirectoryIndex(key,dir_page);
  page_id_t page_id=dir_page->GetBucketPageId(bucket_id);
  return page_id;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  Page* page=buffer_pool_manager_->FetchPage(directory_page_id_);
  return reinterpret_cast<HashTableDirectoryPage *>(page);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page* page=buffer_pool_manager_->FetchPage(bucket_page_id);
  return reinterpret_cast<HashTableBucketPage<KeyType,ValueType,KeyComparator> *>(page);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage* dir_page = FetchDirectoryPage();
  uint32_t dir_index=KeyToDirectoryIndex(key,dir_page);
  page_id_t page_id=dir_page->GetBucketPageId(dir_index);
  HashTableBucketPage<KeyType,ValueType,KeyComparator>* bucket_page= FetchBucketPage(page_id);
  table_latch_.RUnlock();
  return bucket_page->GetValue(key,comparator_,result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage* dir_page=FetchDirectoryPage();
  uint32_t bucket_page_id=KeyToPageId(key,dir_page);
  HashTableBucketPage<KeyType,ValueType,KeyComparator>* bucket_page=FetchBucketPage(bucket_page_id);
  table_latch_.WUnlock();
  if(bucket_page->IsFull()){
      LOG_INFO("Insert Split.");
      return SplitInsert(transaction,key,value);
  }else{
      return bucket_page->Insert(key,value,comparator_);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage* dir_page = FetchDirectoryPage();
  uint32_t org_bucket_id=KeyToDirectoryIndex(key,dir_page);
  // get all data from origin bucket
  dir_page->IncrGlobalDepth();
  // do reshuffle
  page_id_t temp_page_ids[DIRECTORY_ARRAY_SIZE];
  int temp_local_depths[DIRECTORY_ARRAY_SIZE];
  for (size_t i = 0; i < dir_page->Size(); ++i) {
    temp_page_ids[i]=dir_page->GetBucketPageId(i);
    temp_local_depths[i]=dir_page->GetLocalDepth(i);
  }

  // double buckets
  for (size_t i = 0; i < dir_page->Size() * 2; ++i) {
    uint32_t index=2*i;
    dir_page->SetBucketPageId(index,temp_page_ids[i]);
    dir_page->SetBucketPageId(index+1,temp_page_ids[i]);
    dir_page->SetLocalDepth(index,temp_local_depths[i]);
    dir_page->SetLocalDepth(index+1,temp_local_depths[i]);
  }
  // modify current key bucket and reshuffle
  page_id_t* page_id= nullptr;
  buffer_pool_manager_->NewPage(page_id);
  // allocate new page and reshuffle two buckets elements
  uint32_t bucket_id=KeyToDirectoryIndex(key,dir_page);
  if(org_bucket_id*2==bucket_id){
    dir_page->SetBucketPageId(bucket_id+1,*page_id);
    dir_page->SetLocalDepth(bucket_id,dir_page->GetLocalDepth(bucket_id)+1);
    dir_page->SetLocalDepth(bucket_id+1,dir_page->GetLocalDepth(bucket_id)+1);
  }else{
    dir_page->SetBucketPageId(bucket_id,*page_id);
    dir_page->SetLocalDepth(bucket_id-1,dir_page->GetLocalDepth(bucket_id)+1);
    dir_page->SetLocalDepth(bucket_id,dir_page->GetLocalDepth(bucket_id)+1);
  }
  // get origin page and reshuffle
  HashTableBucketPage<KeyType,ValueType,KeyComparator>* org_bucket_page= FetchBucketPage(bucket_id);
  HashTableBucketPage<KeyType,ValueType,KeyComparator>* new_page=FetchBucketPage(*page_id);

  for (size_t i = 0; i < org_bucket_page->Size(); ++i) {
      if(org_bucket_page->IsReadable(i) && KeyToDirectoryIndex(org_bucket_page->KeyAt(i),dir_page)!=bucket_id){
          new_page->Insert(org_bucket_page->KeyAt(i),org_bucket_page->ValueAt(i),comparator_);
          org_bucket_page->RemoveAt(i);
      }
  }

  // handle split task to current dir page with new inserted value
  return Insert(transaction,key,value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage* dir_page=FetchDirectoryPage();
  uint32_t bucket_page_id=KeyToPageId(key,dir_page);
  HashTableBucketPage<KeyType,ValueType,KeyComparator>* page=FetchBucketPage(bucket_page_id);
  bool flag=page->Remove(key,value,comparator_);
  if(page->IsEmpty()){
    Merge(transaction,key,value);
  }
  table_latch_.WUnlock();
  return flag;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
    HashTableDirectoryPage* dir_page=FetchDirectoryPage();
    uint32_t bucket_id=KeyToDirectoryIndex(key,dir_page);
    if(dir_page->GetLocalDepth(bucket_id)<=0){
        return;
    }
    uint32_t split_bucket_id=dir_page->GetSplitImageIndex(bucket_id);
    if(dir_page->GetLocalDepth(split_bucket_id)!=dir_page->GetLocalDepth(bucket_id)){
        return;
    }

    // decline local path and compact current bucket to split image
    uint32_t target_local_depth=dir_page->GetLocalDepth(bucket_id)-1;
    dir_page->SetLocalDepth(split_bucket_id,target_local_depth);
    dir_page->SetLocalDepth(bucket_id,target_local_depth);

    // here assert target bucket is empty, so we do not need to migrate data to split image, just modify point
    dir_page->SetBucketPageId(bucket_id,dir_page->GetBucketPageId(split_bucket_id));
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
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
