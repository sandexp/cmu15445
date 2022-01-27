//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  for (int i = 0; i < pos; ++i) {
    if(cmp(key,array_[i].first)==0){
        return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  // set occupied info of bucket_id
  uint32_t bucket_id=pos;
  for (size_t i = 0; i < pos; ++i) {
    if(cmp(array_[i].first,key)==0 && array_[i].second==value){
        return false;
    }
  }
  MappingType pair=std::pair<KeyType,ValueType>(key,value);
  SetOccupied(bucket_id);
  SetReadable(bucket_id);
  array_[pos++]=pair;
  buckets++;
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  /**
   * Here we remove kv by set readable to false to implement soft delete, but data is still existing in array.
   * First we must retrieve key from array, and get index of kv, then use it as bucket_id to delete kv.
   */
  int index=-1;
  for (int i = 0; i < pos; ++i) {
      MappingType pair=array_[i];
      if(cmp(pair.first,key)==0){
          // remove target key
          index=i;
          break;
      }
  }
  if(index==-1)
      return false;
  int bucket_id=index;
  if(!IsReadable(bucket_id)){
      return false;
  }
  RemoveAt(bucket_id);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  if(IsReadable(bucket_idx) && IsOccupied(bucket_idx)){
      return array_[bucket_idx].first;
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  if(IsOccupied(bucket_idx) && IsReadable(bucket_idx)){
      return array_[bucket_idx].second;
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  uint32_t index=bucket_idx/8;
  if(index>pos){
      return;
  }
  buckets--;
  UnsetReadable(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  uint32_t index=bucket_idx/8;
  uint32_t offset=bucket_idx%8;
  unsigned char c=occupied_[index];
  return (c >> (7-offset) & 1)!=0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t index=bucket_idx/8;
  uint32_t offset=bucket_idx%8;
  BitSet(reinterpret_cast<unsigned char &>(occupied_[index]), offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::UnsetOccupied(uint32_t bucket_idx) {
  uint32_t index=bucket_idx/8;
  uint32_t offset=bucket_idx%8;
  UnSet(reinterpret_cast<unsigned char &>(occupied_[index]), offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  uint32_t index=bucket_idx/8;
  uint32_t offset=bucket_idx%8;
  unsigned char c=readable_[index];
  return (c >> (7-offset) & 1)!=0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t index=bucket_idx/8;
  uint32_t offset=bucket_idx%8;
  BitSet(reinterpret_cast<unsigned char &>(readable_[index]), offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::UnsetReadable(uint32_t bucket_idx) {
  uint32_t index=bucket_idx/8;
  uint32_t offset=bucket_idx%8;
  UnSet(reinterpret_cast<unsigned char &>(readable_[index]), offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return buckets==pos;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  return buckets;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return buckets==0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::BitSet(unsigned char &s,unsigned int n){
    n=n%8;
    unsigned char a=1;
    a=a<<(7-n);
    s=s | a;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::UnSet(unsigned char &s,unsigned int n){
    n=n%8;
    unsigned char a=1;
    a=a<<(7-n);
    s=s & (~a);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
