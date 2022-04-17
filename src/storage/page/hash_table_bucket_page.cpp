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
  bool flag = false;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (IsOccupied(i) && IsReadable(i) && cmp(key, array_[i].first) == 0) {
      result->push_back(array_[i].second);
      flag = true;
    }
  }
  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  // set occupied info of bucket_id
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (cmp(array_[i].first, key) == 0 && array_[i].second == value && IsReadable(i)) {
      return false;
    }
  }
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsReadable(i)) {
      // only insert when this bucket is empty, if all buckets is filled, return false
      MappingType pair = std::pair<KeyType, ValueType>(key, value);
      SetOccupied(i);
      SetReadable(i);
      array_[i] = pair;
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  /**
   * Here we remove kv by set readable to false to implement soft delete, but data is still existing in array.
   * First we must retrieve key from array, and get index of kv, then use it as bucket_id to delete kv.
   */
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    MappingType pair = array_[i];
    if (cmp(pair.first, key) == 0 && value == pair.second && IsReadable(i)) {
      // remove target key
      RemoveAt(i);
      return true;
    }
  }
  // not found given kv , return false
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  UnsetReadable(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  unsigned char c = occupied_[index];
  return (c >> (7 - offset) & 1) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  occupied_[index] = BitSet(reinterpret_cast<unsigned char &>(occupied_[index]), offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::UnsetOccupied(uint32_t bucket_idx) {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  occupied_[index] = UnSet(reinterpret_cast<unsigned char &>(occupied_[index]), offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  unsigned char c = readable_[index];
  return (c >> (7 - offset) & 1) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  readable_[index] = BitSet(reinterpret_cast<unsigned char &>(readable_[index]), offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::UnsetReadable(uint32_t bucket_idx) {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  readable_[index] = UnSet(reinterpret_cast<unsigned char &>(readable_[index]), offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  uint8_t mask = 255;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE / 8; ++i) {
    if ((readable_[i] & mask) != mask) {
      return false;
    }
  }
  for (size_t i = BUCKET_ARRAY_SIZE / 8 * 8; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsReadable(i)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  size_t cnt = 0;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (IsReadable(i)) {
      cnt++;
    }
  }
  return cnt;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  uint8_t mask = 255;
  for (size_t i = 0; i < sizeof(readable_) / sizeof(readable_[0]); ++i) {
    if ((readable_[i] & mask) > 0) {
      return false;
    }
  }
  return true;
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
unsigned char HASH_TABLE_BUCKET_TYPE::BitSet(unsigned char s, unsigned int n) {
  n = n % 8;
  unsigned char a = 1;
  a = a << (7 - n);
  return s | a;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
unsigned char HASH_TABLE_BUCKET_TYPE::UnSet(unsigned char s, unsigned int n) {
  n = n % 8;
  unsigned char a = 1;
  a = a << (7 - n);
  return s & (~a);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::Size() {
  return NumReadable();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::ReSet() {
  memset(occupied_, 0, sizeof(occupied_));
  memset(readable_, 0, sizeof(readable_));
  memset(array_, 0, sizeof(array_));
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
