//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages_) { this->capacity_ = num_pages_; }

LRUReplacer::~LRUReplacer() = default;

/**
 * Remove given frame
 * @param frame_id pending to removed frame
 * @return true if removed, false if not found
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (this->pages_.empty()) {
    return false;
  }
  // if frame is found, remove it from lru cache and update page numbers
  int removal = pages_.back();
  location_.erase(removal);
  pages_.pop_back();
  *frame_id = removal;
  return true;
}

/**
 * Remove frame from lru cache
 * @param frame_id pending to removed frame
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (location_.count(frame_id) == 0) {
    return;
  }
  pages_.remove(frame_id);
  location_.erase(frame_id);
}

/**
 * Add frame to lru cache
 * @param frame_id pending to added frame
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (location_.count(frame_id) > 0) {
    return;
  }
  pages_.push_front(frame_id);
  location_[frame_id] = frame_id;
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> guard(mutex_);
  return this->pages_.size();
}

}  // namespace bustub
