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

LRUReplacer::LRUReplacer(size_t num_pages) {
    this->capacity=num_pages;
    this->page_nums=0;
}

LRUReplacer::~LRUReplacer() = default;

/**
 * Remove given frame
 * @param frame_id pending to removed frame
 * @return true if removed, false if not found
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
    if (page_nums==0){
        return false;
    }
    // if frame is found, remove it from lru cache and update page numbers
    int removal=pages.back();
    location.erase(removal);
    pages.pop_back();
    page_nums--;
    *frame_id=removal;
    return true;
}

/**
 * Remove frame from lru cache
 * @param frame_id pending to removed frame
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
    if(location.count(frame_id)==0){
        return;
    }
    pages.remove(frame_id);
    location.erase(frame_id);
    page_nums--;
}

/**
 * Add frame to lru cache
 * @param frame_id pending to added frame
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
    if(location.count(frame_id)>0){
        return;
    }
    pages.push_front(frame_id);
    location[frame_id]=frame_id;
    page_nums++;
}

size_t LRUReplacer::Size() {
    return this->page_nums;
}

}  // namespace bustub
