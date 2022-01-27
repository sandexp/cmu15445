//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  // get page from page table by page_id
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frameId = page_table_[page_id];
  int index = static_cast<int>(frameId);
  disk_manager_->WritePage(page_id, pages_[index].GetData());
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  // iterator on page table and flush it to disk
  for (std::unordered_map<page_id_t, frame_id_t>::iterator iterator = page_table_.begin();
       iterator != page_table_.end(); iterator++) {
    FlushPgImp(iterator->first);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  // check if all pages in buff are pinned, if so, return null
  bool flag = true;
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPinCount() == 0) {
      flag = false;
      break;
    }
  }
  if (flag) {
    return nullptr;
  }

  // find free page in freelist
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].page_id_ == INVALID_PAGE_ID) {
      // put page into frame
      *page_id = next_page_id_;
      pages_[i].page_id_ = next_page_id_;
      pages_[i].pin_count_ = 1;
      pages_[i].ResetMemory();
      page_table_[next_page_id_] = static_cast<frame_id_t>(i);
      AllocatePage();
      return &pages_[i];
    }
  }
  // reject page's request
  return nullptr;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  if (page_table_.count(page_id) != 0) {
    frame_id_t fId = page_table_[page_id];
    int index = static_cast<int>(fId);
    return &pages_[index];
  }

  // find a replace page in freelist
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].page_id_ == INVALID_PAGE_ID) {
      // invalid page must not be dirty, just replace it with page p
      pages_[i].page_id_ = page_id;
      pages_[i].pin_count_ = 1;
      disk_manager_->ReadPage(page_id, pages_[i].GetData());
      page_table_[page_id] = static_cast<frame_id_t>(i);
      return &pages_[i];
    }
  }
  return nullptr;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t fid = page_table_[page_id];
  int index = static_cast<int>(fid);
  if (pages_[index].pin_count_ != 0) {
    return false;
  }
  // remove from page table
  page_table_.erase(page_id);
  // reset metadata
  pages_[index].ResetMemory();
  // return page to freelist
  pages_[index].page_id_ = INVALID_PAGE_ID;
  DeallocatePage(page_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t fid = page_table_[page_id];
  int index = static_cast<int>(fid);

  if (strcmp(pages_[index].GetData(), "") != 0) {
    pages_[index].is_dirty_ = true;
  }

  is_dirty = pages_[index].is_dirty_;
  if (pages_[index].pin_count_ <= 0) {
    return false;
  }
  pages_[index].pin_count_--;
  // move unpinned page to disk
  if (pages_[index].pin_count_ == 0) {
    if (pages_[index].is_dirty_) {
      FlushPgImp(pages_[index].page_id_);
    }
    pages_[index].page_id_ = INVALID_PAGE_ID;
    pages_[index].ResetMemory();
    // remove from page table
    page_table_.erase(page_id);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
