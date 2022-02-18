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
#include "include/common/logger.h"

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
  if (page_table_.find(page_id) == page_table_.end()) {
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
  if(free_list_.size()==0 && replacer_->Size()==0){
      return nullptr;
  }

  int index=-1;
  frame_id_t frameId;
  // fetch page
  if(free_list_.size()>0){
      frameId=free_list_.front();
      free_list_.pop_front();
      index=static_cast<int>(frameId);
  }else{
      replacer_->Victim(&frameId);
      index=static_cast<int>(frameId);

      // flush dirty page to disk
      if(pages_[index].is_dirty_){
          disk_manager_->WritePage(pages_[index].GetPageId(),pages_[index].GetData());
      }
      // unregister old page
      page_table_.erase(pages_[index].GetPageId());
  }

  *page_id=AllocatePage();
  // LOG_INFO("Begin to allocate. Index= %d, Pool Size= %d, Page Id= %d",index,pool_size_,*page_id);
  // Reset metadata
  pages_[index].ResetMemory();
  pages_[index].page_id_=*page_id;
  pages_[index].is_dirty_= false;
  pages_[index].pin_count_=1;
  // register to page table
  page_table_[*page_id]=frameId;
  // LOG_INFO("Allocate Page %d",*page_id);
  return &pages_[index];
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  // HIT PAGE TABLE
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t fId = page_table_[page_id];
    int index = static_cast<int>(fId);
    pages_[index].pin_count_++;
    return &pages_[index];
  }

  frame_id_t frameId;
  int32_t index=-1;
  if(free_list_.size()>0){
      frameId=free_list_.front();
      free_list_.pop_front();
      index=static_cast<int>(frameId);
  }else if(free_list_.size()==0){
      // find a page in replacer and replace
      replacer_->Victim(&frameId);
      index=static_cast<int>(frameId);
  }
  if(index<0 || index>8192 || pages_[index].GetPinCount()!=0){
      return nullptr;
  }

  // flush r if possible
  if(pages_[index].is_dirty_){
      disk_manager_->WritePage(pages_[index].page_id_,pages_[index].GetData());
  }

  // erase old page and insert new page
  page_table_.erase(pages_[index].page_id_);
  page_table_[page_id]=frameId;
  // update metadata and read data
  pages_[index].page_id_=page_id;
  pages_[index].pin_count_=1;
  disk_manager_->ReadPage(page_id,pages_[index].GetData());
  pages_[index].is_dirty_= false;
  return &pages_[index];
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if (page_table_.find(page_id) == page_table_.end()) {
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
  pages_[index].is_dirty_= false;
  free_list_.push_back(fid);
  DeallocatePage(page_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t fid = page_table_[page_id];
  int index = static_cast<int>(fid);

  pages_[index].is_dirty_=is_dirty;
  if (pages_[index].pin_count_ <= 0) {
    return false;
  }
  pages_[index].pin_count_--;
  // move unpinned lru replacer
  if (pages_[index].pin_count_ == 0) {
    replacer_->Unpin(fid);
    if(replacer_->Size()>pool_size_){
        replacer_->Victim(&fid);
        index=static_cast<int>(fid);
        if(pages_[index].is_dirty_){
            disk_manager_->WritePage(pages_[index].GetPageId(),pages_[index].GetData());
        }
        DeletePgImp(pages_[index].GetPageId());
     }
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
