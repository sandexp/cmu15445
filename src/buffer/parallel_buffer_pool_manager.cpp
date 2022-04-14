//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "common/logger.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstance
  instances_ = new BufferPoolManagerInstance *[num_instances];
  for (size_t i = 0; i < num_instances; ++i) {
    instances_[i] = new BufferPoolManagerInstance(pool_size, static_cast<uint32_t>(num_instances),
                                                  static_cast<uint32_t>(i), disk_manager);
  }
  start_index_ = 0;
  num_instances_ = num_instances;
  pool_size_ = pool_size;
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() { delete[] instances_; }

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return pool_size_ * num_instances_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  if (page_distribute_.count(page_id) == 0) {
    page_distribute_[page_id] = start_index_ % num_instances_;
    start_index_++;
  }
  return instances_[page_distribute_[page_id]];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  // route a manager, hand this task to that manager
  std::lock_guard<std::mutex> guard(latch_);
  BufferPoolManagerInstance *manager = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return manager->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  // route a manager, hand this task to that manager, if page pin reduce to zero, unregister it
  std::lock_guard<std::mutex> guard(latch_);
  BufferPoolManagerInstance *manager = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  bool res = manager->UnpinPage(page_id, is_dirty);
  return res;
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  std::lock_guard<std::mutex> guard(latch_);
  BufferPoolManagerInstance *manager = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return manager->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  std::lock_guard<std::mutex> guard(latch_);
  Page *page;
  bool flag = false;
  for (size_t i = 0; i < num_instances_; ++i) {
    page = instances_[(start_index_ + i) % num_instances_]->NewPage(page_id);
    if (nullptr != page) {
      flag = true;
      start_index_ = start_index_ + i;
      page_distribute_[*page_id] = start_index_ % num_instances_;
      break;
    }
  }
  if (!flag) {
    return nullptr;
  }
  return page;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  std::lock_guard<std::mutex> guard(latch_);
  BufferPoolManagerInstance *manager = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return manager->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  std::lock_guard<std::mutex> guard(latch_);
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; ++i) {
    instances_[i]->FlushAllPages();
  }
}

}  // namespace bustub
