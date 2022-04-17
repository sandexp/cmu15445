//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->child_executor_ =
      static_cast<std::unique_ptr<AbstractExecutor, std::default_delete<AbstractExecutor>> &&>(child_executor);
}

void DeleteExecutor::Init() {}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Transaction *txn = GetExecutorContext()->GetTransaction();
  TableInfo *info = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  std::vector<IndexInfo *> indexes = GetExecutorContext()->GetCatalog()->GetTableIndexes(info->name_);
  Schema schema = info->schema_;

  SeqScanExecutor *executor = dynamic_cast<SeqScanExecutor *>(child_executor_.get());
  assert(executor != nullptr);
  if (executor != nullptr) {
    executor->Init();
    std::list<Tuple> caches = executor->GetTemplateTuples();
    while (!caches.empty()) {
      Tuple tmp = caches.front();
      info->table_->MarkDelete(tmp.GetRid(), txn);

      for (auto &index : indexes) {
        Tuple key_tuple = tmp.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->DeleteEntry(key_tuple, tmp.GetRid(), txn);
      }
      caches.pop_front();
    }
  }
  return false;
}

}  // namespace bustub
