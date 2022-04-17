//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->child_executor_ =
      static_cast<std::unique_ptr<AbstractExecutor, std::default_delete<AbstractExecutor>> &&>(child_executor);
  table_oid_t t_id = plan_->TableOid();
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(t_id);
  table_heap_ = table_info_->table_.get();
}

/**
 * Init Update Operator
 * Just set environment of execution
 */
void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Transaction *txn = GetExecutorContext()->GetTransaction();
  Schema schema = table_info_->schema_;

  while (child_executor_->Next(tuple, rid)) {
    // modify tuple in table
    // update tuple and update index info
    *tuple = GenerateUpdatedTuple(*tuple);
    if (!table_heap_->UpdateTuple(*tuple, *rid, txn)) {
      return false;
    }
    // table_info_->table_->UpdateTuple(updated, updated.GetRid(), txn);
    // update index info
    for (const auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      Tuple insert_tuple = tuple->KeyFromTuple(schema, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      index->index_->InsertEntry(insert_tuple, *rid, exec_ctx_->GetTransaction());
    }
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
