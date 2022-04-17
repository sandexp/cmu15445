//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <execution/execution_engine.h>
#include <vector>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->child_executor_ = std::move(child_executor);
  this->table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan->TableOid());
}

/**
 * Load ctx into environment
 */
void InsertExecutor::Init() {
  if (plan_->IsRawInsert()) {
    iterator_ = plan_->RawValues().begin();
  } else {
    child_executor_->Init();
  }
}

/**
 * Execute insert logic, if using sub-plan, we first need to query and insert result into table
 * @param tuple
 * @param rid
 * @return
 */
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  std::vector<Tuple> tuples;
  if (plan_->IsRawInsert()) {
    if (iterator_ == plan_->RawValues().end()) {
      return false;
    }
    *tuple = Tuple(*iterator_, &table_info_->schema_);
    iterator_++;
  } else {
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }
  }
  // insert into table heap
  if (!table_info_->table_->InsertTuple(*tuple, rid, GetExecutorContext()->GetTransaction())) {
    return false;
  }
  // insert into index
  Transaction *txn = GetExecutorContext()->GetTransaction();
  for (const auto &index : GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_)) {
    Tuple key_tuple =
        tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
    index->index_->InsertEntry(key_tuple, *rid, txn);
  }
  return Next(tuple, rid);
}

}  // namespace bustub
