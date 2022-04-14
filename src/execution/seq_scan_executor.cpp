//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void SeqScanExecutor::Init() {
  table_oid_t t_id = plan_->GetTableOid();
  TableInfo *info = GetExecutorContext()->GetCatalog()->GetTable(t_id);

  assert(info != nullptr && info->table_ != nullptr);
  assert(plan_->GetType()==PlanType::SeqScan);
  // Init table iterator_
  iterator_ = info->table_->Begin(GetExecutorContext()->GetTransaction());
  const AbstractExpression *predicate = plan_->GetPredicate();
  const Schema *schema = &info->schema_;
  Tuple tuple;
  while (iterator_ != info->table_->End()){
    tuple = *iterator_;
    ++iterator_;
    if(predicate == nullptr || predicate->Evaluate(&tuple, schema).GetAs<bool>()){
      result_.push_back(tuple);
    }
  }
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if(result_.empty()){
    return false;
  }
  *tuple = result_.front();
  *rid = tuple->GetRid();
  result_.pop_front();
  return true;
}

std::list<Tuple> SeqScanExecutor::GetTemplateTuples() {
  return result_;
}

}  // namespace bustub
