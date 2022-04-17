//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), iterator_(ht_.Begin()) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  const Schema *output_schema = plan_->OutputSchema();
  while (child_executor_->Next(&tuple, &rid)) {
    DistinctKey key = ProductDistinctKey(tuple, output_schema);
    ht_.Insert(key, tuple);
  }
  iterator_ = ht_.Begin();
}

/**
 * Distinct tuple from child executor, if tuple's hash code has been occurred in table, we will fill it out with result_
 * false
 * @param tuple
 * @param rid
 * @return whether we need this tuple
 */
bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (iterator_ == ht_.End()) {
    return false;
  }
  *tuple = iterator_.Val();
  *rid = tuple->GetRid();
  ++iterator_;
  return true;
}

DistinctKey DistinctExecutor::ProductDistinctKey(const Tuple& tuple, const Schema *output_schema) {
  std::vector<Value> products;
  for (size_t i = 0; i < output_schema->GetColumnCount(); ++i) {
    products.emplace_back(tuple.GetValue(output_schema, i));
  }
  return {products};
}

}  // namespace bustub
