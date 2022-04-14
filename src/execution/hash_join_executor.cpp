//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->left_executor_ =
      static_cast<std::unique_ptr<AbstractExecutor, std::default_delete<AbstractExecutor>> &&>(left_child);
  this->right_executor_ =
      static_cast<std::unique_ptr<AbstractExecutor, std::default_delete<AbstractExecutor>> &&>(right_child);
  this->hast_table_ = new SimpleHashTable(plan_->LeftJoinKeyExpression());
}

/**
 * Load left table into hash table.
 */
void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    hast_table_->Insert(&tuple, const_cast<Schema *>(left_executor_->GetOutputSchema()));
  }
  AbstractExpression *expression = const_cast<AbstractExpression *>(plan_->RightJoinKeyExpression());
  while (right_executor_->Next(&tuple, &rid)) {
    std::vector<Tuple> couples =
        hast_table_->Get(&tuple, const_cast<Schema *>(right_executor_->GetOutputSchema()), expression);
    if (couples.empty()){
      continue;
    }
    for (auto & couple : couples) {
      std::vector<Value> output;
      for (const auto& col : GetOutputSchema()->GetColumns()) {
        output.push_back(col.GetExpr()->EvaluateJoin(&couple, left_executor_->GetOutputSchema(), &tuple,
                                                     right_executor_->GetOutputSchema()));
      }
      result_.emplace_back(output, plan_->OutputSchema());
    }
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (result_.empty()){
    return false;
  }
  *tuple = result_.front();
  *rid = tuple->GetRid();
  result_.pop_front();
  return true;
}

}  // namespace bustub
