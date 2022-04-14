//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <execution/executor_factory.h>

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->left_executor_ =
      static_cast<std::unique_ptr<AbstractExecutor, std::default_delete<AbstractExecutor>> &&>(left_executor);
  this->right_executor_ =
      static_cast<std::unique_ptr<AbstractExecutor, std::default_delete<AbstractExecutor>> &&>(right_executor);
}

// Load Left table when init executor
void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    caches_.push_back(tuple);
  }

  while (right_executor_->Next(&tuple, &rid)) {
    // nest join with cache
    for (auto & cache : caches_) {
      bool predicate =
          plan_->Predicate()
              ->EvaluateJoin(&cache, left_executor_->GetOutputSchema(), &tuple, right_executor_->GetOutputSchema())
              .GetAs<bool>();
      if (predicate) {
        std::vector<Value> output;
        for (const auto& col : GetOutputSchema()->GetColumns()) {
          output.push_back(col.GetExpr()->EvaluateJoin(&cache, left_executor_->GetOutputSchema(), &tuple,
                                                       right_executor_->GetOutputSchema()));
        }
        result_.emplace_back(output, plan_->OutputSchema());
      }
    }
  }
}

/**
 * Load right table and compare to left table of caching.
 * @param tuple
 * @param rid
 * @return
 */
bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (result_.empty()) {
    return false;
  }
  *tuple = result_.front();
  *rid = tuple->GetRid();
  result_.pop_front();
  return true;
}

}  // namespace bustub
