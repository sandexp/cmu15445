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

#include <execution/executor_factory.h>
#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
    this->plan_=plan;
    this->left_executor_= static_cast<std::unique_ptr<AbstractExecutor, std::default_delete<AbstractExecutor>> &&>(left_executor);
    this->right_executor_= static_cast<std::unique_ptr<AbstractExecutor, std::default_delete<AbstractExecutor>> &&>(right_executor);
}

// Load Left table when init executor
void NestedLoopJoinExecutor::Init() {
    left_executor_->Init();
    right_executor_->Init();
    Tuple tuple;
    RID rid;
    while (left_executor_->Next(&tuple,&rid)){
        cache_.push_back(tuple);
    }

    while (right_executor_->Next(&tuple,&rid)){
        // nest join with cache
        for (size_t i = 0; i < cache_.size(); ++i) {
            bool predicate= plan_->Predicate()->EvaluateJoin(&cache_[i],left_executor_->GetOutputSchema(),&tuple,right_executor_->GetOutputSchema()).GetAs<bool>();
            if(predicate){
                std::vector<Value> output;
                for(auto col:GetOutputSchema()->GetColumns()){
                    output.push_back(col.GetExpr()->EvaluateJoin(&cache_[i],left_executor_->GetOutputSchema(),&tuple,right_executor_->GetOutputSchema()));
                }
                result.push_back(Tuple(output,plan_->OutputSchema()));
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
    if(result.size()<=0){
        return false;
    }
    *tuple=result.front();
    *rid=tuple->GetRid();
    result.pop_front();
    return true;
}

}  // namespace bustub
