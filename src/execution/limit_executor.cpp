//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
    this->plan_=plan;
    this->child_executor_= static_cast<std::unique_ptr<AbstractExecutor, std::default_delete<AbstractExecutor>> &&>(child_executor);
}

void LimitExecutor::Init() {
    child_executor_->Init();
    tuple_numbers_=0;
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
    if(tuple_numbers_< plan_->GetLimit()){
        tuple_numbers_++;
        // if tuple not enough for limit then return all
        return child_executor_->Next(tuple,rid);
    }
    return false;
}

}  // namespace bustub
