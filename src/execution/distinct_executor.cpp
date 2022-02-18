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
    : AbstractExecutor(exec_ctx) {
    this->plan_=plan;
    this->child_executor_= static_cast<std::unique_ptr<AbstractExecutor, std::default_delete<AbstractExecutor>> &&>(child_executor);
}

void DistinctExecutor::Init() {
    child_executor_->Init();
    hash_table_.clear();
}

/**
 * Distinct tuple from child executor, if tuple's hash code has been occurred in table, we will fill it out with result false
 * @param tuple
 * @param rid
 * @return whether we need this tuple
 */
bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
    bool flag=child_executor_->Next(tuple,rid);
    uint64_t hash_code= GetHashCode(*tuple,*plan_->OutputSchema());
    // LOG_INFO("RID= %d, Data= %s, Res= %d",tuple->GetRid().Get(),tuple->ToString(child_executor_->GetOutputSchema()).c_str(),flag);
    if(hash_table_.find(hash_code)!=hash_table_.end()){
        return false;
    }else{
        hash_table_.insert(hash_code);
        return flag;
    }
}

uint64_t DistinctExecutor::GetHashCode(Tuple tuple,Schema schema){
    uint64_t hash_code=0;
    uint32_t columns=schema.GetColumnCount();
    uint32_t base=1;
    for (uint32_t i = 0; i < columns; ++i) {
        Value value=tuple.GetValue(&schema,i);
        uint32_t base_hash=hash_fn_.GetHash(value.ToString());
        hash_code+=base_hash*base;
        base*=2;
    }
    return hash_code;
}

}  // namespace bustub
