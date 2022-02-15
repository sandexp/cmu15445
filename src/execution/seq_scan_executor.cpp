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

#include "type/value.h"
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
    plan_=plan;
}

void SeqScanExecutor::Init() {

    table_oid_t tId=plan_->GetTableOid();
    TableInfo* info=GetExecutorContext()->GetCatalog()->GetTable(tId);

    assert(info!= nullptr);
    // Init table iterator
    iterator=info->table_->Begin(GetExecutorContext()->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {

    const AbstractExpression* predicate=plan_->GetPredicate();
    table_oid_t tId=plan_->GetTableOid();
    TableInfo* info=GetExecutorContext()->GetCatalog()->GetTable(tId);
    const Schema* schema=&info->schema_;

    if(iterator==info->table_->End()){
        return false;
    }

    *rid=iterator->GetRid();
    *tuple=*iterator;

    ++iterator;
    if(predicate== nullptr)
        return true;
    Value value=predicate->Evaluate(tuple,schema);
    return value.GetAs<bool>();
}

}  // namespace bustub
