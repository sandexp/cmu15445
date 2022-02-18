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

#include <memory>
#include <execution/execution_engine.h>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
    this->plan_=plan;

}

/**
 * Load ctx into environment
 */
void InsertExecutor::Init() {
    table_oid_t tId=plan_->TableOid();
    TableInfo* info=GetExecutorContext()->GetCatalog()->GetTable(tId);
    assert(info!= nullptr);
}

/**
 * Execute insert logic, if using sub-plan, we first need to query and insert result into table
 * @param tuple
 * @param rid
 * @return
 */
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {

    table_oid_t tId=plan_->TableOid();
    TableInfo* table_info=GetExecutorContext()->GetCatalog()->GetTable(tId);
    std::vector<IndexInfo*> index_infos=GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
    Transaction* txn=GetExecutorContext()->GetTransaction();
    Schema schema=table_info->schema_;

    // step 1: insert into table
    if(plan_->IsRawInsert()){
        // Raw insert
        std::vector<std::vector<Value>> values=plan_->RawValues();
        // translate raw values to tuple
        for (size_t i = 0; i < values.size(); ++i) {
            // insert into table
            Tuple target=Tuple(values[i],&schema);
            bool is_success=table_info->table_->InsertTuple(target, rid,txn);
            if(is_success){
                // step 2: update index with extendible hash table
                for (size_t j = 0; j < index_infos.size(); ++j) {
                    UpdateIndex(index_infos[j],target,rid,txn);
                }
            }
        }
    }else{
        // plan insert
        AbstractPlanNode* abs_plan= const_cast<AbstractPlanNode *>(plan_->GetChildAt(0));
        SeqScanPlanNode* sub_plan= dynamic_cast<SeqScanPlanNode *>(abs_plan);

        SeqScanExecutor sub_executor(exec_ctx_,sub_plan);
        sub_executor.Init();

        Tuple sub_tuple;
        RID sub_rid;

        while (sub_executor.Next(&sub_tuple, &sub_rid)) {
            bool is_success=table_info->table_->InsertTuple(sub_tuple,&sub_rid,txn);
            if(is_success){
                // step 2: update index with extendible hash table
                for (size_t i = 0; i < index_infos.size(); ++i) {
                    UpdateIndex(index_infos[i],sub_tuple,rid,txn);
                }
            }
        }
    }
    return false;
}

void InsertExecutor::UpdateIndex(IndexInfo* index_info,Tuple tuple,RID* rid,Transaction* txn){
    index_info->index_->InsertEntry(tuple,*rid,txn);
}

}  // namespace bustub
