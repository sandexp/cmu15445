//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
    this->plan_=plan;
    this->child_executor_= static_cast<std::unique_ptr<AbstractExecutor, std::default_delete<AbstractExecutor>> &&>(child_executor);
}

void DeleteExecutor::Init() {
    child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
    Transaction* txn=GetExecutorContext()->GetTransaction();
    TableInfo* info=GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
    std::vector<IndexInfo *> indexes=GetExecutorContext()->GetCatalog()->GetTableIndexes(info->name_);

    while (child_executor_->Next(tuple,rid)){
        info->table_->MarkDelete(*rid,txn);
        // delete entries from indexes
        // update index info
        for (size_t i = 0; i < indexes.size(); ++i) {
            indexes[i]->index_->DeleteEntry(*tuple,*rid,txn);
        }
    }
    return false;
}

}  // namespace bustub
