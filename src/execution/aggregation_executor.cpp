//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->child_ = static_cast<std::unique_ptr<AbstractExecutor, std::default_delete<AbstractExecutor>> &&>(child);
  this->hash_table_ = new SimpleAggregationHashTable(plan_->GetAggregates(), plan_->GetAggregateTypes());
  this->iterator_ = hash_table_->Begin();
}

/**
 * Put result_ into hash table
 */
void AggregationExecutor::Init() {
  this->child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    // generate aggregate key
    AggregateKey agg_key;
    for (size_t i = 0; i < plan_->GetGroupBys().size(); ++i) {
      const AbstractExpression *expression = plan_->GetGroupByAt(i);
      Value val = expression->Evaluate(&tuple, child_->GetOutputSchema());
      agg_key.group_bys_.push_back(val);
    }

    // generate aggregate value
    AggregateValue agg_value;
    for (size_t i = 0; i < plan_->GetAggregates().size(); ++i) {
      const AbstractExpression *expression = plan_->GetAggregateAt(i);
      Value val = expression->Evaluate(&tuple, child_->GetOutputSchema());
      agg_value.aggregates_.push_back(val);
    }
    // insert into hash table
    hash_table_->InsertCombine(agg_key, agg_value);
  }
  iterator_ = hash_table_->Begin();
  while (iterator_ != hash_table_->End()) {
    AggregateKey agg_key = iterator_.Key();
    AggregateValue agg_value = iterator_.Val();
    ++iterator_;
    if (plan_->GetHaving() == nullptr ||
        plan_->GetHaving()->EvaluateAggregate(agg_key.group_bys_, agg_value.aggregates_).GetAs<bool>()) {
      std::vector<Value> output;
      for (const auto &col : plan_->OutputSchema()->GetColumns()) {
        output.push_back(col.GetExpr()->EvaluateAggregate(agg_key.group_bys_, agg_value.aggregates_));
      }
      result_.emplace_back(output, plan_->OutputSchema());
    }
  }
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (result_.empty()) {
    return false;
  }
  *tuple = result_.front();
  result_.pop_front();
  return true;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
