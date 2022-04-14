//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

class SimpleHashTable {
 public:
  explicit SimpleHashTable(const AbstractExpression *key_expr) { this->key_expr_ = key_expr; }

  /**
   * Insert Tuple into current hash table
   * @param tuple
   * @param schema
   */
  void Insert(Tuple *tuple, Schema *schema) {
    Value value = key_expr_->Evaluate(tuple, schema);
    uint64_t hash_code = GetHashCode(&value);
    kt_[hash_code].push_back(*tuple);
  }

  /**
   * Get equivalent value of given tuple
   * @param tuple target tuple
   * @param schema schema info
   * @param expression outside expression
   * @return equivalent value list
   */
  std::vector<Tuple> Get(Tuple *tuple, Schema *schema, AbstractExpression *expression) {
    Value value = expression->Evaluate(tuple, schema);
    uint64_t hash_code = GetHashCode(&value);
    if (kt_.find(hash_code) == kt_.end()) {
      return {};
    }
    return kt_[hash_code];
  }

 private:
  uint64_t GetHashCode(Value *value) { return hash_fn_.GetHash(*value); }

 private:
  /** Hash table which storage tuples. */
  std::unordered_map<uint64_t, std::vector<Tuple>> kt_{};
  /** Key Evaluator */
  const AbstractExpression *key_expr_;
  /** Hash Function */
  HashFunction<Value> hash_fn_;
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /**Left executor */
  std::unique_ptr<AbstractExecutor> left_executor_;
  /**Right executor*/
  std::unique_ptr<AbstractExecutor> right_executor_;
  /**Simple Hash Table */
  SimpleHashTable *hast_table_;
  /** Result list**/
  std::list<Tuple> result_;
};

}  // namespace bustub
