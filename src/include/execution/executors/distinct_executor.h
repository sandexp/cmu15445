//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executors/abstract_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

struct DistinctKey {
  std::vector<Value> distinct_keys_;

  bool operator==(const DistinctKey &other) const {
    for (uint32_t i = 0; i < other.distinct_keys_.size(); i++) {
      if (distinct_keys_[i].CompareEquals(other.distinct_keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {

template <>
struct hash<bustub::DistinctKey> {
  std::size_t operator()(const bustub::DistinctKey &distinct_key) const {
    size_t curr_hash = 0;
    for (const auto &key : distinct_key.distinct_keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

class SimpleDistinctHashTable {
 public:
  void Insert(const DistinctKey &distinct_key, const Tuple &tuple) { ht_.insert({distinct_key, tuple}); }

  class Iterator {
   public:
    explicit Iterator(std::unordered_map<DistinctKey, Tuple>::const_iterator iter) : iter_{iter} {};

    const DistinctKey &Key() { return iter_->first; }

    const Tuple &Val() { return iter_->second; }

    Iterator &operator++() {
      ++iter_;
      return *this;
    }

    bool operator==(const Iterator &other) { return this->iter_ == other.iter_; }

    bool operator!=(const Iterator &other) { return this->iter_ != other.iter_; }

   private:
    std::unordered_map<DistinctKey, Tuple>::const_iterator iter_;
  };

  Iterator Begin() { return Iterator(ht_.cbegin()); }

  Iterator End() { return Iterator(ht_.cend()); }

 private:
  std::unordered_map<DistinctKey, Tuple> ht_{};
};

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  DistinctKey ProductDistinctKey(const Tuple& tuple, const Schema *output_schema);

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** Hash table to implement distinct*/
  SimpleDistinctHashTable ht_;
  /** Hash table iterator */
  SimpleDistinctHashTable::Iterator iterator_;
};
}  // namespace bustub
