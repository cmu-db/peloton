//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregator.h
//
// Identification: src/include/executor/aggregator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>
#include <unordered_set>

#include "common/container_tuple.h"
#include "executor/abstract_executor.h"
#include "planner/aggregate_plan.h"
#include "type/value_factory.h"
#include "type/value_peeker.h"

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//

namespace peloton {

namespace storage {
class AbstractTable;
}

namespace executor {

/*
 * Base class for an individual aggregate that aggregates a specific
 * column for a group
 */
class AbstractAttributeAggregator {
 public:
  virtual ~AbstractAttributeAggregator();

  void SetDistinct(bool distinct) { is_distinct_ = distinct; }

  void Advance(const type::Value val);
  type::Value Finalize();

  virtual void DAdvance(const type::Value &val) = 0;
  virtual type::Value DFinalize() = 0;

 private:
  typedef std::unordered_set<type::Value, type::Value::hash,
                             type::Value::equal_to>
      DistinctSetType;

  DistinctSetType distinct_set_;

  bool is_distinct_ = false;
};

class SumAggregator : public AbstractAttributeAggregator {
 public:
  SumAggregator() : have_advanced(false) {
    // aggregate initialized on first advance
  }

  void DAdvance(const type::Value &val) {
    if (val.IsNull()) {
      return;
    }
    if (!have_advanced) {
      aggregate = val.Copy();
      have_advanced = true;
    } else {
      aggregate = aggregate.Add(val);
    }
  }

  type::Value DFinalize() {
    if (!have_advanced)
      return type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER);
    return aggregate;
  }

 private:
  type::Value aggregate;

  bool have_advanced;
};

class AvgAggregator : public AbstractAttributeAggregator {
 public:
  AvgAggregator(bool is_weighted) : is_weighted(is_weighted), count(0) {
    default_delta = type::ValueFactory::GetIntegerValue(1);
  }

  void DAdvance(const type::Value &val) { this->DAdvance(val, default_delta); }

  void DAdvance(const type::Value &val, const type::Value &delta) {
    if (val.IsNull()) {
      return;
    }

    // Weighted average
    if (is_weighted) {
      type::Value weighted_val = val.Multiply(delta);
      if (count == 0) {
        aggregate = weighted_val;
      } else {
        aggregate = aggregate.Add(weighted_val);
      }
      count += type::ValuePeeker::PeekInteger(delta);
    } else {
      if (count == 0) {
        aggregate = val.Copy();
      } else {
        aggregate = aggregate.Add(val);
      }
      count += 1;
    }
  }

  type::Value DFinalize() {
    if (count == 0) {
      return type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER);
    }
    type::Value final_result = aggregate.Divide(
        type::ValueFactory::GetDecimalValue(static_cast<double>(count)));
    return final_result;
  }

 private:
  /** @brief aggregate initialized on first advance. */
  type::Value aggregate;

  /** @brief  default delta for weighted average */
  type::Value default_delta;

  bool is_weighted;

  /** @brief count of elements aggregated */
  int64_t count;
};

// count always holds integer
class CountAggregator : public AbstractAttributeAggregator {
 public:
  CountAggregator() : count(0) {}

  void DAdvance(const type::Value &val) {
    if (val.IsNull()) {
      return;
    }
    count++;
  }

  type::Value DFinalize() { return type::ValueFactory::GetBigIntValue(count); }

 private:
  int64_t count;
};

class CountStarAggregator : public AbstractAttributeAggregator {
 public:
  CountStarAggregator() : count(0) {}

  void DAdvance(const type::Value &val UNUSED_ATTRIBUTE) { ++count; }

  type::Value DFinalize() { return type::ValueFactory::GetBigIntValue(count); }

 private:
  int64_t count;
};

class MaxAggregator : public AbstractAttributeAggregator {
 public:
  MaxAggregator() : have_advanced(false) {
    aggregate = type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER);
  }

  void DAdvance(const type::Value &val) {
    if (val.IsNull()) {
      return;
    }
    if (!have_advanced) {
      aggregate = val.Copy();
      have_advanced = true;
    } else {
      aggregate = aggregate.Max(val);
    }
  }

  type::Value DFinalize() { return aggregate; }

 private:
  type::Value aggregate;

  bool have_advanced;
};

class MinAggregator : public AbstractAttributeAggregator {
 public:
  MinAggregator() : have_advanced(false) {
    aggregate = type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER);
  }

  void DAdvance(const type::Value &val) {
    if (val.IsNull()) {
      return;
    }

    if (!have_advanced) {
      aggregate = val.Copy();
      have_advanced = true;
    } else {
      aggregate = aggregate.Min(val);
    }
  }

  type::Value DFinalize() { return aggregate; }

 private:
  type::Value aggregate;

  bool have_advanced;
};

/** brief Create an instance of an aggregator for the specified aggregate */
AbstractAttributeAggregator *GetAttributeAggregatorInstance(
    ExpressionType agg_type);

/*
 * Interface for an aggregator (not an an individual attribute aggregate)
 *
 * This will aggregate some number of tuples and produce the results in the
 * provided output .
 */
class AbstractAggregator {
 public:
  AbstractAggregator(const planner::AggregatePlan *node,
                     storage::AbstractTable *output_table,
                     executor::ExecutorContext *econtext)
      : node(node), output_table(output_table), executor_context(econtext) {}

  virtual bool Advance(AbstractTuple *next_tuple) = 0;

  virtual bool Finalize() = 0;

  virtual ~AbstractAggregator() {}

 protected:
  /** @brief Plan node */
  const planner::AggregatePlan *node;

  /** @brief Output table */
  storage::AbstractTable *output_table;

  /** @brief Executor Context */
  executor::ExecutorContext *executor_context = nullptr;
};

/**
 * @brief Used when input is NOT sorted.
 * Will maintain an internal hash table.
 */
class HashAggregator : public AbstractAggregator {
 public:
  HashAggregator(const planner::AggregatePlan *node,
                 storage::AbstractTable *output_table,
                 executor::ExecutorContext *econtext, size_t num_input_columns);

  bool Advance(AbstractTuple *next_tuple) override;

  bool Finalize() override;

  ~HashAggregator();

 private:
  const size_t num_input_columns;

  /** List of aggregates for a specific group. */
  struct AggregateList {
    // Keep a deep copy of the first tuple we met of this group
    std::vector<type::Value> first_tuple_values;

    // The aggregates for each column for this group
    AbstractAttributeAggregator **aggregates;
  };

  /** Hash function of internal hash table */
  struct ValueVectorHasher
      : std::unary_function<std::vector<type::Value>, std::size_t> {
    // Generate a 64-bit number for the a vector of value
    size_t operator()(const std::vector<type::Value> &values) const {
      size_t seed = 0;
      for (auto v : values) {
        v.HashCombine(seed);
      }
      return seed;
    }
  };

  struct ValueVectorCmp {
    bool operator()(const std::vector<type::Value> &lhs,
                    const std::vector<type::Value> &rhs) const {
      for (size_t i = 0; i < lhs.size() && i < rhs.size(); i++) {
        if (lhs[i].CompareNotEquals(rhs[i]) == CmpBool::TRUE) return false;
      }
      if (lhs.size() == rhs.size()) return true;
      return false;
    }
  };

  // Default equal_to should works well
  typedef std::unordered_map<std::vector<type::Value>, AggregateList *,
                             ValueVectorHasher, ValueVectorCmp>
      HashAggregateMapType;

  /** @brief Group by key values used */
  std::vector<type::Value> group_by_key_values;

  /** @brief Hash table */
  HashAggregateMapType aggregates_map;
};

/**
 * @brief Used when input is sorted on group-by keys.
 */
class SortedAggregator : public AbstractAggregator {
 public:
  SortedAggregator(const planner::AggregatePlan *node,
                   storage::AbstractTable *output_table,
                   executor::ExecutorContext *econtext,
                   size_t num_input_columns);

  bool Advance(AbstractTuple *next_tuple) override;

  bool Finalize() override;

  ~SortedAggregator();

 private:
  //  AbstractTuple *prev_tuple = nullptr;
  std::vector<type::Value> delegate_tuple_values_;
  const ContainerTuple<std::vector<type::Value>> delegate_tuple_;
  const size_t num_input_columns_;
  AbstractAttributeAggregator **aggregates;
};

/**
 * @brief Used when there's NO Group-By.
 */
class PlainAggregator : public AbstractAggregator {
 public:
  PlainAggregator(const planner::AggregatePlan *node,
                  storage::AbstractTable *output_table,
                  executor::ExecutorContext *econtext);

  bool Advance(AbstractTuple *next_tuple) override;

  bool Finalize() override;

  ~PlainAggregator();

 private:
  AbstractAttributeAggregator **aggregates;
};
}
// namespace executor
}  // namespace peloton
