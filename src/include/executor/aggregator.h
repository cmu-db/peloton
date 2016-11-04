//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregator.h
//
// Identification: src/include/executor/aggregator.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <unordered_map>
#include <unordered_set>

#include "common/value_factory.h"
#include "executor/abstract_executor.h"
#include "planner/aggregate_plan.h"
#include "common/container_tuple.h"

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//

namespace peloton {

namespace storage {
class DataTable;
}

namespace executor {

/*
 * Base class for an individual aggregate that aggregates a specific
 * column for a group
 */
class Agg {
 public:
  virtual ~Agg();

  void SetDistinct(bool distinct) { is_distinct_ = distinct; }

  void Advance(const common::Value val);
  common::Value Finalize();

  virtual void DAdvance(const common::Value &val) = 0;
  virtual common::Value DFinalize() = 0;

 private:
  typedef std::unordered_set<common::Value , common::Value::hash, common::Value::equal_to>
      DistinctSetType;

  DistinctSetType distinct_set_;

  bool is_distinct_ = false;
};

class SumAgg : public Agg {
 public:
  SumAgg() : have_advanced(false) {
    // aggregate initialized on first advance
  }

  void DAdvance(const common::Value &val) {
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

  common::Value DFinalize() {
    if (!have_advanced)
      return common::ValueFactory::GetNullValueByType(common::Type::INTEGER);
    return aggregate;
  }

 private:
  common::Value aggregate;

  bool have_advanced;
};

class AvgAgg : public Agg {
 public:
  AvgAgg(bool is_weighted) : is_weighted(is_weighted), count(0) {
    default_delta = common::ValueFactory::GetIntegerValue(1);
  }

  void DAdvance(const common::Value& val) {
    this->DAdvance(val, default_delta);
  }

  void DAdvance(const common::Value& val, const common::Value& delta) {
    if (val.IsNull()) {
      return;
    }

    // Weighted average
    if (is_weighted) {
      common::Value weighted_val = val.Multiply(delta);
      if (count == 0) {
        aggregate = weighted_val;
      } else {
        aggregate = aggregate.Add(weighted_val);
      }
      count += common::ValuePeeker::PeekInteger(delta);
    } else {
      if (count == 0) {
        aggregate = val.Copy();
      } else {
        aggregate = aggregate.Add(val);
      }
      count += 1;
    }
  }

  common::Value DFinalize() {
    if (count == 0) {
      return common::ValueFactory::GetNullValueByType(common::Type::INTEGER);
    }
    common::Value final_result = aggregate.Divide(
        common::ValueFactory::GetDoubleValue(static_cast<double>(count)));
    return final_result;
  }

 private:
  /** @brief aggregate initialized on first advance. */
  common::Value aggregate;

  /** @brief  default delta for weighted average */
  common::Value default_delta;

  bool is_weighted;

  /** @brief count of elements aggregated */
  int64_t count;
};

// count always holds integer
class CountAgg : public Agg {
 public:
  CountAgg() : count(0) {}

  void DAdvance(const common::Value &val) {
    if (val.IsNull()) {
      return;
    }
    count++;
  }

  common::Value DFinalize() {
    return common::ValueFactory::GetBigIntValue(count);
  }

 private:
  int64_t count;
};

class CountStarAgg : public Agg {
 public:
  CountStarAgg() : count(0) {}

  void DAdvance(const common::Value &val UNUSED_ATTRIBUTE) { ++count; }

  common::Value DFinalize() {
    return common::ValueFactory::GetBigIntValue(count);
  }

 private:
  int64_t count;
};

class MaxAgg : public Agg {
 public:
  MaxAgg() : have_advanced(false) {
    aggregate =
        common::ValueFactory::GetNullValueByType(common::Type::INTEGER);
  }

  void DAdvance(const common::Value& val) {
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

  common::Value DFinalize() {
    return aggregate;
  }

 private:
  common::Value aggregate;

  bool have_advanced;
};

class MinAgg : public Agg {
 public:
  MinAgg() : have_advanced(false) {
    aggregate =
        common::ValueFactory::GetNullValueByType(common::Type::INTEGER);
  }

  void DAdvance(const common::Value &val) {
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

  common::Value DFinalize() {
    return aggregate;
  }

 private:
  common::Value aggregate;

  bool have_advanced;
};

/** brief Create an instance of an aggregator for the specified aggregate */
Agg *GetAggInstance(ExpressionType agg_type);

/*
 * Interface for an aggregator (not an an individual aggregate)
 *
 * This will aggregate some number of tuples and produce the results in the
 * provided output .
 */
class AbstractAggregator {
 public:
  AbstractAggregator(const planner::AggregatePlan *node,
                     storage::DataTable *output_table,
                     executor::ExecutorContext *econtext)
      : node(node), output_table(output_table), executor_context(econtext) {}

  virtual bool Advance(AbstractTuple *next_tuple) = 0;

  virtual bool Finalize() = 0;

  virtual ~AbstractAggregator() {}

 protected:
  /** @brief Plan node */
  const planner::AggregatePlan *node;

  /** @brief Output table */
  storage::DataTable *output_table;

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
                 storage::DataTable *output_table,
                 executor::ExecutorContext *econtext, size_t num_input_columns);

  bool Advance(AbstractTuple *next_tuple) override;

  bool Finalize() override;

  ~HashAggregator();

 private:
  const size_t num_input_columns;

  /** List of aggregates for a specific group. */
  struct AggregateList {
    // Keep a deep copy of the first tuple we met of this group
    std::vector<common::Value> first_tuple_values;

    // The aggregates for each column for this group
    Agg **aggregates;
  };

  /** Hash function of internal hash table */
  struct ValueVectorHasher
      : std::unary_function<std::vector<common::Value>, std::size_t> {
    // Generate a 64-bit number for the a vector of value
    size_t operator()(const std::vector<common::Value> &values) const {
      size_t seed = 0;
      for (auto v : values) {
        v.HashCombine(seed);
      }
      return seed;
    }
  };

  struct ValueVectorCmp {
    bool operator()(const std::vector<common::Value> &lhs,
                    const std::vector<common::Value> &rhs) const {
      for (size_t i = 0; i < lhs.size() && i < rhs.size(); i++) {
        common::Value neq = (lhs[i].CompareNotEquals(rhs[i]));
        if (neq.IsTrue())
          return false;
      }
      if (lhs.size() == rhs.size())
        return true;
      return false;
    }
  };

  // Default equal_to should works well
  typedef std::unordered_map<std::vector<common::Value>, AggregateList *,
                             ValueVectorHasher, ValueVectorCmp> HashAggregateMapType;

  /** @brief Group by key values used */
  std::vector<common::Value> group_by_key_values;

  /** @brief Hash table */
  HashAggregateMapType aggregates_map;
};

/**
 * @brief Used when input is sorted on group-by keys.
 */
class SortedAggregator : public AbstractAggregator {
 public:
  SortedAggregator(const planner::AggregatePlan *node,
                   storage::DataTable *output_table,
                   executor::ExecutorContext *econtext,
                   size_t num_input_columns);

  bool Advance(AbstractTuple *next_tuple) override;

  bool Finalize() override;

  ~SortedAggregator();

 private:
  //  AbstractTuple *prev_tuple = nullptr;
  std::vector<common::Value> delegate_tuple_values_;
  const expression::ContainerTuple<std::vector<common::Value>> delegate_tuple_;
  const size_t num_input_columns_;
  Agg **aggregates;
};

/**
 * @brief Used when there's NO Group-By.
 */
class PlainAggregator : public AbstractAggregator {
 public:
  PlainAggregator(const planner::AggregatePlan *node,
                  storage::DataTable *output_table,
                  executor::ExecutorContext *econtext);

  bool Advance(AbstractTuple *next_tuple) override;

  bool Finalize() override;

  ~PlainAggregator();

 private:
  Agg **aggregates;
};
}
// namespace executor
}  // namespace peloton
