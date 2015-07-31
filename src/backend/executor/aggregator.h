/*-------------------------------------------------------------------------
 *
 * aggregator.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/executor/aggregator.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/value_factory.h"
#include "backend/executor/abstract_executor.h"
#include "backend/expression/container_tuple.h"
#include "backend/storage/abstract_backend.h"
#include "backend/storage/data_table.h"
#include "backend/planner/aggregate_node.h"

#include <unordered_map>

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//

namespace peloton {
namespace executor {

/*
 * Base class for an individual aggregate that aggregates a specific
 * column for a group
 */
class Agg {
 public:
  virtual ~Agg() {}
  virtual void Advance(const Value val) = 0;
  virtual Value Finalize() = 0;
};

class SumAgg : public Agg {
 public:
  SumAgg() : have_advanced(false) {
    // aggregate initialized on first advance
  }

  void Advance(const Value val) {
    if (val.IsNull()) {
      return;
    }
    if (!have_advanced) {
      aggregate = val;
      have_advanced = true;
    } else {
      aggregate = aggregate.OpAdd(val);
    }
  }

  Value Finalize() {
    if (!have_advanced) {
      return ValueFactory::GetNullValue();
    }
    return aggregate;
  }

 private:
  Value aggregate;

  bool have_advanced;
};

class AvgAgg : public Agg {
 public:
  AvgAgg(bool is_weighted) : is_weighted(is_weighted), count(0) {
    default_delta = ValueFactory::GetIntegerValue(1);
  }

  void Advance(const Value val) { this->Advance(val, default_delta); }

  void Advance(const Value val, const Value delta) {
    if (val.IsNull()) {
      return;
    }

    // Weighted average
    if (is_weighted) {
      Value weighted_val = val.OpMultiply(delta);
      if (count == 0) {
        aggregate = weighted_val;
      } else {
        aggregate = aggregate.OpAdd(weighted_val);
      }
      count += ValuePeeker::PeekAsInteger(delta);
    } else {
      if (count == 0) {
        aggregate = val;
      } else {
        aggregate = aggregate.OpAdd(val);
      }
      count += 1;
    }
  }

  Value Finalize() {
    if (count == 0) {
      return ValueFactory::GetNullValue();
    }
    const Value final_result = aggregate.OpDivide(
        ValueFactory::GetDoubleValue(static_cast<double>(count)));
    return final_result;
  }

 private:
  /** @brief aggregate initialized on first advance. */
  Value aggregate;

  /** @brief  default delta for weighted average */
  Value default_delta;

  bool is_weighted;

  /** @brief count of elements aggregated */
  int64_t count;
};

// count always holds integer
class CountAgg : public Agg {
 public:
  CountAgg() : count(0) {}

  void Advance(const Value val) {
    if (val.IsNull()) {
      return;
    }
    count++;
  }

  Value Finalize() { return ValueFactory::GetBigIntValue(count); }

 private:
  int64_t count;
};

class CountStarAgg : public Agg {
 public:
  CountStarAgg() : count(0) {}

  void Advance(const Value val __attribute__((unused))) { ++count; }

  Value Finalize() { return ValueFactory::GetBigIntValue(count); }

 private:
  int64_t count;
};

class MaxAgg : public Agg {
 public:
  MaxAgg() : have_advanced(false) { aggregate.SetNull(); }

  void Advance(const Value val) {
    if (val.IsNull()) {
      return;
    }
    if (!have_advanced) {
      aggregate = val;
      have_advanced = true;
    } else {
      aggregate = aggregate.OpMax(val);
    }
  }

  Value Finalize() {
    if (!have_advanced) {
      return ValueFactory::GetNullValue();
    }
    return aggregate;
  }

 private:
  Value aggregate;

  bool have_advanced;
};

class MinAgg : public Agg {
 public:
  MinAgg() : have_advanced(false) { aggregate.SetNull(); }

  void Advance(const Value val) {
    if (val.IsNull()) {
      return;
    }

    if (!have_advanced) {
      aggregate = val;
      have_advanced = true;
    } else {
      aggregate = aggregate.OpMin(val);
    }
  }

  Value Finalize() {
    if (!have_advanced) {
      return ValueFactory::GetNullValue();
    }
    return aggregate;
  }

 private:
  Value aggregate;

  bool have_advanced;
};

/** brief Create an instance of an aggregator for the specified aggregate */
Agg *GetAggInstance(ExpressionType agg_type);

/**
 * List of aggregates for a specific group.
 */
struct AggregateList {
  // A tuple from the group of tuples being aggregated.
  // Source of pass through columns.
  AbstractTuple *group_tuple;

  // The aggregates for each column for this group
  Agg **aggregates;
};

/*
 * Type of the hash table used to store aggregates for each group.
 */
typedef std::unordered_map<storage::Tuple, AggregateList *,
                           storage::TupleHasher,
                           storage::TupleComparator> HashAggregateMapType;

/*
 * Interface for an aggregator (not an an individual aggregate)
 *
 * This will aggregate some number of tuples and produce the results in the
 * provided output .
 */
template <PlanNodeType aggregate_type>
class Aggregator {
 public:
  Aggregator(const planner::AggregateNode *node,
             storage::DataTable *output_table,
             const concurrency::Transaction *transaction_id);

  bool Advance(AbstractTuple *next_tuple, AbstractTuple *prev_tuple);

  bool Finalize(AbstractTuple *prev_tuple);

  ~Aggregator();

 private:
  /** @brief Plan node */
  const planner::AggregateNode *node;

  /** @brief Output table */
  storage::DataTable *output_table;

  /** @brief Transaction id for mutating table */
  const concurrency::Transaction *transaction;

  /** @brief Aggregates */
  Agg **aggregates = nullptr;

  /** @brief Group by columns */
  std::vector<oid_t> group_by_columns;

  /** @brief Aggregate columns */
  std::vector<oid_t> aggregate_columns;

  /** @brief Aggregate types */
  std::vector<ExpressionType> aggregate_types;

  //===--------------------------------------------------------------------===//
  // Used only for hash aggregation
  //===--------------------------------------------------------------------===//

  /** @brief Group by key tuple used */
  storage::Tuple *group_by_key_tuple = nullptr;

  /** @brief Hash table */
  HashAggregateMapType aggregates_map;

  /** @brief Group by key tuple used */
  const catalog::Schema *group_by_key_schema = nullptr;
};

}  // namespace executor
}  // namespace peloton
