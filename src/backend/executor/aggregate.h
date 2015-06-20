/*-------------------------------------------------------------------------
 *
 * aggregate.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/executor/aggregate.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/value_factory.h"
#include "backend/executor/abstract_executor.h"

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//

namespace nstore {
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
  SumAgg() :
    have_advanced(false) {
    // aggregate initialized on first advance
  }

  void Advance(const Value val){
    if (val.IsNull()) {
      return;
    }
    if (!have_advanced) {
      aggregate = val;
      have_advanced = true;
    }
    else {
      aggregate = aggregate.OpAdd(val);
    }
  }

  Value Finalize()
  {
    if (!have_advanced)
    {
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
  AvgAgg(bool is_weighted) :
    is_weighted(is_weighted),
    count(0) {
    default_delta = ValueFactory::GetIntegerValue(1);
  }

  void Advance(const Value val) {
    this->Advance(val, default_delta);
  }

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
      count += delta.GetInteger();
    }
    else {
      if (count == 0) {
        aggregate = val;
      } else {
        aggregate = aggregate.OpAdd(val);
      }
      count += 1;
    }

  }

  Value Finalize() {
    if (count == 0)
    {
      return ValueFactory::GetNullValue();
    }
    const Value final_result =
        aggregate.OpDivide(ValueFactory::GetDoubleValue(static_cast<double>(count)));
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

//count always holds integer
class CountAgg : public Agg {
 public:
  CountAgg() :
    count(0){
  }

  void Advance(const Value val) {
    if (val.IsNull()) {
      return;
    }
    count++;
  }

  Value Finalize() {
    return ValueFactory::GetBigIntValue(count);
  }

 private:
  int64_t count;
};

class CountStarAgg : public Agg {
 public:
  CountStarAgg() :
    count(0) {
  }

  void Advance(const Value val) {
    ++count;
  }

  Value Finalize() {
    return ValueFactory::GetBigIntValue(count);
  }

 private:
  int64_t count;
};

class MaxAgg : public Agg {
 public:
  MaxAgg() :
    have_advanced(false) {
    aggregate.SetNull();
  }

  void Advance(const Value val) {
    if (val.IsNull()) {
      return;
    }
    if (!have_advanced) {
      aggregate = val;
      have_advanced = true;
    }
    else {
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
  MinAgg() :
    have_advanced(false) {
    aggregate.SetNull();
  }

  void Advance(const Value val) {
    if (val.IsNull()) {
      return;
    }

    if (!have_advanced){
      aggregate = val;
      have_advanced = true;
    }
    else {
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

/*
 * Create an instance of an aggregator for the specified aggregate
 * type, column type, and result type. The object is constructed in
 * memory from the provided memrory pool.
 */
inline Agg* GetAggInstance(Backend* backend, ExpressionType agg_type) {
  Agg* aggregator;

  switch (agg_type) {
    case EXPRESSION_TYPE_AGGREGATE_COUNT:
      aggregator = new (backend->Allocate(sizeof(CountAgg))) CountAgg();
      break;
    case EXPRESSION_TYPE_AGGREGATE_COUNT_STAR:
      aggregator = new (backend->Allocate(sizeof(CountStarAgg))) CountStarAgg();
      break;
    case EXPRESSION_TYPE_AGGREGATE_SUM:
      aggregator = new (backend->Allocate(sizeof(SumAgg))) SumAgg();
      break;
    case EXPRESSION_TYPE_AGGREGATE_AVG:
      aggregator = new (backend->Allocate(sizeof(AvgAgg))) AvgAgg(false);
      break;
    case EXPRESSION_TYPE_AGGREGATE_WEIGHTED_AVG:
      aggregator = new (backend->Allocate(sizeof(AvgAgg))) AvgAgg(true);
      break;
    case EXPRESSION_TYPE_AGGREGATE_MIN:
      aggregator = new (backend->Allocate(sizeof(MinAgg))) MinAgg();
      break;
    case EXPRESSION_TYPE_AGGREGATE_MAX  :
      aggregator = new (backend->Allocate(sizeof(MaxAgg))) MaxAgg();
      break;
    default: {
      std::string message =  "Unknown aggregate type " + std::to_string(agg_type);
      throw UnknownTypeException(message);
    }
  }

  return aggregator;
}


} // namespace executor
} // namespace nstore
