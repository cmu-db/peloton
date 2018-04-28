//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_metric.h
//
// Identification: src/statistics/abstract_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <memory>
#include <atomic>

#include "common/platform.h"
#include "common/internal_types.h"
#include "statistics/stat_insertion_point.h"
#include "statistics/abstract_raw_data.h"

namespace peloton {
namespace stats {

class AbstractMetricOld : public Printable {
 public:
  AbstractMetricOld(MetricType type_);
  virtual ~AbstractMetricOld();
  const inline MetricType &GetType() const { return type_; }
  virtual void Reset() = 0;
  virtual const std::string GetInfo() const = 0;
  virtual void Aggregate(AbstractMetricOld &source) = 0;
 private:
  // The type this metric belongs to
  MetricType type_;
};

class Metric {
 public:
  virtual ~Metric() = default;

  // TODO(tianyu): fill arguments
  virtual void OnTransactionBegin() {};
  virtual void OnTransactionCommit(oid_t) {};
  virtual void OnTransactionAbort(oid_t) {};
  virtual void OnTupleRead() {};
  virtual void OnTupleUpdate() {};
  virtual void OnTupleInsert() {};
  virtual void OnTupleDelete() {};
  virtual void OnIndexRead() {};
  virtual void OnIndexUpdate() {};
  virtual void OnIndexInsert() {};
  virtual void OnIndexDelete() {};
  virtual void OnQueryBegin() {};
  virtual void OnQueryEnd() {};

  virtual std::shared_ptr<AbstractRawData> Swap() = 0;
};

template <typename DataType>
class AbstractMetric : public Metric {
 public:
  // Should only be called by the aggregator thread
  std::shared_ptr<AbstractRawData> Swap() override {
    DataType *old_data = raw_data_.exchange(new DataType());
    // We will need to wait for last writer to finish before it's safe
    // to start reading the content. It is okay to block since this
    // method should only be called from the aggregator thread.
    while (!old_data->SafeToAggregate())
      _mm_pause();
    return std::shared_ptr<AbstractRawData>(old_data);
  }

 protected:
  std::atomic<DataType *> raw_data_;
};
}  // namespace stats
}  // namespace peloton
