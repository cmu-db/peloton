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
#include "statistics/stats_event_type.h"
#include "statistics/abstract_raw_data.h"

namespace peloton {
namespace stats {
// TODO(tianyu): Remove this
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
/**
 * @brief Interface representing a metric.
 * A metric is defined to be some piece of logic that processes events generated
 * by the database. @see StatsEventType for a list of available events.
 * It is guaranteed that the appropriate callback method (identified by the
 * naming pattern On[event name]) is invoked and the args filled out with
 * relevant information. To enable safe and efficient collection of data,
 * it is required that all data be collected be written to a RawData
 * object, @see AbstractRawData.
 *
 * While you could write your own metric directly extending from this class,
 * it is recommended that you use @see AbstractMetric class, which takes in
 * an AbstractRawData class a a template argument and implements the tricky
 * concurrent code for you.
 *
 * To write a new Metric, first write your own RawData class, extending from
 * AbstractRawData, and extend from AbstractMetric with your RawData class as
 * template argument. Then, override the event callbacks that you wish to know
 * about. @see AbstractMetric on how to deal with concurrency.
 */
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
  virtual void OnIndexRead(oid_t, size_t) {};
  virtual void OnIndexUpdate(oid_t) {};
  virtual void OnIndexInsert(oid_t) {};
  virtual void OnIndexDelete(oid_t) {};
  virtual void OnQueryBegin() {};
  virtual void OnQueryEnd() {};

  /**
   * @brief Replace RawData with an empty one and return the old one.
   *
   * Data from a metric is collected first into a thread-local storage to
   * ensure efficiency and safety, and periodically aggregated by an aggregator
   * thread into meaningful statistics. However, new statistics can still come
   * in when we aggregate, resulting in race conditions. To avoid this, every
   * time the aggregator wishes to aggregate data, the RawData object is
   * extracted and a fresh one swapped in, so collection continues seamlessly
   * while the aggregator is working.
   *
   * Unless you know what you are doing, you should probably just use the one
   * implemented for you(@see AbstractMetric). Otherwise, it is guaranteed that
   * this method is only called from the aggregator thread, so it is okay to
   * block in this method. As soon as this method returns, the aggregator
   * assumes that it is safe to start reading from the data and discards the
   * data after it's done. Therefore, it is essential that any implementation
   * ensures this method does not return if the collecting thread still can
   * write to the old raw data.
   *
   * @return a shared pointer to the old AbstractRawData
   */
  virtual std::shared_ptr<AbstractRawData> Swap() = 0;
};

/* Forward Declaration */
template<typename DataType>
class AbstractMetric;

/**
 * @brief Wraps around a pointer to an AbstractRawData to allow safe access.
 *
 * This class is always handed out by an AbstractMetric and would prevent an
 * Aggregator from reading or deleting the AbstractRawData it holds. When the
 * object goes out of scope, its destructor will unblock the aggregator. Access
 * to its underlying pointer is always non-blocking.
 *
 * @tparam DataType the type of AbstractRawData this Wrapper holds
 */
template<typename DataType>
class RawDataWrapper {
  friend class AbstractMetric<DataType>;
 public:
  inline RawDataWrapper(RawDataWrapper &&other) = default;

  inline ~RawDataWrapper() { safe_ = true; } // Unblock aggregator

  DISALLOW_COPY(RawDataWrapper);

  /**
   * @return the underlying pointer
   */
  inline DataType *operator->() const { return ptr_; }
 private:
  /**
   * Constructs a new Wrapper instance
   * @param ptr the pointer it wraps around
   * @param safe the boolean variable it uses to signal its lifetime
   */
  inline RawDataWrapper(DataType *ptr, bool &safe) : ptr_(ptr), safe_(safe) {
    safe_ = false;
  }
  DataType *ptr_;
  bool &safe_;
};

/**
 * @brief General purpose implementation to Metric that you should inherit from.
 *
 * This class implements the tricky Swap method and exposes an interface for
 * children class. @see Metric for detail
 *
 * @tparam DataType the type of AbstractRawData this Metric holds
 */
template<typename DataType>
class AbstractMetric : public Metric {
 public:
  /**
   * @see Metric
   *
   * To ensure this method works as intended, be sure to use GetRawData() to
   * access the underlying raw data
   * @return a shared pointer to the old AbstractRawData
   */
  std::shared_ptr<AbstractRawData> Swap() override {
    // After this point, the collector thread can not see old data on new
    // events, but will still be able to write to it, if they loaded the
    // pointer before this operation but haven't written to it yet.
    DataType *old_data = raw_data_.exchange(new DataType());
    // We will need to wait for last writer to finish before it's safe
    // to start reading the content. It is okay to block since this
    // method should only be called from the aggregator thread.
    while (!safe_) _mm_pause();
    return std::shared_ptr<AbstractRawData>(old_data);
  }

 protected:
  /**
   * @see RawDataWrapper
   *
   * Always use this method to access the raw data within an AbstractMetric.
   * @return a RawDataWrapper object to access raw_data_
   */
  inline RawDataWrapper<DataType> GetRawData() {
    return {raw_data_.load(), safe_};
  }

 private:
  std::atomic<DataType *> raw_data_;
  bool safe_ = true;

};
}  // namespace stats
}  // namespace peloton
