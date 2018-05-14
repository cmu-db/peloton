//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_metric.h
//
// Identification: src/include/statistics/abstract_metric.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <memory>
#include <string>

#include "common/internal_types.h"
#include "common/platform.h"
#include "statistics/abstract_raw_data.h"
#include "statistics/stats_event_type.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
} // namespace concurrency

namespace stats {
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

  // TODO(tianyu): Add more parameters for events as needed
  /**
   * @param Context of the transaction beginning
   */
  virtual void OnTransactionBegin(const concurrency::TransactionContext *) {};

  /**
   * @param Context of the transaction committing
   * @param Tile Group ID that used to track database where the txn happens.
   */
  virtual void OnTransactionCommit(const concurrency::TransactionContext *,
                                   oid_t) {};

  /**
   * @param Context of the transaction aborting
   * @param Tile Group ID that used to track database where the txn happens.
   */
  virtual void OnTransactionAbort(const concurrency::TransactionContext *,
                                  oid_t) {};

  /**
   * @param Context of the transaction performing read
   * @param Tile Group ID that used to track database and table
   *        where the read happens.
   */
  virtual void OnTupleRead(const concurrency::TransactionContext *, oid_t) {};

  /**
   * @param Context of the transaction performing update
   * @param Tile Group ID that used to track database and table
   *        where the update happens.
   */
  virtual void OnTupleUpdate(const concurrency::TransactionContext *, oid_t) {};

  /**
   * @param Context of the transaction performing insert
   * @param Tile Group ID that used to track database and table
   *        where the insert happens.
   */
  virtual void OnTupleInsert(const concurrency::TransactionContext *, oid_t) {};

  /**
   * @param Context of the transaction performing delete
   * @param Tile Group ID that used to track database and table
   *        where the delete happens.
  */
  virtual void OnTupleDelete(const concurrency::TransactionContext *, oid_t) {};

  /**
   * @param Database and index id pair that the index read happens
   * @param Number of read happening
   */
  virtual void OnIndexRead(std::pair<oid_t, oid_t>, size_t) {};

  /**
   * @param Database and index id pair that the index update happens
   */
  virtual void OnIndexUpdate(std::pair<oid_t, oid_t>) {};

  /**
   * @param Database and index id pair that the index insert happens
   */
  virtual void OnIndexInsert(std::pair<oid_t, oid_t>) {};

  /**
   * @param Database and index id pair that the index delete happens
   */
  virtual void OnIndexDelete(std::pair<oid_t, oid_t>) {};

  /**
   * @param Database and index/table id pair that the memory allocation happens
   * @param Number of bytes being allocated
   */
  virtual void OnMemoryAlloc(std::pair<oid_t, oid_t>, size_t) {};

  /**
   * @param Database and index/table id pair that the memory free happens
   * @param Number of bytes being freed
   */
  virtual void OnMemoryFree(std::pair<oid_t, oid_t>, size_t) {};

  /**
   * @param Database and index/table id pair that the memory usage happens
   * @param Number of bytes being used
   */
  virtual void OnMemoryUsage(std::pair<oid_t, oid_t>, size_t) {};

  /**
   * @param Database and index/table id pair that the memory reclaim happens
   * @param Number of bytes being reclaim
   */
  virtual void OnMemoryReclaim(std::pair<oid_t, oid_t>, size_t) {};

  /**
   * @brief collect the signal of query begin
   */
  virtual void OnQueryBegin() {};

  /**
   * @brief collect the signal of query end
   */
  virtual void OnQueryEnd() {};

  /**
   * @brief Event used to test the framework
   */
  virtual void OnTest(int) {};

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

  inline ~RawDataWrapper() { safe_ = true; }  // Unblock aggregator

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
  inline RawDataWrapper(DataType *ptr, std::atomic<bool> &safe)
      : ptr_(ptr), safe_(safe) {}
  DataType *ptr_;
  std::atomic<bool> &safe_;
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
  AbstractMetric() : raw_data_(new DataType()), safe_{true} {}

  ~AbstractMetric() { delete raw_data_.load(); }
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
    // safe_ should first be flipped to false before loading the raw_data_ so
    // that the aggregator would always be blocked when it tries to swap out if
    // there is a reader. At most one instance of this should be live at any
    // given time.
    PELOTON_ASSERT(safe_);
    safe_ = false;
    return {raw_data_.load(), safe_};
  }

 private:
  std::atomic<DataType *> raw_data_;
  std::atomic<bool> safe_;
};
}  // namespace stats
}  // namespace peloton
