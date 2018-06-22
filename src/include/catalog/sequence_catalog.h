//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sequence_catalog.h
//
// Identification: src/include/catalog/sequence_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_sequence
//
// Schema: (column offset: column_name)
// 0: oid (pkey)
// 1: sqdboid     : database_oid
// 2: sqnamespace : namespace_oid
// 3: sqname      : sequence_name
// 4: sqinc       : seq_increment
// 5: sqmax       : seq_max
// 6: sqmin       : seq_min
// 7: sqstart     : seq_start_
// 8: sqcycle     : seq_cycle
// 9: sqval       : seq_value
//
// Indexes: (index offset: indexed columns)
// 0: oid (primary key)
// 1: (sqdboid, sqnamespace, sqname) (secondary key 0)
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <mutex>
#include <boost/functional/hash.hpp>

#include "catalog/abstract_catalog.h"
#include "catalog/catalog_defaults.h"
#include "catalog/system_catalogs.h"
#include "common/internal_types.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
}

namespace catalog {

class SequenceCatalogObject {
 public:
  SequenceCatalogObject(oid_t seq_oid, oid_t db_oid, oid_t namespace_oid,
                        const std::string &name,
                        const int64_t seqstart, const int64_t seqincrement,
                        const int64_t seqmax, const int64_t seqmin,
                        const bool seqcycle, const int64_t seqval,
                        concurrency::TransactionContext *txn)
      : seq_oid_(seq_oid),
        db_oid_(db_oid),
        namespace_oid_(namespace_oid),
        seq_name_(name),
        seq_start_(seqstart),
        seq_increment_(seqincrement),
        seq_max_(seqmax),
        seq_min_(seqmin),
        seq_cycle_(seqcycle),
        seq_curr_val_(seqval),
        txn_(txn) {};


  oid_t GetSequenceOid() const {
    return (seq_oid_);
  }

  oid_t GetDatabaseOid() const {
    return (db_oid_);
  }

  oid_t GetNamespaceOid() const {
    return (namespace_oid_);
  }

  std::string GetName() const {
    return (seq_name_);
  }

  int64_t GetStart() const {
    return (seq_start_);
  }

  int64_t GetIncrement() const {
    return (seq_increment_);
  }

  int64_t GetMax() const {
    return (seq_max_);
  }

  int64_t GetMin() const {
    return (seq_min_);
  }

  int64_t GetCacheSize() const {
    return (seq_cache_);
  }

  bool GetAllowCycle() const {
    return (seq_cycle_);
  }

  /**
   * @brief   Get the nextval of the sequence
   * @exception throws SequenceException if the sequence exceeds the upper/lower limit
   * @return
   */
  int64_t GetNextVal();

  int64_t GetCurrVal() const {
    return seq_prev_val_;
  }

 protected:

  void SetCycle(bool cycle) { seq_cycle_ = cycle; };

  void SetCurrVal(int64_t curr_val) {
    seq_curr_val_ = curr_val;
  };  // only visible for test!

 private:
  oid_t seq_oid_;
  oid_t db_oid_;
  oid_t namespace_oid_;
  std::string seq_name_;
  int64_t seq_start_;      // Start value of the sequence
  int64_t seq_increment_;  // Increment value of the sequence
  int64_t seq_max_;        // Maximum value of the sequence
  int64_t seq_min_;        // Minimum value of the sequence
  int64_t seq_cache_;      // Cache size of the sequence
  bool seq_cycle_;         // Whether the sequence cycles
  int64_t seq_curr_val_;
  int64_t seq_prev_val_;

  concurrency::TransactionContext *txn_;
};

class SequenceCatalog : public AbstractCatalog {
 public:
  SequenceCatalog(const std::string &database_name,
                  concurrency::TransactionContext *txn);
  ~SequenceCatalog();

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//

  /**
   * @brief   Insert the sequence by name.
   * @param   txn           current transaction
   * @param   database_oid  the database_oid for the sequence
   * @param   namespace_oid the namespace oid for the sequence
   * @param   sequence_name the name of the sequence
   * @param   seq_increment the increment per step of the sequence
   * @param   seq_max       the max value of the sequence
   * @param   seq_min       the min value of the sequence
   * @param   seq_start     the start of the sequence
   * @param   seq_cycle     whether the sequence cycles
   * @param   pool          an instance of abstract pool
   * @return  ResultType::SUCCESS if the sequence exists, ResultType::FAILURE otherwise.
   * @exception throws SequenceException if the sequence already exists.
   */
  bool InsertSequence(
      concurrency::TransactionContext *txn,
      oid_t database_oid,
      oid_t namespace_oid,
      std::string sequence_name,
      int64_t seq_increment, int64_t seq_max, int64_t seq_min,
      int64_t seq_start, bool seq_cycle,
      type::AbstractPool *pool);

  /**
   * @brief   Delete the sequence by name.
   * @param   txn            current transaction
   * @param   database_oid  the database_oid for the sequence
   * @param   namespace_oid the namespace oid for the sequence
   * @param   sequence_name  the name of the sequence
   * @return  ResultType::SUCCESS if the sequence exists, throw exception otherwise.
   */
  ResultType DropSequence(
      concurrency::TransactionContext *txn,
      oid_t database_oid,
      oid_t namespace_oid,
      const std::string &sequence_name);

  /**
   * @brief   get sequence from pg_sequence table
   * @param   txn           current transaction
   * @param   database_oid  the database_oid for the sequence
   * @param   namespace_oid the namespace oid for the sequence
   * @param   sequence_name the name of the sequence
   * @return  a SequenceCatalogObject if the sequence is found, nullptr otherwise
   */
  std::shared_ptr<SequenceCatalogObject> GetSequence(
      concurrency::TransactionContext *txn,
      oid_t database_oid,
      oid_t namespace_oid,
      const std::string &sequence_name);

  /**
   * @brief   get sequence oid from pg_sequence given sequence_name and database_oid
   * @param   txn           current transaction
   * @param   database_oid  the database_oid for the sequence
   * @param   namespace_oid the namespace oid for the sequence
   * @param   sequence_name the name of the sequence
   * @return  the oid_t of the sequence if the sequence is found, INVALID_OI otherwise
   */
  oid_t GetSequenceOid(
      concurrency::TransactionContext *txn,
      oid_t database_oid,
      oid_t namespace_oid,
      const std::string &sequence_name);

  /**
   * @brief   update the next value of the sequence in the underlying storage
   * @param   txn           current transaction
   * @param   database_oid  the database_oid for the sequence
   * @param   namespace_oid the namespace oid for the sequence
   * @param   sequence_oid  the sequence_oid of the sequence
   * @param   nextval       the nextval of the sequence
   * @return  the result of the transaction
   * @return
   */
  bool UpdateNextVal(
      concurrency::TransactionContext *txn,
      oid_t database_oid,
      oid_t namespace_oid,
      const std::string &sequence_name,
      int64_t nextval);

  enum ColumnId {
    SEQUENCE_OID = 0,
    DATABASE_OID = 1,
    NAMESPACE_OID = 2,
    SEQUENCE_NAME = 3,
    SEQUENCE_INC = 4,
    SEQUENCE_MAX = 5,
    SEQUENCE_MIN = 6,
    SEQUENCE_START = 7,
    SEQUENCE_CYCLE = 8,
    SEQUENCE_VALUE = 9
  };

  enum IndexId {
    PRIMARY_KEY = 0,
    DATABASE_NAMESPACE_SEQNAME_KEY = 1
  };

 private:
  oid_t GetNextOid() { return oid_++ | SEQUENCE_OID_MASK; }

  void ValidateSequenceArguments(
      int64_t seq_increment,
      int64_t seq_max,
      int64_t seq_min,
      int64_t seq_start) {

    if (seq_min > seq_max) {
        throw SequenceException(
            StringUtil::Format(
              "MINVALUE (%ld) must be no greater than MAXVALUE (%ld)",
              seq_min, seq_max));
    }

    if (seq_increment == 0) {
        throw SequenceException(
            StringUtil::Format("INCREMENT must not be zero"));
    }

    if (seq_increment > 0 && seq_start < seq_min) {
        throw SequenceException(
            StringUtil::Format(
              "START value (%ld) cannot be less than MINVALUE (%ld)",
              seq_start, seq_min));
    }

    if (seq_increment < 0 && seq_start > seq_max) {
        throw SequenceException(
            StringUtil::Format(
              "START value (%ld) cannot be greater than MAXVALUE (%ld)",
              seq_start, seq_max));
    }
  };
};

}  // namespace catalog
}  // namespace peloton
