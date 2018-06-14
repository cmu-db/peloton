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
// pg_trigger
//
// Schema: (column offset: column_name)
// 0: oid (pkey)
// 1: sqdboid   : database_oid
// 2: sqname    : sequence_name
// 3: sqinc     : seq_increment
// 4: sqmax     : seq_max
// 5: sqmin     : seq_min
// 6: sqstart   : seq_start
// 7: sqcycle   : seq_cycle
// 7: sqval     : seq_value
//
// Indexes: (index offset: indexed columns)
// 0: oid (primary key)
// 1: (sqdboid, sqname) (secondary key 0)
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

namespace peloton {

namespace concurrency {
class TransactionContext;
}

namespace catalog {

class SequenceCatalogObject {
 public:
  SequenceCatalogObject(oid_t seqoid, oid_t dboid, const std::string &name,
                        const int64_t seqstart, const int64_t seqincrement,
                        const int64_t seqmax, const int64_t seqmin,
                        const bool seqcycle, const int64_t seqval,
                        concurrency::TransactionContext *txn)
      : seq_oid(seqoid),
        db_oid(dboid),
        seq_name(name),
        seq_start(seqstart),
        seq_increment(seqincrement),
        seq_max(seqmax),
        seq_min(seqmin),
        seq_cycle(seqcycle),
        seq_curr_val(seqval),
        txn_(txn) {};


  oid_t GetSequenceOid() const {
    return (seq_oid);
  }

  oid_t GetDatabaseOid() const {
    return (db_oid);
  }

  std::string GetName() const {
    return (seq_name);
  }

  int64_t GetStart() const {
    return (seq_start);
  }

  int64_t GetIncrement() const {
    return (seq_increment);
  }

  int64_t GetMax() const {
    return (seq_max);
  }

  int64_t GetMin() const {
    return (seq_min);
  }

  int64_t GetCacheSize() const {
    return (seq_cache);
  }

  bool GetAllowCycle() const {
    return (seq_cycle);
  }

  /**
   * @brief   Get the nextval of the sequence
   * @exception throws SequenceException if the sequence exceeds the upper/lower limit
   * @return
   */
  int64_t GetNextVal();

  int64_t GetCurrVal() const {
    return seq_prev_val;
  }

 protected:

  void SetCycle(bool cycle) { seq_cycle = cycle; };

  void SetCurrVal(int64_t curr_val) {
    seq_curr_val = curr_val;
  };  // only visible for test!

 private:
  oid_t seq_oid;
  oid_t db_oid;
  std::string seq_name;
  int64_t seq_start;      // Start value of the sequence
  int64_t seq_increment;  // Increment value of the sequence
  int64_t seq_max;        // Maximum value of the sequence
  int64_t seq_min;        // Minimum value of the sequence
  int64_t seq_cache;      // Cache size of the sequence
  bool seq_cycle;         // Whether the sequence cycles
  int64_t seq_curr_val;

  concurrency::TransactionContext *txn_;

  int64_t seq_prev_val;
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
   * @param   database_oid  the databse_oid associated with the sequence
   * @param   sequence_name the name of the sequence
   * @param   seq_increment the increment per step of the sequence
   * @param   seq_max       the max value of the sequence
   * @param   seq_min       the min value of the sequence
   * @param   seq_start     the start of the sequence
   * @param   seq_cycle     whether the sequence cycles
   * @param   pool          an instance of abstract pool
   * @param   txn           current transaction
   * @return  ResultType::SUCCESS if the sequence exists, ResultType::FAILURE otherwise.
   * @exception throws SequenceException if the sequence already exists.
   */
  bool InsertSequence(oid_t database_oid, std::string sequence_name,
                      int64_t seq_increment, int64_t seq_max, int64_t seq_min,
                      int64_t seq_start, bool seq_cycle,
                      type::AbstractPool *pool,
                      concurrency::TransactionContext *txn);

  /**
   * @brief   Delete the sequence by name.
   * @param   database_name  the database name associated with the sequence
   * @param   sequence_name  the name of the sequence
   * @param   txn            current transaction
   * @return  ResultType::SUCCESS if the sequence exists, throw exception otherwise.
   */
  ResultType DropSequence(const std::string &database_name,
                          const std::string &sequence_name,
                          concurrency::TransactionContext *txn);

  /**
   * @brief   Delete the sequence by name. The sequence is guaranteed to exist.
   * @param   database_oid  the databse_oid associated with the sequence
   * @param   sequence_name the name of the sequence
   * @param   txn           current transaction
   * @return  The result of DeleteWithIndexScan.
   */
  bool DeleteSequenceByName(const std::string &sequence_name,
                            oid_t database_oid,
                            concurrency::TransactionContext *txn);

  /**
   * @brief   get sequence from pg_sequence table
   * @param   database_oid  the databse_oid associated with the sequence
   * @param   sequence_name the name of the sequence
   * @param   txn           current transaction
   * @return  a SequenceCatalogObject if the sequence is found, nullptr otherwise
   */
  std::shared_ptr<SequenceCatalogObject> GetSequence(
      oid_t database_oid, const std::string &sequence_name,
      concurrency::TransactionContext *txn);

  /**
   * @brief   get sequence oid from pg_sequence given sequence_name and database_oid
   * @param   database_oid  the databse_oid associated with the sequence
   * @param   sequence_name the name of the sequence
   * @param   txn           current transaction
   * @return  the oid_t of the sequence if the sequence is found, INVALID_OI otherwise
   */
  oid_t GetSequenceOid(std::string sequence_name, oid_t database_oid,
                       concurrency::TransactionContext *txn);

  /**
   * @brief   update the next value of the sequence in the underlying storage
   * @param   sequence_oid  the sequence_oid of the sequence
   * @param   nextval       the nextval of the sequence
   * @param   txn           current transaction
   * @return  the result of the transaction
   * @return
   */
  bool UpdateNextVal(oid_t sequence_oid, int64_t nextval,
    concurrency::TransactionContext *txn);

  enum ColumnId {
    SEQUENCE_OID = 0,
    DATABSE_OID = 1,
    SEQUENCE_NAME = 2,
    SEQUENCE_INC = 3,
    SEQUENCE_MAX = 4,
    SEQUENCE_MIN = 5,
    SEQUENCE_START = 6,
    SEQUENCE_CYCLE = 7,
    SEQUENCE_VALUE = 8
  };

  enum IndexId {
    PRIMARY_KEY = 0,
    DBOID_SEQNAME_KEY = 1
  };

  void InsertCurrValCache(std::string session_namespace_, std::string sequence_name, int64_t currval){
    std::tuple<std::string, std::string> key(session_namespace_, sequence_name);
    size_t hash_key = key_hash(key);
    sequence_currval_cache[hash_key] = currval;
    namespace_hash_lists[session_namespace_].push_back(hash_key);
    sequence_name_hash_lists[sequence_name].push_back(hash_key);
  }

  void EvictNamespaceCurrValCache(std::string session_namespace_){
    std::vector<size_t> hash_keys = namespace_hash_lists[session_namespace_];
    for (size_t hash_key : hash_keys){
      sequence_currval_cache.erase(hash_key);
    }
    namespace_hash_lists.erase(session_namespace_);
  }

  void EvictSequenceNameCurrValCache(std::string sequence_name){
    std::vector<size_t> hash_keys = sequence_name_hash_lists[sequence_name];
    for (size_t hash_key : hash_keys){
      sequence_currval_cache.erase(hash_key);
    }
    sequence_name_hash_lists.erase(sequence_name);
  }

  bool CheckCachedCurrValExistence(std::string session_namespace_, std::string sequence_name) {
    std::tuple<std::string, std::string> key(session_namespace_, sequence_name);
    size_t hash_key = key_hash(key);

    if (sequence_currval_cache.find(hash_key) != sequence_currval_cache.end())
      return true;

    return false;
  }

  int64_t GetCachedCurrVal(std::string session_namespace_, std::string sequence_name){
    std::tuple<std::string, std::string> key(session_namespace_, sequence_name);
    size_t hash_key = key_hash(key);

    return sequence_currval_cache.find(hash_key)->second;
  }

 private:
  oid_t GetNextOid() { return oid_++ | SEQUENCE_OID_MASK; }

  std::unordered_map<size_t, int64_t> sequence_currval_cache;
  std::unordered_map<std::string, std::vector<size_t>> namespace_hash_lists;
  std::unordered_map<std::string, std::vector<size_t>> sequence_name_hash_lists;
  boost::hash<std::tuple<std::string, std::string>> key_hash;

  void ValidateSequenceArguments(int64_t seq_increment, int64_t seq_max,
     int64_t seq_min, int64_t seq_start) {
    if (seq_min > seq_max) {
        throw SequenceException(
            StringUtil::Format(
              "MINVALUE (%ld) must be no greater than MAXVALUE (%ld)", seq_min, seq_max));
    }

    if (seq_increment == 0) {
        throw SequenceException(
            StringUtil::Format("INCREMENT must not be zero"));
    }

    if (seq_increment > 0 && seq_start < seq_min) {
        throw SequenceException(
            StringUtil::Format(
              "START value (%ld) cannot be less than MINVALUE (%ld)", seq_start, seq_min));
    }

    if (seq_increment < 0 && seq_start > seq_max) {
        throw SequenceException(
            StringUtil::Format(
              "START value (%ld) cannot be greater than MAXVALUE (%ld)", seq_start, seq_max));
    }
  };
};

}  // namespace catalog
}  // namespace peloton
