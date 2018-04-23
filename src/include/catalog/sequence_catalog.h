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

#include "catalog/abstract_catalog.h"
#include "catalog/catalog_defaults.h"

#define SEQUENCE_CATALOG_NAME "pg_sequence"

namespace peloton {

namespace concurrency {
class TransactionContext;
}

namespace catalog {

class SequenceCatalogObject {
 public:
  SequenceCatalogObject(oid_t seqoid, const std::string &name,
                        const int64_t seqstart, const int64_t seqincrement,
                        const int64_t seqmax, const int64_t seqmin,
                        const bool seqcycle, const int64_t seqval,
                        concurrency::TransactionContext *txn)
      : seq_oid(seqoid),
        seq_name(name),
        seq_start(seqstart),
        seq_increment(seqincrement),
        seq_max(seqmax),
        seq_min(seqmin),
        seq_cycle(seqcycle),
        txn_(txn),
        seq_curr_val(seqval){};

  oid_t seq_oid;
  std::string seq_name;
  int64_t seq_start;      // Start value of the sequence
  int64_t seq_increment;  // Increment value of the sequence
  int64_t seq_max;        // Maximum value of the sequence
  int64_t seq_min;        // Minimum value of the sequence
  int64_t seq_cache;      // Cache size of the sequence
  bool seq_cycle;         // Whether the sequence cycles
  concurrency::TransactionContext *txn_;

  std::mutex sequence_mutex;  // mutex for all operations
  int64_t GetNextVal() {
    return get_next_val();
  };

  int64_t GetCurrVal() {
    return seq_curr_val;
  };

  void SetCurrVal(int64_t curr_val) {
    seq_curr_val = curr_val;
  };  // only visible for test!
  void SetCycle(bool cycle) { seq_cycle = cycle; };

 private:
  int64_t seq_curr_val;
  int64_t get_next_val();
};

class SequenceCatalog : public AbstractCatalog {
 public:
  ~SequenceCatalog();

  // Global Singleton
  static SequenceCatalog &GetInstance(
      concurrency::TransactionContext *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertSequence(oid_t database_oid, std::string sequence_name,
                      int64_t seq_increment, int64_t seq_max, int64_t seq_min,
                      int64_t seq_start, bool seq_cycle,
                      type::AbstractPool *pool,
                      concurrency::TransactionContext *txn);

  ResultType DropSequence(const std::string &database_name,
                          const std::string &sequence_name,
                          concurrency::TransactionContext *txn);

  bool DeleteSequenceByName(const std::string &sequence_name,
                            oid_t database_oid,
                            concurrency::TransactionContext *txn);

  std::shared_ptr<SequenceCatalogObject> GetSequence(
      oid_t database_oid, const std::string &sequence_name,
      concurrency::TransactionContext *txn);

  oid_t GetSequenceOid(std::string sequence_name, oid_t database_oid,
                       concurrency::TransactionContext *txn);

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

  enum IndexId { PRIMARY_KEY = 0, DBOID_SEQNAME_KEY = 1 };

 private:
  SequenceCatalog(concurrency::TransactionContext *txn);

  oid_t GetNextOid() { return oid_++ | SEQUENCE_OID_MASK; }
};

}  // namespace catalog
}  // namespace peloton
