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
        txn_(txn),
        seq_curr_val(seqval){};

  oid_t seq_oid;
  oid_t db_oid;
  std::string seq_name;
  int64_t seq_start;      // Start value of the sequence
  int64_t seq_increment;  // Increment value of the sequence
  int64_t seq_max;        // Maximum value of the sequence
  int64_t seq_min;        // Minimum value of the sequence
  int64_t seq_cache;      // Cache size of the sequence
  bool seq_cycle;         // Whether the sequence cycles
  concurrency::TransactionContext *txn_;

  int64_t seq_prev_val;

  int64_t GetNextVal();

  int64_t GetCurrVal() {
    return seq_prev_val;
  };

  void SetCurrVal(int64_t curr_val) {
    seq_curr_val = curr_val;
  };  // only visible for test!
  void SetCycle(bool cycle) { seq_cycle = cycle; };

 private:
  int64_t seq_curr_val;
};

class SequenceCatalog : public AbstractCatalog {
 public:
  SequenceCatalog(const std::string &database_name,
                  concurrency::TransactionContext *txn);
  ~SequenceCatalog();

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

  enum IndexId {
    PRIMARY_KEY = 0,
    DBOID_SEQNAME_KEY = 1
  };

 private:
  oid_t GetNextOid() { return oid_++ | SEQUENCE_OID_MASK; }

  void ValidateSequenceArguments(int64_t seq_increment, int64_t seq_max,
     int64_t seq_min, int64_t seq_start) {
    if (seq_min > seq_max) {
        throw SequenceException(
            StringUtil::Format(
              "MINVALUE (%d) must be less than MAXVALUE (%d)", seq_min, seq_max));
    }

    if (seq_increment == 0) {
        throw SequenceException(
            StringUtil::Format("INCREMENT must not be zero"));
    }

    if (seq_increment > 0 && seq_start < seq_min) {
        throw SequenceException(
            StringUtil::Format(
              "START value (%d) cannot be less than MINVALUE (%d)", seq_start, seq_min));
    }

    if (seq_increment < 0 && seq_start > seq_max) {
        throw SequenceException(
            StringUtil::Format(
              "START value (%d) cannot be greater than MAXVALUE (%d)", seq_start, seq_max));
    }
  };
};

}  // namespace catalog
}  // namespace peloton
