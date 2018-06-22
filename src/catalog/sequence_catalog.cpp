//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sequence_catalog.cpp
//
// Identification: src/catalog/sequence_catalog.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/sequence_catalog.h"

#include <memory>

#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
#include "common/internal_types.h"
#include "executor/executor_context.h"
#include "executor/update_executor.h"
#include "function/functions.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"
#include "planner/update_plan.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

SequenceCatalog::SequenceCatalog(const std::string &database_name,
                                 concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " + database_name +
                          "." CATALOG_SCHEMA_NAME "." SEQUENCE_CATALOG_NAME
                          " ("
                          "oid            INT NOT NULL PRIMARY KEY, "
                          "sqdboid        INT NOT NULL, "
                          "sqnamespaceoid INT NOT NULL, "
                          "sqname         VARCHAR NOT NULL, "
                          "sqinc          BIGINT NOT NULL, "
                          "sqmax          BIGINT NOT NULL, "
                          "sqmin          BIGINT NOT NULL, "
                          "sqstart        BIGINT NOT NULL, "
                          "sqcycle        BOOLEAN NOT NULL, "
                          "sqval          BIGINT NOT NULL);",
                      txn) {
  Catalog::GetInstance()->CreateIndex(
      database_name, CATALOG_SCHEMA_NAME, SEQUENCE_CATALOG_NAME,
      {ColumnId::DATABASE_OID, ColumnId::SEQUENCE_NAME},
      SEQUENCE_CATALOG_NAME "_skey0", false, IndexType::BWTREE, txn);
}

SequenceCatalog::~SequenceCatalog() {}

bool SequenceCatalog::InsertSequence(concurrency::TransactionContext *txn,
                                     oid_t database_oid, oid_t namespace_oid,
                                     std::string sequence_name,
                                     int64_t seq_increment, int64_t seq_max,
                                     int64_t seq_min, int64_t seq_start,
                                     bool seq_cycle, type::AbstractPool *pool) {
  LOG_DEBUG("Insert Sequence Database Oid: %u", database_oid);
  LOG_DEBUG("Insert Sequence Sequence Name: %s", sequence_name.c_str());

  ValidateSequenceArguments(seq_increment, seq_max, seq_min, seq_start);
  if (GetSequence(txn, database_oid, namespace_oid, sequence_name) != nullptr) {
    throw CatalogException(StringUtil::Format("Sequence %s already exists!",
                                              sequence_name.c_str()));
  }

  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  tuple->SetValue(ColumnId::SEQUENCE_OID,
                  type::ValueFactory::GetIntegerValue(GetNextOid()), pool);
  tuple->SetValue(ColumnId::DATABASE_OID,
                  type::ValueFactory::GetIntegerValue(database_oid), pool);
  tuple->SetValue(ColumnId::NAMESPACE_OID,
                  type::ValueFactory::GetIntegerValue(namespace_oid), pool);
  tuple->SetValue(ColumnId::SEQUENCE_NAME,
                  type::ValueFactory::GetVarcharValue(sequence_name), pool);
  tuple->SetValue(ColumnId::SEQUENCE_INC,
                  type::ValueFactory::GetBigIntValue(seq_increment), pool);
  tuple->SetValue(ColumnId::SEQUENCE_MAX,
                  type::ValueFactory::GetBigIntValue(seq_max), pool);
  tuple->SetValue(ColumnId::SEQUENCE_MIN,
                  type::ValueFactory::GetBigIntValue(seq_min), pool);
  tuple->SetValue(ColumnId::SEQUENCE_START,
                  type::ValueFactory::GetBigIntValue(seq_start), pool);
  tuple->SetValue(ColumnId::SEQUENCE_CYCLE,
                  type::ValueFactory::GetBooleanValue(seq_cycle), pool);
  // When insert value, seqval = seq_start_
  tuple->SetValue(ColumnId::SEQUENCE_VALUE,
                  type::ValueFactory::GetBigIntValue(seq_start), pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

ResultType SequenceCatalog::DropSequence(concurrency::TransactionContext *txn,
                                         oid_t database_oid,
                                         oid_t namespace_oid,
                                         const std::string &sequence_name) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }

  oid_t sequence_oid =
      Catalog::GetInstance()
          ->GetSystemCatalogs(database_oid)
          ->GetSequenceCatalog()
          ->GetSequenceOid(txn, database_oid, namespace_oid, sequence_name);
  if (sequence_oid == INVALID_OID) {
    throw CatalogException(StringUtil::Format("Sequence %s does not exist!",
                                              sequence_name.c_str()));
  }

  LOG_INFO("sequence %d will be deleted!", sequence_oid);

  oid_t index_offset = IndexId::DATABASE_NAMESPACE_SEQNAME_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(namespace_oid).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(sequence_name).Copy());

  // TODO: Check return result
  DeleteWithIndexScan(index_offset, values, txn);

  return ResultType::SUCCESS;
}

std::shared_ptr<SequenceCatalogObject> SequenceCatalog::GetSequence(
    concurrency::TransactionContext *txn, oid_t database_oid,
    oid_t namespace_oid, const std::string &sequence_name) {
  // clang-format off
  std::vector<oid_t> column_ids(
      {ColumnId::SEQUENCE_OID,
       ColumnId::SEQUENCE_START,
       ColumnId::SEQUENCE_INC,
       ColumnId::SEQUENCE_MAX,
       ColumnId::SEQUENCE_MIN,
       ColumnId::SEQUENCE_CYCLE,
       ColumnId::SEQUENCE_VALUE}
  );
  // clang-format on

  oid_t index_offset = IndexId::DATABASE_NAMESPACE_SEQNAME_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(namespace_oid).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(sequence_name).Copy());

  // the result is a vector of executor::LogicalTile
  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);
  // careful! the result tile could be null!
  if (result_tiles == nullptr || result_tiles->size() == 0) {
    LOG_WARN("No sequence on database %d and %s", database_oid,
             sequence_name.c_str());
    return std::shared_ptr<SequenceCatalogObject>(nullptr);
  } else {
    LOG_INFO("size of the result tiles = %lu", result_tiles->size());
  }

  PELOTON_ASSERT(result_tiles->size() == 1);
  size_t tuple_count = (*result_tiles)[0]->GetTupleCount();
  PELOTON_ASSERT(tuple_count == 1);
  (void)tuple_count;
  auto new_sequence = std::make_shared<SequenceCatalogObject>(
      (*result_tiles)[0]->GetValue(0, 0).GetAs<oid_t>(), database_oid,
      namespace_oid, sequence_name,
      (*result_tiles)[0]->GetValue(0, 1).GetAs<int64_t>(),
      (*result_tiles)[0]->GetValue(0, 2).GetAs<int64_t>(),
      (*result_tiles)[0]->GetValue(0, 3).GetAs<int64_t>(),
      (*result_tiles)[0]->GetValue(0, 4).GetAs<int64_t>(),
      (*result_tiles)[0]->GetValue(0, 5).GetAs<bool>(),
      (*result_tiles)[0]->GetValue(0, 6).GetAs<int64_t>(), txn);

  return new_sequence;
}

bool SequenceCatalog::UpdateNextVal(concurrency::TransactionContext *txn,
                                    oid_t database_oid, oid_t namespace_oid,
                                    const std::string &sequence_name,
                                    int64_t nextval) {
  std::vector<oid_t> column_ids({ColumnId::DATABASE_OID,
                                 ColumnId::NAMESPACE_OID,
                                 ColumnId::SEQUENCE_NAME});
  oid_t index_offset = IndexId::DATABASE_NAMESPACE_SEQNAME_KEY;
  std::vector<type::Value> scan_values;
  scan_values.push_back(
      type::ValueFactory::GetIntegerValue(database_oid).Copy());
  scan_values.push_back(
      type::ValueFactory::GetIntegerValue(namespace_oid).Copy());
  scan_values.push_back(
      type::ValueFactory::GetVarcharValue(sequence_name).Copy());

  std::vector<oid_t> update_columns(
      {SequenceCatalog::ColumnId::SEQUENCE_VALUE});
  std::vector<type::Value> update_values;
  update_values.push_back(type::ValueFactory::GetBigIntValue(nextval).Copy());

  return UpdateWithIndexScan(update_columns, update_values, scan_values,
                             index_offset, txn);
}

int64_t SequenceCatalogObject::GetNextVal() {
  int64_t result = seq_curr_val_;
  seq_prev_val_ = result;
  if (seq_increment_ > 0) {
    // Check to see whether the nextval is out of bound
    if ((seq_max_ >= 0 && seq_curr_val_ > seq_max_ - seq_increment_) ||
        (seq_max_ < 0 && seq_curr_val_ + seq_increment_ > seq_max_)) {
      if (!seq_cycle_) {
        throw SequenceException(StringUtil::Format(
            "nextval: reached maximum value of sequence %s (%ld)",
            seq_name_.c_str(), seq_max_));
      }
      seq_curr_val_ = seq_min_;
    } else
      seq_curr_val_ += seq_increment_;
  } else {
    // Check to see whether the nextval is out of bound
    if ((seq_min_ < 0 && seq_curr_val_ < seq_min_ - seq_increment_) ||
        (seq_min_ >= 0 && seq_curr_val_ + seq_increment_ < seq_min_)) {
      if (!seq_cycle_) {
        throw SequenceException(StringUtil::Format(
            "nextval: reached minimum value of sequence %s (%ld)",
            seq_name_.c_str(), seq_min_));
      }
      seq_curr_val_ = seq_max_;
    } else
      seq_curr_val_ += seq_increment_;
  }

  // FIXME
  // If I am already inside of a SequenceCatalogObject, why
  // do I have to do another lookup to get myself? This seems like
  // are doing an unnecessary second look-up?
  Catalog::GetInstance()
      ->GetSystemCatalogs(db_oid_)
      ->GetSequenceCatalog()
      ->UpdateNextVal(txn_, db_oid_, namespace_oid_, seq_name_, seq_curr_val_);
  return result;
}

oid_t SequenceCatalog::GetSequenceOid(concurrency::TransactionContext *txn,
                                      oid_t database_oid, oid_t namespace_oid,
                                      const std::string &sequence_name) {
  std::vector<oid_t> column_ids({ColumnId::SEQUENCE_OID});
  oid_t index_offset = IndexId::DATABASE_NAMESPACE_SEQNAME_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(namespace_oid).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(sequence_name).Copy());

  // the result is a vector of executor::LogicalTile
  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);
  // careful! the result tile could be null!
  if (result_tiles == nullptr || result_tiles->size() == 0) {
    LOG_INFO("no sequence on database %d and %s", database_oid,
             sequence_name.c_str());
    return INVALID_OID;
  }

  PELOTON_ASSERT(result_tiles->size() == 1);
  return (*result_tiles)[0]->GetValue(0, 0).GetAs<oid_t>();
}

}  // namespace catalog
}  // namespace peloton
