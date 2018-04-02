//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sequence_catalog.h
//
// Identification: src/catalog/sequence_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/sequence_catalog.h"

#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
#include "common/internal_types.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

SequenceCatalog &SequenceCatalog::GetInstance(concurrency::TransactionContext *txn) {
  static SequenceCatalog sequence_catalog{txn};
  return sequence_catalog;
}

SequenceCatalog::SequenceCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." SEQUENCE_CATALOG_NAME
                      " ("
                      "oid          INT NOT NULL PRIMARY KEY, "
                      "sqdboid      INT NOT NULL, "
                      "sqname       VARCHAR NOT NULL, "
                      "sqinc        INT NOT NULL, "
                      "sqmax        INT NOT NULL, "
                      "sqmin        INT NOT NULL, "
                      "sqstart      INT NOT NULL, "
                      "sqcycle      BOOLEAN NOT NULL, "
                      "sqval        INT NOT NULL);",
                      txn) {
  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, SEQUENCE_CATALOG_NAME,
      {ColumnId::DATABSE_OID, ColumnId::SEQUENCE_NAME},
      SEQUENCE_CATALOG_NAME "_skey0", false, IndexType::BWTREE, txn);
}

SequenceCatalog::~SequenceCatalog() {}

bool SequenceCatalog::InsertSequence(oid_t database_oid, std::string sequence_name,
                    int64_t seq_increment, int64_t seq_max, int64_t seq_min,
                    int64_t seq_start, bool seq_cycle,
                    type::AbstractPool *pool,
                    concurrency::TransactionContext *txn){
  LOG_DEBUG("In Insert Sequence Mode");
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(GetNextOid());
  auto val1 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val2 = type::ValueFactory::GetVarcharValue(sequence_name);
  auto val3 = type::ValueFactory::GetIntegerValue(seq_increment);
  auto val4 = type::ValueFactory::GetIntegerValue(seq_max);
  auto val5 = type::ValueFactory::GetIntegerValue(seq_min);
  auto val6 = type::ValueFactory::GetIntegerValue(seq_start);
  auto val7 = type::ValueFactory::GetBooleanValue(seq_cycle);
  // When insert value, seqval = seq_start
  auto val8 = type::ValueFactory::GetIntegerValue(seq_start);

  tuple->SetValue(ColumnId::SEQUENCE_OID, val0, pool);
  tuple->SetValue(ColumnId::DATABSE_OID, val1, pool);
  tuple->SetValue(ColumnId::SEQUENCE_NAME, val2, pool);
  tuple->SetValue(ColumnId::SEQUENCE_INC, val3, pool);
  tuple->SetValue(ColumnId::SEQUENCE_MAX, val4, pool);
  tuple->SetValue(ColumnId::SEQUENCE_MIN, val5, pool);
  tuple->SetValue(ColumnId::SEQUENCE_START, val6, pool);
  tuple->SetValue(ColumnId::SEQUENCE_CYCLE, val7, pool);
  tuple->SetValue(ColumnId::SEQUENCE_VALUE, val8, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

ResultType SequenceCatalog::DropSequence(const std::string &database_name,
                        const std::string &sequence_name,
                        concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to drop sequence: %s",
              database_name.c_str());
    return ResultType::FAILURE;
  }

  auto database_object =
      Catalog::GetInstance()->GetDatabaseObject(database_name, txn);

  oid_t sequence_oid = SequenceCatalog::GetInstance().GetSequenceOid(
      sequence_name, database_object->GetDatabaseOid(), txn);
  if (sequence_oid == INVALID_OID) {
    LOG_TRACE("Cannot find sequence %s to drop!", sequence_name.c_str());
    return ResultType::FAILURE;
  }

  LOG_INFO("trigger %d will be deleted!", sequence_oid);

  bool delete_success =
      DeleteSequenceByName(sequence_name, database_object->GetDatabaseOid(), txn);
  if (delete_success) {
    // might need to do sth. else after implementing insert
    return ResultType::SUCCESS;
  }

  LOG_DEBUG("Failed to delete sequence");
  return ResultType::FAILURE;
}

bool SequenceCatalog::DeleteSequenceByName(const std::string &sequence_name, oid_t database_oid,
                          concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::DBOID_SEQNAME_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(sequence_name).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::unique_ptr<sequence::Sequence> SequenceCatalog::GetSequence(
    oid_t database_oid, const std::string &sequence_name, concurrency::TransactionContext *txn){
  std::vector<oid_t> column_ids(
      {ColumnId::SEQUENCE_NAME,ColumnId::SEQUENCE_START,
       ColumnId::SEQUENCE_INC, ColumnId::SEQUENCE_MAX,
       ColumnId::SEQUENCE_MIN, ColumnId::SEQUENCE_CYCLE,
       ColumnId::SEQUENCE_VALUE});
  oid_t index_offset = IndexId::DBOID_SEQNAME_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(sequence_name).Copy());

  // the result is a vector of executor::LogicalTile
  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);
  // carefull! the result tile could be null!
  if (result_tiles == nullptr) {
    LOG_INFO("no sequence on database %d and %s", database_oid, sequence_name.c_str());
    return std::unique_ptr<sequence::Sequence>(nullptr);
  } else {
    LOG_INFO("size of the result tiles = %lu", result_tiles->size());
  }

  PL_ASSERT(result_tiles->size() == 1);
  for (unsigned int i = 0; i < result_tiles->size(); i++) {
    size_t tuple_count = (*result_tiles)[i]->GetTupleCount();
    for (size_t j = 0; j < tuple_count; j++) {
      // create a new sequence instance
      std::unique_ptr<sequence::Sequence> new_sequence =
       std::make_unique<sequence::Sequence>(
          (*result_tiles)[i]->GetValue(j, 0).ToString(),
          (*result_tiles)[i]->GetValue(j, 1).GetAs<int64_t>(),
          (*result_tiles)[i]->GetValue(j, 2).GetAs<int64_t>(),
          (*result_tiles)[i]->GetValue(j, 3).GetAs<int64_t>(),
          (*result_tiles)[i]->GetValue(j, 4).GetAs<int64_t>(),
          (*result_tiles)[i]->GetValue(j, 5).GetAs<bool>(),
          (*result_tiles)[i]->GetValue(j, 6).GetAs<int64_t>());

      return new_sequence;
    }
  }

  return std::unique_ptr<sequence::Sequence>(nullptr);
}

oid_t SequenceCatalog::GetSequenceOid(std::string sequence_name, oid_t database_oid,
                    concurrency::TransactionContext *txn) {
  std::vector<oid_t> column_ids(
      {ColumnId::SEQUENCE_OID});
  oid_t index_offset = IndexId::DBOID_SEQNAME_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(sequence_name).Copy());

  // the result is a vector of executor::LogicalTile
  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);
  // carefull! the result tile could be null!
  if (result_tiles == nullptr) {
    LOG_INFO("no trigger on sequence %d and %s", database_oid, sequence_name.c_str());
    return INVALID_OID;
  }

  PL_ASSERT(result_tiles->size() == 1);
  oid_t result;
  for (unsigned int i = 0; i < result_tiles->size(); i++) {
    result = (*result_tiles)[i]->GetValue(0, 0).GetAs<uint32_t>();
  }

  return result;
}

}  // namespace catalog
}  // namespace peloton
