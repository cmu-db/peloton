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

#include <memory>

#include "catalog/sequence_catalog.h"

#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
#include "common/internal_types.h"
#include "storage/data_table.h"
#include "type/value_factory.h"
#include "function/functions.h"
#include "planner/update_plan.h"
#include "executor/update_executor.h"
#include "executor/executor_context.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"

namespace peloton {
namespace catalog {

int64_t SequenceCatalogObject::get_next_val() {
  int64_t result = seq_curr_val;
  if(seq_increment > 0) {
    if ((seq_max >= 0 && seq_curr_val > seq_max - seq_increment) ||
      (seq_max < 0 && seq_curr_val + seq_increment > seq_max)){
        if (!seq_cycle) {
          throw SequenceException(StringUtil::Format("Sequence exceeds upper limit!"));
        }
        seq_curr_val = seq_min;
      }
    else
      seq_curr_val += seq_increment;
  }
  else {
    if ((seq_min < 0 && seq_curr_val < seq_min - seq_increment) ||
      (seq_min >= 0 && seq_curr_val + seq_increment < seq_min))
    {
      if (!seq_cycle){
          throw SequenceException(StringUtil::Format("Sequence exceeds lower limit!"));
      }
      seq_curr_val = seq_max;
    }
    else
      seq_curr_val += seq_increment;
  }

  // auto &peloton_parser = parser::PostgresParser::GetInstance();
  // std::string update_statement = "UPDATE " + std::string(SEQUENCE_CATALOG_NAME) +
  //                                " SET sqval=" + std::to_string(seq_curr_val) +
  //                                " WHERE oid=" + std::to_string(seq_oid);
  // LOG_DEBUG("update sql statement:%s", update_statement.c_str());
  // auto update_plan = optimizer::Optimizer().BuildPelotonPlanTree(
  //           peloton_parser.BuildParseTree(update_statement),
  //           CATALOG_DATABASE_NAME, txn_).get();

  // std::unique_ptr<executor::ExecutorContext> context(
  //     new executor::ExecutorContext(txn_));
  // executor::UpdateExecutor executor(update_plan, context.get());
  // executor.Init();
  // bool status = executor.Execute();
  // LOG_DEBUG("status of update nextval:%d", status);
  return result;
}

SequenceCatalog &SequenceCatalog::GetInstance(concurrency::TransactionContext *txn) {
  static SequenceCatalog sequence_catalog{txn};
  return sequence_catalog;
}

SequenceCatalog::SequenceCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " SEQUENCE_CATALOG_NAME
                      " ("
                      "oid          INT NOT NULL PRIMARY KEY, "
                      "sqdboid      INT NOT NULL, "
                      "sqname       VARCHAR NOT NULL, "
                      "sqinc        BIGINT NOT NULL, "
                      "sqmax        BIGINT NOT NULL, "
                      "sqmin        BIGINT NOT NULL, "
                      "sqstart      BIGINT NOT NULL, "
                      "sqcycle      BOOLEAN NOT NULL, "
                      "sqval        BIGINT NOT NULL);",
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
  LOG_DEBUG("Insert Sequence Database Oid: %u", database_oid);
  LOG_DEBUG("Insert Sequence Sequence Name: %s", sequence_name.c_str());
  if(GetSequence(database_oid, sequence_name, txn) != nullptr) {
    throw SequenceException(StringUtil::Format("Insert Sequence with Duplicate Sequence Name: %s", sequence_name.c_str()));
  }

  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(GetNextOid());
  auto val1 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val2 = type::ValueFactory::GetVarcharValue(sequence_name);
  auto val3 = type::ValueFactory::GetBigIntValue(seq_increment);
  auto val4 = type::ValueFactory::GetBigIntValue(seq_max);
  auto val5 = type::ValueFactory::GetBigIntValue(seq_min);
  auto val6 = type::ValueFactory::GetBigIntValue(seq_start);
  auto val7 = type::ValueFactory::GetBooleanValue(seq_cycle);
  // When insert value, seqval = seq_start
  auto val8 = type::ValueFactory::GetBigIntValue(seq_start);

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

  LOG_INFO("sequence %d will be deleted!", sequence_oid);

  oid_t database_oid = database_object->GetDatabaseOid();
  DeleteSequenceByName(sequence_name, database_oid, txn);

  return ResultType::SUCCESS;
}

bool SequenceCatalog::DeleteSequenceByName(const std::string &sequence_name, oid_t database_oid,
                          concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::DBOID_SEQNAME_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(sequence_name).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::shared_ptr<SequenceCatalogObject> SequenceCatalog::GetSequence(
    oid_t database_oid, const std::string &sequence_name, concurrency::TransactionContext *txn){
  std::vector<oid_t> column_ids(
      {ColumnId::SEQUENCE_OID, ColumnId::SEQUENCE_NAME,
       ColumnId::SEQUENCE_START, ColumnId::SEQUENCE_INC,
       ColumnId::SEQUENCE_MAX, ColumnId::SEQUENCE_MIN,
       ColumnId::SEQUENCE_CYCLE, ColumnId::SEQUENCE_VALUE});
  oid_t index_offset = IndexId::DBOID_SEQNAME_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(sequence_name).Copy());

  // the result is a vector of executor::LogicalTile
  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);
  // carefull! the result tile could be null!
  if (result_tiles == nullptr || result_tiles->size() == 0) {
    LOG_INFO("no sequence on database %d and %s", database_oid, sequence_name.c_str());
    return std::shared_ptr<SequenceCatalogObject>(nullptr);
  } else {
    LOG_INFO("size of the result tiles = %lu", result_tiles->size());
  }

  PELOTON_ASSERT(result_tiles->size() == 1);
  size_t tuple_count = (*result_tiles)[0]->GetTupleCount();
  PELOTON_ASSERT(tuple_count == 1);
  auto new_sequence =
    std::make_shared<SequenceCatalogObject>(
      (*result_tiles)[0]->GetValue(0, 0).GetAs<oid_t>(),
      (*result_tiles)[0]->GetValue(0, 1).ToString(),
      (*result_tiles)[0]->GetValue(0, 2).GetAs<int64_t>(),
      (*result_tiles)[0]->GetValue(0, 3).GetAs<int64_t>(),
      (*result_tiles)[0]->GetValue(0, 4).GetAs<int64_t>(),
      (*result_tiles)[0]->GetValue(0, 5).GetAs<int64_t>(),
      (*result_tiles)[0]->GetValue(0, 6).GetAs<bool>(),
      (*result_tiles)[0]->GetValue(0, 7).GetAs<int64_t>(), txn);

  return new_sequence;
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
  if (result_tiles == nullptr || result_tiles->size() == 0) {
    LOG_INFO("no sequence on database %d and %s", database_oid, sequence_name.c_str());
    return INVALID_OID;
  }

  PELOTON_ASSERT(result_tiles->size() == 1);
  oid_t result;
  result = (*result_tiles)[0]->GetValue(0, 0).GetAs<oid_t>();

  return result;
}

}  // namespace catalog
}  // namespace peloton
