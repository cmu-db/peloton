//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_metrics_catalog.cpp
//
// Identification: src/catalog/query_metrics_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/query_metrics_catalog.h"

#include "catalog/catalog.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

QueryMetricsCatalog::QueryMetricsCatalog(concurrency::TransactionContext *txn,
                                         const std::string &database_name)
    : AbstractCatalog(txn, "CREATE TABLE " + database_name +
    "." CATALOG_SCHEMA_NAME "." QUERY_METRICS_CATALOG_NAME
    " ("
    "query_name   VARCHAR NOT NULL PRIMARY KEY, "
    "database_oid INT NOT NULL PRIMARY KEY, "
    "num_params   INT NOT NULL, "
    "param_types    VARBINARY, "
    "param_formats  VARBINARY, "
    "param_values   VARBINARY, "
    "reads    INT NOT NULL, "
    "updates  INT NOT NULL, "
    "deletes  INT NOT NULL, "
    "inserts  INT NOT NULL, "
    "latency  INT NOT NULL, "
    "cpu_time INT NOT NULL, "
    "time_stamp INT NOT NULL);") {
  // Add secondary index here if necessary
}

QueryMetricsCatalog::~QueryMetricsCatalog() {}

bool QueryMetricsCatalog::InsertQueryMetrics(concurrency::TransactionContext *txn,
                                             const std::string &name,
                                             oid_t database_oid,
                                             int64_t num_params,
                                             const stats::QueryMetric::QueryParamBuf &type_buf,
                                             const stats::QueryMetric::QueryParamBuf &format_buf,
                                             const stats::QueryMetric::QueryParamBuf &value_buf,
                                             int64_t reads,
                                             int64_t updates,
                                             int64_t deletes,
                                             int64_t inserts,
                                             int64_t latency,
                                             int64_t cpu_time,
                                             int64_t time_stamp,
                                             type::AbstractPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetVarcharValue(name, pool);
  auto val1 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val2 = type::ValueFactory::GetIntegerValue(num_params);

  auto val3 = type::ValueFactory::GetNullValueByType(type::TypeId::VARBINARY);
  auto val4 = type::ValueFactory::GetNullValueByType(type::TypeId::VARBINARY);
  auto val5 = type::ValueFactory::GetNullValueByType(type::TypeId::VARBINARY);

  if (num_params != 0) {
    val3 =
        type::ValueFactory::GetVarbinaryValue(type_buf.buf, type_buf.len, true);
    val4 = type::ValueFactory::GetVarbinaryValue(format_buf.buf, format_buf.len,
                                                 true);
    val5 = type::ValueFactory::GetVarbinaryValue(value_buf.buf, value_buf.len,
                                                 true);
  }

  auto val6 = type::ValueFactory::GetIntegerValue(reads);
  auto val7 = type::ValueFactory::GetIntegerValue(updates);
  auto val8 = type::ValueFactory::GetIntegerValue(deletes);
  auto val9 = type::ValueFactory::GetIntegerValue(inserts);
  auto val10 = type::ValueFactory::GetIntegerValue(latency);
  auto val11 = type::ValueFactory::GetIntegerValue(cpu_time);
  auto val12 = type::ValueFactory::GetIntegerValue(time_stamp);

  tuple->SetValue(ColumnId::NAME, val0, pool);
  tuple->SetValue(ColumnId::DATABASE_OID, val1, pool);
  tuple->SetValue(ColumnId::NUM_PARAMS, val2, pool);
  tuple->SetValue(ColumnId::PARAM_TYPES, val3, pool);
  tuple->SetValue(ColumnId::PARAM_FORMATS, val4, pool);
  tuple->SetValue(ColumnId::PARAM_VALUES, val5, pool);
  tuple->SetValue(ColumnId::READS, val6, pool);
  tuple->SetValue(ColumnId::UPDATES, val7, pool);
  tuple->SetValue(ColumnId::DELETES, val8, pool);
  tuple->SetValue(ColumnId::INSERTS, val9, pool);
  tuple->SetValue(ColumnId::LATENCY, val10, pool);
  tuple->SetValue(ColumnId::CPU_TIME, val11, pool);
  tuple->SetValue(ColumnId::TIME_STAMP, val12, pool);

  // Insert the tuple
  return InsertTuple(txn, std::move(tuple));
}

bool QueryMetricsCatalog::DeleteQueryMetrics(concurrency::TransactionContext *txn,
                                             const std::string &name) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Primary key index

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid_).Copy());

  return DeleteWithIndexScan(txn, index_offset, values);
}

stats::QueryMetric::QueryParamBuf QueryMetricsCatalog::GetParamTypes(concurrency::TransactionContext *txn,
                                                                     const std::string &name) {
  std::vector<oid_t> column_ids({ColumnId::PARAM_TYPES});  // param_types
  oid_t index_offset = IndexId::PRIMARY_KEY;               // Primary key index
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid_).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);

  stats::QueryMetric::QueryParamBuf param_types;
  PELOTON_ASSERT(result_tiles->size() <= 1);  // unique
  if (result_tiles->size() != 0) {
    PELOTON_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      auto param_types_value = (*result_tiles)[0]->GetValue(0, 0);
      param_types.buf = const_cast<uchar *>(
          reinterpret_cast<const uchar *>(param_types_value.GetData()));
      param_types.len = param_types_value.GetLength();
    }
  }

  return param_types;
}

int64_t QueryMetricsCatalog::GetNumParams(concurrency::TransactionContext *txn,
                                          const std::string &name) {
  std::vector<oid_t> column_ids({ColumnId::NUM_PARAMS});  // num_params
  oid_t index_offset = IndexId::PRIMARY_KEY;              // Primary key index
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);

  int64_t num_params = 0;
  PELOTON_ASSERT(result_tiles->size() <= 1);  // unique
  if (result_tiles->size() != 0) {
    PELOTON_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      num_params = (*result_tiles)[0]
                       ->GetValue(0, 0)
                       .GetAs<int>();  // After projection left 1 column
    }
  }

  return num_params;
}

}  // namespace catalog
}  // namespace peloton
