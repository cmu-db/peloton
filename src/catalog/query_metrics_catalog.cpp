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
#include "catalog/catalog_defaults.h"
#include "common/macros.h"

namespace peloton {
namespace catalog {

QueryMetricsCatalog *QueryMetricsCatalog::GetInstance(
    concurrency::Transaction *txn) {
  static std::unique_ptr<QueryMetricsCatalog> query_metrics_catalog(
      new QueryMetricsCatalog(txn));

  return query_metrics_catalog.get();
}

QueryMetricsCatalog::QueryMetricsCatalog(concurrency::Transaction *txn)
    : AbstractCatalog(QUERY_METRICS_CATALOG_NAME,
                      "CREATE TABLE " CATALOG_DATABASE_NAME
                      "." QUERY_METRICS_CATALOG_NAME
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
                      "cpu_time INT NOT NULL);",
                      txn) {
  // Add secondary index here if necessary
}

QueryMetricsCatalog::~QueryMetricsCatalog() {}

/*@brief   private function for initialize schema of pg_query_metrics
* @return  unqiue pointer to schema
*/
std::unique_ptr<catalog::Schema> QueryMetricsCatalog::InitializeSchema() {
  // convenience variables
  catalog::Constraint not_null_constraint(ConstraintType::NOTNULL, "not_null");
  auto integer_type_size = type::Type::GetTypeSize(type::Type::INTEGER);
  auto varchar_type_size = type::Type::GetTypeSize(type::Type::VARCHAR);
  auto varbinary_type_size = type::Type::GetTypeSize(type::Type::VARBINARY);

  // Add columns
  // Primary keys
  auto name_column = catalog::Column(type::Type::VARCHAR, varchar_type_size,
                                     "query_name", false);
  name_column.AddConstraint(
      catalog::Constraint(ConstraintType::PRIMARY, "primary_key"));
  name_column.AddConstraint(not_null_constraint);
  auto database_oid_column = catalog::Column(
      type::Type::INTEGER, integer_type_size, "database_oid", true);
  database_oid_column.AddConstraint(
      catalog::Constraint(ConstraintType::PRIMARY, "primary_key"));
  database_oid_column.AddConstraint(not_null_constraint);

  // Parameters
  auto num_params_column = catalog::Column(
      type::Type::INTEGER, integer_type_size, "num_params", true);
  num_params_column.AddConstraint(not_null_constraint);
  // For varbinary types, we don't want to inline it since it could be large
  auto param_types_column = catalog::Column(
      type::Type::VARBINARY, varbinary_type_size, "param_types", false);
  auto param_formats_column = catalog::Column(
      type::Type::VARBINARY, varbinary_type_size, "param_formats", false);
  auto param_values_column = catalog::Column(
      type::Type::VARBINARY, varbinary_type_size, "param_values", false);

  // Physical statistics
  auto reads_column =
      catalog::Column(type::Type::INTEGER, integer_type_size, "reads", true);
  reads_column.AddConstraint(not_null_constraint);
  auto updates_column =
      catalog::Column(type::Type::INTEGER, integer_type_size, "updates", true);
  updates_column.AddConstraint(not_null_constraint);
  auto deletes_column =
      catalog::Column(type::Type::INTEGER, integer_type_size, "deletes", true);
  deletes_column.AddConstraint(not_null_constraint);
  auto inserts_column =
      catalog::Column(type::Type::INTEGER, integer_type_size, "inserts", true);
  inserts_column.AddConstraint(not_null_constraint);
  auto latency_column =
      catalog::Column(type::Type::INTEGER, integer_type_size, "latency", true);
  latency_column.AddConstraint(not_null_constraint);
  auto cpu_time_column =
      catalog::Column(type::Type::INTEGER, integer_type_size, "cpu_time", true);

  // MAX_INT only tracks the number of seconds since epoch until 2037
  auto timestamp_column = catalog::Column(
      type::Type::INTEGER, integer_type_size, "time_stamp", true);
  timestamp_column.AddConstraint(not_null_constraint);

  std::unique_ptr<catalog::Schema> query_metrics_catalog_schema(
      new catalog::Schema({name_column, database_oid_column, num_params_column,
                           param_types_column, param_formats_column,
                           param_values_column, reads_column, updates_column,
                           deletes_column, inserts_column, latency_column,
                           cpu_time_column, timestamp_column}));
  return query_metrics_catalog_schema;
}

bool QueryMetricsCatalog::InsertQueryMetrics(
    const std::string &name, oid_t database_oid, int64_t num_params,
    const stats::QueryMetric::QueryParamBuf &type_buf,
    const stats::QueryMetric::QueryParamBuf &format_buf,
    const stats::QueryMetric::QueryParamBuf &value_buf, int64_t reads,
    int64_t updates, int64_t deletes, int64_t inserts, int64_t latency,
    int64_t cpu_time, int64_t time_stamp, type::AbstractPool *pool,
    concurrency::Transaction *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetVarcharValue(name, nullptr);
  auto val1 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val2 = type::ValueFactory::GetIntegerValue(num_params);

  auto val3 = type::ValueFactory::GetNullValueByType(type::Type::VARBINARY);
  auto val4 = type::ValueFactory::GetNullValueByType(type::Type::VARBINARY);
  auto val5 = type::ValueFactory::GetNullValueByType(type::Type::VARBINARY);

  if (num_params != 0) {
    val3 = type::ValueFactory::GetVarbinaryValue(type_buf.buf, type_buf.len,
                                                 false);
    val4 = type::ValueFactory::GetVarbinaryValue(format_buf.buf, format_buf.len,
                                                 false);
    val5 = type::ValueFactory::GetVarbinaryValue(value_buf.buf, value_buf.len,
                                                 false);
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
  return InsertTuple(std::move(tuple), txn);
}

bool QueryMetricsCatalog::DeleteQueryMetrics(const std::string &name,
                                             oid_t database_oid,
                                             concurrency::Transaction *txn) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Primary key index

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

stats::QueryMetric::QueryParamBuf QueryMetricsCatalog::GetParamTypes(
    const std::string &name, oid_t database_oid,
    concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({ColumnId::PARAM_TYPES});  // param_types
  oid_t index_offset = IndexId::PRIMARY_KEY;               // Primary key index
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  stats::QueryMetric::QueryParamBuf param_types;
  PL_ASSERT(result_tiles->size() <= 1);  // unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      param_types_value = (*result_tiles)[0]->GetValue(0, 0);
      param_types.buf = param_types_value.GetData();
      param_types.len = param_types_value.GetLength();
    }
  }

  return param_types;
}

int64_t QueryMetricsCatalog::GetNumParams(const std::string &name,
                                          oid_t database_oid,
                                          concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids({ColumnId::NUM_PARAMS});  // num_params
  oid_t index_offset = IndexId::PRIMARY_KEY;              // Primary key index
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  int64_t num_params = 0;
  PL_ASSERT(result_tiles->size() <= 1);  // unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      num_params = (*result_tiles)[0]
                       ->GetValue(0, 0)
                       .GetAs<int>();  // After projection left 1 column
    }
  }

  return num_params;
}

}  // End catalog namespace
}  // End peloton namespace
