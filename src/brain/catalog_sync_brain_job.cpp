//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_sync_brain_job.cpp
//
// Identification: src/brain/catalog_sync_brain_job.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "brain/catalog_sync_brain_job.h"

namespace peloton {
namespace brain {
void CatalogSyncBrainJob::OnJobInvocation(BrainEnvironment *env) {
  auto &manager = concurrency::TransactionManagerFactory::GetInstance();
  for (auto *catalog : catalog::Catalog::GetInstance()->AvailableCatalogs())
    SyncCatalog(catalog, env, manager);
}

time_t CatalogSyncBrainJob::TimeFromString(const char *str) {
  struct tm tm;
  PELOTON_MEMSET(&tm, 0, sizeof(struct tm));
  strptime(str, "%Y-%m-%d %H:%M:%S", &tm);
  return mktime(&tm);
}

std::string CatalogSyncBrainJob::FetchCatalogQuery(catalog::AbstractCatalog *catalog) {
  // We need to special cast these two tables because we cannot put a reasonable
  // primary key on them without sequences
  if (catalog->GetName() == QUERY_HISTORY_CATALOG_NAME)
    return "SELECT * FROM pg_catalog." + std::string(QUERY_HISTORY_CATALOG_NAME)
        + " WHERE timestamp > " + std::to_string(last_history_timestamp_);
  else
    return "SELECT * FROM pg_catalog." + catalog->GetName();
}

void CatalogSyncBrainJob::UpdateTimestamp(catalog::AbstractCatalog *catalog,
                                          pqxx::field field) {
  if (catalog->GetName() == QUERY_HISTORY_CATALOG_NAME
      && field.name() == std::string("timestamp"))
    last_history_timestamp_ =
        std::max(last_history_timestamp_, field.as<int64_t>());
}

void CatalogSyncBrainJob::SyncCatalog(catalog::AbstractCatalog *catalog,
                                      BrainEnvironment *env,
                                      concurrency::TransactionManager &manager) {
  pqxx::result r = env->ExecuteQuery(FetchCatalogQuery(catalog));
  for (auto row : r) {
    concurrency::TransactionContext *txn =
        manager.BeginTransaction(IsolationLevelType::REPEATABLE_READS);
    catalog::Schema *catalog_schema = catalog->GetDataTable()->GetSchema();
    std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(catalog_schema, true));
    for (auto field : row) {
      oid_t column_id = catalog_schema->GetColumnID(field.name());
      tuple->SetValue(column_id, PqxxFieldToPelotonValue(field));
      UpdateTimestamp(catalog, field);
    }
    catalog->InsertTuple(std::move(tuple), txn);
    // We know this will always succeed on the brain side
    manager.CommitTransaction(txn);
  }
}

type::Value CatalogSyncBrainJob::PqxxFieldToPelotonValue(pqxx::field &f) {
  type::TypeId type = PostgresValueTypeToPelotonValueType(
      static_cast<PostgresValueType>(f.type()));
  if (f.is_null()) {
    return type == peloton::type::TypeId::VARCHAR
           ? type::ValueFactory::GetVarcharValue("")
           : type::ValueFactory::GetNullValueByType(type);
  }
  switch (type) {
    case type::TypeId::BOOLEAN:
      return type::ValueFactory::GetBooleanValue(f.as<bool>());
    case type::TypeId::TINYINT:
      return type::ValueFactory::GetTinyIntValue(static_cast<int8_t>(f.as<
          int32_t>()));
    case type::TypeId::SMALLINT:
      return type::ValueFactory::GetSmallIntValue(static_cast<int16_t>(f.as<
          int32_t>()));
    case type::TypeId::INTEGER:
      return type::ValueFactory::GetIntegerValue(f.as<int32_t>());
    case type::TypeId::BIGINT:
      return type::ValueFactory::GetBigIntValue(f.as<int64_t>());
    case type::TypeId::TIMESTAMP:
      return type::ValueFactory::GetTimestampValue(TimeFromString(f.c_str()));
    case type::TypeId::DECIMAL:
      return type::ValueFactory::GetDecimalValue(f.as<double>());
    case type::TypeId::VARCHAR:return type::ValueFactory::GetVarcharValue(f.c_str());
    default:
      throw ConversionException(StringUtil::Format(
          "No corresponding c++ type for postgres type %d",
          static_cast<int>(type)));
  }
}

} // namespace brain
} // nanespace peloton