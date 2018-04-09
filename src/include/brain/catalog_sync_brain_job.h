//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_sync_brain_job.h
//
// Identification: src/include/brain/catalog_sync_brain_job.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once
#include <vector>
#include <string>
#include <pqxx/pqxx>
#include "concurrency/transaction_manager_factory.h"
#include "brain/brain.h"
#include "catalog/catalog.h"
#include "catalog/table_catalog.h"
#include "storage/data_table.h"
#include "catalog/schema.h"
#include "type/value_factory.h"

namespace peloton {
namespace brain {
class CatalogSyncBrainJob : public BrainJob {
 public:
  CatalogSyncBrainJob(BrainEnvironment *env, std::vector<std::string>)
      : BrainJob(env) {}

  // TODO(tianyu): Eventually use Log for replication
  void OnJobInvocation(BrainEnvironment *env) override {
    for (auto *catalog : catalog::Catalog::GetInstance()->AvailableCatalogs()) {
      pqxx::result r =
          env->ExecuteQuery("SELECT * FROM pg_catalog." + catalog->GetName());
      for (auto &row : r) {
        concurrency::TransactionContext *txn =
            concurrency::TransactionManagerFactory::GetInstance()
                .BeginTransaction(IsolationLevelType::REPEATABLE_READS);
        catalog::Schema *catalog_schema = catalog->GetDataTable()->GetSchema();
        std::unique_ptr<storage::Tuple> tuple(catalog_schema);
        for (auto &field : row) {
          oid_t column_id = catalog_schema->GetColumnID(field.name());
          tuple->SetValue(column_id, PqxxFieldToPelotonValue(field));
        }
        catalog->InsertTuple(std::move(tuple), txn);
      }
    }
  }

 private:
  type::Value PqxxFieldToPelotonValue(pqxx::field &f) {
    type::TypeId type = PostgresValueTypeToPelotonValueType(
        reinterpret_cast<PostgresValueType>(f.type()));
    if (f.is_null()) return type::ValueFactory::GetNullValueByType(type);
    switch (type) {
      case type::TypeId::BOOLEAN:
        return type::ValueFactory::GetBooleanValue(f.as<bool>());
      case type::TypeId::TINYINT:
        return type::ValueFactory::GetTinyIntValue(f.as<int8_t>());
      case type::TypeId::SMALLINT:
        return type::ValueFactory::GetSmallIntValue(f.as<int16_t>());
      case type::TypeId::INTEGER:
        return type::ValueFactory::GetIntegerValue(f.as<int32_t>());
      case type::TypeId::BIGINT:
        return type::ValueFactory::GetBigIntValue(f.as<int64_t>());
      case type::TypeId::TIMESTAMP:
        return type::ValueFactory::GetTimestampValue(f.as<int64_t>());
      case type::TypeId::DECIMAL:
        return type::ValueFactory::GetDecimalValue(f.as<double>());
      case type::TypeId::VARCHAR:
        return type::ValueFactory::GetVarcharValue(f.c_str());
      default:
        throw ConversionException(StringUtil::Format(
            "No corresponding c++ type for postgres type %d",
            static_cast<int>(type)));
    }
  }
};
}
}