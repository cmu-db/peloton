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
#include "brain/brain.h"
#include "catalog/catalog.h"
#include "catalog/table_catalog.h"
#include "storage/data_table.h"
#include "catalog/schema.h"


namespace peloton{
namespace brain {
  class CatalogSyncBrainJob: public BrainJob {
   public:
    CatalogSyncBrainJob(BrainEnvironment *env, std::vector<std::string>): BrainJob(env) {}

    void OnJobInvocation(BrainEnvironment *env) override {
      for (auto *catalog : catalog::Catalog::GetInstance()->AvailableCatalogs()) {
        pqxx::result r = env->ExecuteQuery("SELECT * FROM pg_catalog." + catalog->GetName());
        for (auto row : r) {
          catalog::Schema *catalog_schema = catalog->GetDataTable()->GetSchema();
          std::unique_ptr<storage::Tuple> tuple(catalog_schema);
          for (auto field : row) {
            oid_t column_id = catalog_schema->GetColumnID(field.name());

          }
        }
      }
    }
  };
}
}