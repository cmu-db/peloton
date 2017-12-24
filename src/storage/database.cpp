//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database.cpp
//
// Identification: src/storage/database.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "catalog/foreign_key.h"
#include "codegen/query_cache.h"
#include "common/exception.h"
#include "common/logger.h"
#include "gc/gc_manager_factory.h"
#include "index/index.h"
#include "storage/database.h"
#include "storage/table_factory.h"

namespace peloton {
namespace storage {

Database::Database(const oid_t database_oid) : database_oid(database_oid) {}

Database::~Database() {
  // Clean up all the tables
  LOG_TRACE("Deleting tables from database");
  for (auto table : tables) {
    delete table;
  }

  LOG_TRACE("Finish deleting tables from database");
}

//===----------------------------------------------------------------------===//
// TABLE
//===----------------------------------------------------------------------===//

void Database::AddTable(storage::DataTable *table, bool is_catalog) {
  {
    std::lock_guard<std::mutex> lock(database_mutex);
    tables.push_back(table);

    if (is_catalog == false) {
      // Register table to GC manager.
      auto *gc_manager = &gc::GCManagerFactory::GetInstance();
      assert(gc_manager != nullptr);
      gc_manager->RegisterTable(table->GetOid());
    }
  }
}

storage::DataTable *Database::GetTableWithOid(const oid_t table_oid) const {
  for (auto *table : tables) {
    if (table->GetOid() == table_oid) {
      return table;
    }
  }

  // Table not found
  throw CatalogException("Table with oid = " + std::to_string(table_oid) +
                         " is not found");
  return nullptr;
}

storage::DataTable *Database::GetTableWithName(
    const std::string &table_name) const {
  for (auto table : tables)
    if (table->GetName() == table_name) return table;
  throw CatalogException("Table '" + table_name + "' does not exist");
  return nullptr;
}

void Database::DropTableWithOid(const oid_t table_oid) {
  {
    std::lock_guard<std::mutex> lock(database_mutex);

    // Deregister table from GC manager.
    auto *gc_manager = &gc::GCManagerFactory::GetInstance();
    PL_ASSERT(gc_manager != nullptr);
    gc_manager->DeregisterTable(table_oid);

    // Deregister table from Query Cache manager
    codegen::QueryCache::Instance().Remove(table_oid);

    oid_t table_offset = 0;
    for (auto table : tables) {
      if (table->GetOid() == table_oid) {
        delete table;
        break;
      }
      table_offset++;
    }
    PL_ASSERT(table_offset < tables.size());

    // Drop the table
    tables.erase(tables.begin() + table_offset);
  }
}

storage::DataTable *Database::GetTable(const oid_t table_offset) const {
  PL_ASSERT(table_offset < tables.size());
  auto table = tables.at(table_offset);
  return table;
}

oid_t Database::GetTableCount() const { return tables.size(); }

//===----------------------------------------------------------------------===//
// UTILITIES
//===----------------------------------------------------------------------===//

// Get a string representation for debugging
const std::string Database::GetInfo() const {
  std::ostringstream os;

  os << peloton::GETINFO_THICK_LINE << std::endl;
  os << "DATABASE(" << GetOid() << ") : \n";

  oid_t table_count = GetTableCount();
  os << "Table Count : " << table_count << std::endl;

  oid_t table_itr = 0;
  for (auto table : tables) {
    if (table != nullptr) {
      os << "(" << ++table_itr << "/" << table_count << ") "
         << "Table Oid : " << table->GetOid() << std::endl;

      oid_t index_count = table->GetIndexCount();

      if (index_count > 0) {
        os << "Index Count : " << index_count << std::endl;
        for (oid_t index_itr = 0; index_itr < index_count; index_itr++) {
          auto index = table->GetIndex(index_itr);
          if (index == nullptr) continue;

          switch (index->GetIndexType()) {
            case IndexConstraintType::PRIMARY_KEY:
              os << "primary key index \n";
              break;
            case IndexConstraintType::UNIQUE:
              os << "unique index \n";
              break;
            default:
              os << "default index \n";
              break;
          }

          os << *index << std::endl;
        }
      }

      if (table->HasForeignKeys()) {
        os << "foreign tables \n";

        oid_t foreign_key_count = table->GetForeignKeyCount();
        for (oid_t foreign_key_itr = 0; foreign_key_itr < foreign_key_count;
             foreign_key_itr++) {
          auto foreign_key = table->GetForeignKey(foreign_key_itr);

          auto sink_table_oid = foreign_key->GetSinkTableOid();
          auto sink_table = GetTableWithOid(sink_table_oid);

          os << "table name : " << sink_table->GetName() << std::endl;
        }
      }
    }
  }

  os << peloton::GETINFO_THICK_LINE;

  return os.str();
}

// deprecated, use catalog::DatabaseCatalog::GetInstance()->GetDatabaseName()
std::string Database::GetDBName() { return database_name; }

// deprecated, use catalog::DatabaseCatalog::GetInstance()->GetDatabaseName()
void Database::setDBName(const std::string &database_name) {
  Database::database_name = database_name;
}

}  // namespace storage
}  // namespace peloton
