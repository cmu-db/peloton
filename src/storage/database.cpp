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
#include "storage/database.h"
#include "storage/table_factory.h"
#include "common/logger.h"
#include "index/index.h"

namespace peloton {
namespace storage {

Database::Database(const oid_t &database_oid) : database_oid(database_oid) {}

Database::~Database() {
  // Clean up all the tables
  for (auto table : tables) delete table;
}

//===--------------------------------------------------------------------===//
// TABLE
//===--------------------------------------------------------------------===//

void Database::AddTable(storage::DataTable *table) {
  {
    std::lock_guard<std::mutex> lock(database_mutex);
    tables.push_back(table);
  }
}

storage::DataTable *Database::GetTableWithOid(const oid_t table_oid) const {
  for (auto table : tables)
    if (table->GetOid() == table_oid) return table;

  return nullptr;
}

storage::DataTable *Database::GetTableWithName(
    const std::string table_name) const {
  for (auto table : tables)
    if (table->GetName() == table_name) return table;

  return nullptr;
}

void Database::DropTableWithOid(const oid_t table_oid) {
  {
    std::lock_guard<std::mutex> lock(database_mutex);

    oid_t table_offset = 0;
    for (auto table : tables) {
      if (table->GetOid() == table_oid) {
        delete table;
        break;
      }
      table_offset++;
    }
    PL_ASSERT(table_offset < tables.size());

    // Drop the database
    tables.erase(tables.begin() + table_offset);
  }
}

storage::DataTable *Database::GetTable(const oid_t table_offset) const {
  PL_ASSERT(table_offset < tables.size());
  auto table = tables.at(table_offset);
  return table;
}

oid_t Database::GetTableCount() const { return tables.size(); }

//===--------------------------------------------------------------------===//
// UTILITIES
//===--------------------------------------------------------------------===//

// Get a string representation for debugging
const std::string Database::GetInfo() const {
  std::ostringstream os;

  os << "=====================================================\n";
  os << "DATABASE(" << GetOid() << ") : \n";

  oid_t table_count = GetTableCount();
  os << "Table Count : " << table_count << "\n";

  oid_t table_itr = 0;
  for (auto table : tables) {
    if (table != nullptr) {
      os << "(" << ++table_itr << "/" << table_count << ") "
         << "Table Name(" << table->GetOid() << ") : " << table->GetName()
         << std::endl;

      oid_t index_count = table->GetIndexCount();

      if (index_count > 0) {
        os << "Index Count : " << index_count << std::endl;
        for (oid_t index_itr = 0; index_itr < index_count; index_itr++) {
          index::Index *index = table->GetIndex(index_itr);

          switch (index->GetIndexType()) {
            case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY:
              os << "primary key index \n";
              break;
            case INDEX_CONSTRAINT_TYPE_UNIQUE:
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

  os << "=====================================================\n";

  return os.str();
}

}  // End storage namespace
}  // End peloton namespace
