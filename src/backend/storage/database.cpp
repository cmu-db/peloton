//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// database.cpp
//
// Identification: src/backend/storage/database.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "postmaster/peloton.h"
#include "backend/storage/database.h"
#include "backend/storage/table_factory.h"
#include "backend/common/logger.h"

#include <sstream>

namespace peloton {
namespace storage {

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
      if (table->GetOid() == table_oid){
        delete table;
        break;
      }
      table_offset++;
    }
    assert(table_offset < tables.size());

    // Drop the database
    tables.erase(tables.begin() + table_offset);
  }
}

storage::DataTable *Database::GetTable(const oid_t table_offset) const {
  assert(table_offset < tables.size());
  auto table = tables.at(table_offset);
  return table;
}

oid_t Database::GetTableCount() const { return tables.size(); }

//===--------------------------------------------------------------------===//
// STATS
//===--------------------------------------------------------------------===//

void Database::UpdateStats() const{
  LOG_INFO("Update All Stats in Database(%u)", database_oid);
  for( int table_offset=0; table_offset<GetTableCount(); table_offset++){
    auto table = GetTable(table_offset);
    bridge::Bridge::SetNumberOfTuples(table->GetOid(), table->GetNumberOfTuples());

    for( int index_offset=0; index_offset<table->GetIndexCount(); index_offset++){
      auto index = table->GetIndex(index_offset);
      bridge::Bridge::SetNumberOfTuples(index->GetOid(), index->GetNumberOfTuples());
    }
  }
}

void Database::UpdateStatsWithOid(const oid_t table_oid) const{
  LOG_INFO("Update table(%u)'s stats in Database(%u)", table_oid, database_oid);

  auto table = GetTableWithOid(table_oid);
  bridge::Bridge::SetNumberOfTuples(table_oid, table->GetNumberOfTuples());

  for( int index_offset=0; index_offset<table->GetIndexCount(); index_offset++){
    auto index = table->GetIndex(index_offset);
    bridge::Bridge::SetNumberOfTuples(index->GetOid(), index->GetNumberOfTuples());
  }
}

//===--------------------------------------------------------------------===//
// UTILITIES
//===--------------------------------------------------------------------===//

std::ostream &operator<<(std::ostream &os, const Database &database) {
  os << "=====================================================\n";
  os << "DATABASE(" << database.GetOid() << ") : \n";

  oid_t table_count = database.GetTableCount();
  os << "Table Count : " << table_count << "\n";

  oid_t table_itr = 0;
  for (auto table : database.tables) {
    if (table != nullptr) {
      std::cout << "(" << ++table_itr << "/" << table_count << ") "
                << "Table Name(" << table->GetOid()
                << ") : " << table->GetName() << "\n" << *(table->GetSchema())
                << std::endl;

      oid_t index_count = table->GetIndexCount();

      if (index_count > 0) {
        os << "Index Count : " << index_count << std::endl;
        for (int index_itr = 0; index_itr < index_count; index_itr++) {
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
        for (int foreign_key_itr = 0; foreign_key_itr < foreign_key_count;
             foreign_key_itr++) {
          auto foreign_key = table->GetForeignKey(foreign_key_itr);

          auto sink_table_oid = foreign_key->GetSinkTableOid();
          auto sink_table = database.GetTableWithOid(sink_table_oid);

          auto sink_table_schema = sink_table->GetSchema();
          os << "table name : " << sink_table->GetName() << " "
                    << *sink_table_schema << std::endl;
        }
      }
    }
  }

  os << "=====================================================\n";

  return os;
}

}  // End storage namespace
}  // End peloton namespace
