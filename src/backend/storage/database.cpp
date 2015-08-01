/*-------------------------------------------------------------------------
 *
 * database.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/table.cpp
 *
 *-------------------------------------------------------------------------
 */

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

void Database::AddTable(storage::DataTable* table) {
  {
    std::lock_guard<std::mutex> lock(database_mutex);
    tables.push_back(table);
  }
}

storage::DataTable* Database::GetTableWithOid(const oid_t table_oid) const {
  for (auto table : tables)
    if (table->GetOid() == table_oid) return table;

  return nullptr;
}

storage::DataTable* Database::GetTableWithName(
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
      if (table->GetOid() == table_oid) break;
      table_offset++;
    }
    assert(table_offset < tables.size());

    // Drop the database
    tables.erase(tables.begin() + table_offset);
  }
}

storage::DataTable* Database::GetTable(const oid_t table_offset) const {
  assert(table_offset < tables.size());
  auto table = tables.at(table_offset);
  return table;
}

oid_t Database::GetTableCount() const { return tables.size(); }

//===--------------------------------------------------------------------===//
// STATS
//===--------------------------------------------------------------------===//

void Database::UpdateStats(Peloton_Status* status){
  LOG_INFO("Update All Stats in Database(%u)", database_oid);

  // TODO :: need to check whether ... table... is changed or not

  std::vector<dirty_table_info*> dirty_tables;

  for( int table_offset=0; table_offset<GetTableCount(); table_offset++){
    auto table = GetTable(table_offset);

    std::vector<dirty_index_info*> dirty_indexes;
    for (int index_offset = 0; index_offset < table->GetIndexCount(); index_offset++) {
      auto index = table->GetIndex(index_offset);
      auto dirty_index = CreateDirtyIndex(index->GetOid(), index->GetNumberOfTuples());

      dirty_indexes.push_back(dirty_index);
    }
    auto dirty_table = CreateDirtyTable(table->GetOid(),
                                        table->GetNumberOfTuples(),
                                        CreateDirtyIndexes(dirty_indexes),
                                        dirty_indexes.size());

    dirty_tables.push_back(dirty_table);
  }

  status->m_dirty_tables = CreateDirtyTables(dirty_tables);
  status->m_dirty_count = dirty_tables.size();
}

void Database::UpdateStatsWithOid(const oid_t table_oid){
  LOG_INFO("Update table(%u)'s stats in Database(%u)", table_oid, database_oid);
  auto table = GetTableWithOid(table_oid);
  bridge::Bridge::SetNumberOfTuples(table_oid, table->GetNumberOfTuples());

  for (int index_offset = 0; index_offset < table->GetIndexCount();
       index_offset++) {
    auto index = table->GetIndex(index_offset);
    bridge::Bridge::SetNumberOfTuples(index->GetOid(),
                                      index->GetNumberOfTuples());
  }
}

//===--------------------------------------------------------------------===//
// UTILITIES
//===--------------------------------------------------------------------===//

dirty_table_info** Database::CreateDirtyTables(std::vector< dirty_table_info*> dirty_tables_vec){
  //XXX:: TopSharedMem???

  MemoryContext oldcxt = MemoryContextSwitchTo(TopSharedMemoryContext);
  dirty_table_info** dirty_tables =  (dirty_table_info**)palloc(sizeof(dirty_table_info*)*dirty_tables_vec.size());
  MemoryContextSwitchTo(oldcxt);

  oid_t table_itr=0;
  for(auto dirty_table : dirty_tables_vec)
    dirty_tables[table_itr] = dirty_table;
    
  return dirty_tables;
}

dirty_index_info** Database::CreateDirtyIndexes(std::vector< dirty_index_info*> dirty_indexes_vec){

  MemoryContext oldcxt = MemoryContextSwitchTo(TopSharedMemoryContext);
  dirty_index_info** dirty_indexes =  (dirty_index_info**)palloc(sizeof(dirty_index_info*)*dirty_indexes_vec.size());
  MemoryContextSwitchTo(oldcxt);

  oid_t index_itr=0;
  for(auto dirty_index : dirty_indexes_vec)
    dirty_indexes[index_itr] = dirty_index;
    
  return dirty_indexes;
}

dirty_table_info* Database::CreateDirtyTable(oid_t table_oid, float number_of_tuples,  dirty_index_info** dirty_indexes, oid_t index_count){

  MemoryContext oldcxt = MemoryContextSwitchTo(TopSharedMemoryContext);
  dirty_table_info* dirty_table = (dirty_table_info*)palloc(sizeof(dirty_table_info));
  MemoryContextSwitchTo(oldcxt);

  dirty_table->table_oid = table_oid;
  dirty_table->number_of_tuples = number_of_tuples;
  dirty_table->dirty_indexes = dirty_indexes;
  dirty_table->index_count = index_count;

  return dirty_table;
}

dirty_index_info* Database::CreateDirtyIndex(oid_t index_oid, float number_of_tuples){

  MemoryContext oldcxt = MemoryContextSwitchTo(TopSharedMemoryContext);
  dirty_index_info* dirty_index = (dirty_index_info*)palloc(sizeof(dirty_index_info));
  MemoryContextSwitchTo(oldcxt);

  dirty_index->index_oid = index_oid;
  dirty_index->number_of_tuples = number_of_tuples;

  return dirty_index;
}

std::ostream& operator<<(std::ostream& os, const Database& database) {
  os << "=====================================================\n";
  os << "DATABASE(" << database.GetOid() << ") : \n";

  oid_t table_count = database.GetTableCount();
  std::cout << "Table Count : " << table_count << "\n";

  oid_t table_itr = 0;
  for (auto table : database.tables) {
    if (table != nullptr) {
      std::cout << "(" << ++table_itr << "/" << table_count << ") "
                << "Table Name(" << table->GetOid() << ") : " << table->GetName() << "\n"
                << *(table->GetSchema()) << std::endl;

      oid_t index_count = table->GetIndexCount();

      if (index_count > 0) {
        std::cout << "Index Count : " << index_count << std::endl;
        for (int index_itr = 0; index_itr < index_count; index_itr++) {
          index::Index* index = table->GetIndex(index_itr);

          switch (index->GetIndexType()) {
            case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY:
              std::cout << "primary key index \n";
              break;
            case INDEX_CONSTRAINT_TYPE_UNIQUE:
              std::cout << "unique index \n";
              break;
            default:
              std::cout << "default index \n";
              break;
          }
          std::cout << *index << std::endl;
        }
      }

      if (table->HasForeignKeys()) {
        std::cout << "foreign tables \n";

        oid_t foreign_key_count = table->GetForeignKeyCount();
        for (int foreign_key_itr = 0; foreign_key_itr < foreign_key_count;
             foreign_key_itr++) {
          auto foreign_key = table->GetForeignKey(foreign_key_itr);

          auto sink_table_oid = foreign_key->GetSinkTableOid();
          auto sink_table = database.GetTableWithOid(sink_table_oid);

          auto sink_table_schema = sink_table->GetSchema();
          std::cout << "table name : " << sink_table->GetName() << " "
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
