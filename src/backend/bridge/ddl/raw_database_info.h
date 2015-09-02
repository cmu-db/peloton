#pragma once

#include "backend/bridge/ddl/raw_table_info.h"
#include "backend/bridge/ddl/raw_index_info.h"
#include "backend/bridge/ddl/raw_foreign_key_info.h"

#include "utils/relcache.h"
#include "commands/dbcommands.h"

namespace peloton {
namespace bridge {

class raw_database_info {

public:
  raw_database_info(const raw_database_info &) = delete;
  raw_database_info &operator=(const raw_database_info &) = delete;
  raw_database_info(raw_database_info &&) = delete;
  raw_database_info &operator=(raw_database_info &&) = delete;

  raw_database_info(Oid database_oid)
  : database_oid(database_oid)  {
    database_name = get_database_name(database_oid);
  }

  oid_t GetDbOid(void) const { return database_oid; }

  std::string GetDbName(void) const { return database_name; }

  //===--------------------------------------------------------------------===//
  // Collect Raw Data from Postgres
  //===--------------------------------------------------------------------===//

  void CollectRawTableAndIndex(void);

  void CollectRawForeignKeys(void);

  std::vector<raw_column_info> CollectRawColumn(oid_t relation_oid,
                                                Relation pg_attribute_rel);

  void AddRawTable(oid_t table_oid,
                   std::string table_name,
                   std::vector<raw_column_info> raw_columns);

  void AddRawIndex(oid_t index_oid,
                   std::string index_name,
                   std::vector<raw_column_info> raw_columns);

  void AddRawForeignKey(raw_foreign_key_info raw_foreign_key);

  //===--------------------------------------------------------------------===//
  // Create Peloton Objects with Raw Data
  //===--------------------------------------------------------------------===//

  bool CreateDatabase(void) const;

  void CreateTables(void) const;

  void CreateIndexes(void) const;

  void CreateForeignkeys(void) const;

private:

  oid_t database_oid;
  std::string database_name;

  std::vector<raw_table_info> raw_tables;
  std::vector<raw_index_info> raw_indexes;
  std::vector<raw_foreign_key_info> raw_foreign_keys;

};

}  // namespace bridge
}  // namespace peloton
