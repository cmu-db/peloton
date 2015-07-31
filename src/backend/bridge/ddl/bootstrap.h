/*-------------------------------------------------------------------------
 *
 * bootstrap.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/bootstrap.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <vector>

#include "postgres.h"
#include "c.h"
#include "miscadmin.h"
#include "access/htup.h"
#include "utils/rel.h"

#include "backend/catalog/schema.h"
#include "backend/catalog/schema.h"
#include "backend/bridge/ddl/ddl.h"
#include "backend/bridge/ddl/ddl_index.h"
#include "backend/bridge/ddl/raw_structure.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Bootstrap
//===--------------------------------------------------------------------===//

class Bootstrap {
 public:
  static raw_database_info* GetRawDatabase(void);

  static bool NewBootstrapPeloton(raw_database_info* raw_database);

  // TODO :: Remove 
  static bool BootstrapPeloton(void);


 private:

  static raw_database_info* InitRawDatabase();

  static void GetRawTableAndIndex(std::vector<raw_table_info*>& raw_tables,
                                  std::vector<raw_index_info*>& raw_indexes);

  static void GetRawForeignKeys(std::vector<raw_foreignkey_info*>& raw_foreignkeys);

  static raw_table_info* GetRawTable(oid_t table_oid, 
                                     std::string table_name, 
                                     std::vector<raw_column_info*> raw_columns);

  static raw_index_info* GetRawIndex(oid_t index_oid, 
                                     std::string index_name,
                                     std::vector<raw_column_info*> raw_columns);

  static std::vector<raw_column_info*> GetRawColumn(Oid tuple_oid, 
                                                    Relation pg_attribute_rel);

  static void CreateTables(raw_table_info** raw_tables, oid_t table_count);

  static void CreateIndexes(raw_index_info** raw_indexes, 
                            oid_t index_count);

  static void CreateForeignkeys(raw_foreignkey_info** raw_foreignkeys, 
                                oid_t foreignkey_count);

  static std::vector<catalog::Column>
         CreateColumns(raw_column_info** raw_columns, 
                       oid_t column_count);

  static std::vector<std::string> CreateKeyColumnNames(char** raw_column_names, oid_t raw_column_count);
 

  static std::vector<catalog::Constraint>
         CreateConstraints(raw_constraint_info** raw_constraints, 
                           oid_t constraint_count);

  // Transform a pg class tuple to a list of columns
  // TODO REMOVE
  static std::vector<peloton::catalog::Column> GetRelationColumns(
      Oid tuple_oid, Relation pg_attribute_rel);

  // Create a peloton table or index
  // TODO REMOVE
  static void CreatePelotonStructure(
      char relation_kind, char* relation_name, Oid tuple_oid,
      const std::vector<catalog::Column>& columns,
      std::vector<IndexInfo>& index_infos);

  // Set up the foreign keys constraints
  // TODO REMOVE
  static void CreateIndexInfos(oid_t tuple_oid, char* relation_name,
                               const std::vector<catalog::Column>& columns,
                               std::vector<IndexInfo>& index_infos);

  // Set up the foreign keys constraints
  // TODO REMOVE
  static void LinkForeignKeys();

  // TODO REMOVE
  // Create databases
  static void CreateDatabases();

};

}  // namespace bridge
}  // namespace peloton
