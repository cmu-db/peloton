//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bootstrap.h
//
// Identification: src/backend/bridge/ddl/bootstrap.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "backend/catalog/schema.h"
#include "postgres.h"
#include "c.h"
#include "ddl_raw_structures.h"
#include "miscadmin.h"
#include "utils/rel.h"

struct Peloton_Status;

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Bootstrap
//===--------------------------------------------------------------------===//

class Bootstrap {
 public:
  static raw_database_info *GetRawDatabase(void);

  static bool BootstrapPeloton(raw_database_info *raw_database,
                               Peloton_Status *status);

 private:
  static raw_database_info *InitRawDatabase();

  static void GetRawTableAndIndex(std::vector<raw_table_info *> &raw_tables,
                                  std::vector<raw_index_info *> &raw_indexes);

  static void GetRawForeignKeys(
      std::vector<raw_foreign_key_info *> &raw_foreignkeys);

  static raw_table_info *GetRawTable(
      oid_t table_oid, std::string table_name,
      std::vector<raw_column_info *> raw_columns);

  static raw_index_info *GetRawIndex(
      oid_t index_oid, std::string index_name,
      std::vector<raw_column_info *> raw_columns);

  static std::vector<raw_column_info *> GetRawColumn(Oid tuple_oid,
                                                     char relation_kind,
                                                     Relation pg_attribute_rel);

  static void CreateTables(raw_table_info **raw_tables, oid_t table_count);

  static void CreateIndexes(raw_index_info **raw_indexes, oid_t index_count);

  static void CreateForeignkeys(raw_foreign_key_info **raw_foreignkeys,
                                oid_t foreignkey_count);

  static std::vector<catalog::Column> CreateColumns(
      raw_column_info **raw_columns, oid_t column_count);

  static std::vector<std::string> CreateKeyColumnNames(char **raw_column_names,
                                                       oid_t raw_column_count);

  static std::vector<catalog::Constraint> CreateConstraints(
      raw_constraint_info **raw_constraints, oid_t constraint_count);
};

}  // namespace bridge
}  // namespace peloton
