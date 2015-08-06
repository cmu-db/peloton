//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bootstrap_utils.h
//
// Identification: src/backend/bridge/ddl/bootstrap_utils.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/bridge/ddl/bootstrap.h"

#include "postgres.h"
#include "c.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Bootstrap Utils
//===--------------------------------------------------------------------===//

class BootstrapUtils {
 public:
  //===--------------------------------------------------------------------===//
  // Copy operator
  //===--------------------------------------------------------------------===//

  static void CopyRawTables(raw_database_info *raw_database,
                            std::vector<raw_table_info *> raw_tables);

  static void CopyRawIndexes(raw_database_info *raw_database,
                             std::vector<raw_index_info *> raw_indexes);

  static void CopyRawForeignkeys(
      raw_database_info *raw_database,
      std::vector<raw_foreign_key_info *> raw_foreignkeys);

  static char *CopyString(const char *string);

  static char **CopyStrings(std::vector<std::string> strings);

  //===--------------------------------------------------------------------===//
  // Print operator
  //===--------------------------------------------------------------------===//
  static void PrintRawDatabase(raw_database_info *raw_database);

  static void PrintRawTables(raw_table_info **raw_tables, oid_t table_count);

  static void PrintRawIndexes(raw_index_info **raw_indexes, oid_t index_count);

  static void PrintRawForeignkeys(raw_foreign_key_info **raw_foreignkeys,
                                  oid_t foreignkey_count);

  static void PrintRawTable(raw_table_info *raw_table);

  static void PrintRawIndex(raw_index_info *raw_index);

  static void PrintRawConstraint(raw_constraint_info *raw_constraint);

  static void PrintRawConstraints(raw_constraint_info **raw_constraints,
                                  oid_t constraint_count);

  static void PrintRawColumn(raw_column_info *raw_column);

  static void PrintRawColumns(raw_column_info **raw_columns,
                              oid_t column_count);

  static void PrintColumnNames(char **column_names, oid_t column_count);

  static void PrintColumnNums(int *column_nums, oid_t column_count);
};

}  // namespace bridge
}  // namespace peloton
