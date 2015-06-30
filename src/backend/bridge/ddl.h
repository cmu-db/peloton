/**
 * @brief Header for postgres ddl.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "pg_config_manual.h"
#include "c.h"

#ifdef __cplusplus
#include "bridge/bridge.h"
#include "backend/catalog/catalog.h"
#include "backend/catalog/constraint.h"
#include "backend/catalog/schema.h"
#endif

typedef struct 
{
  int valueType;
  int column_offset;
  int column_length;
  char name[NAMEDATALEN];
  bool allow_null;
  bool is_inlined;

  /* constraints */
  int* constraintType;
  char** conname;
} DDL_ColumnInfo;

/* ------------------------------------------------------------
 * C-style function declarations
 * ------------------------------------------------------------
 */

extern bool DDLCreateTable(char *table_name,
                           DDL_ColumnInfo* schema,
                           int num_columns,
                           int *num_of_constraints_of_each_column);

extern bool DDLDropTable(Oid table_oid);

extern bool DDLCreateIndex(char *index_name,
                           char *table_name,
                           int index_type,
                           bool unique_keys,
                           char** key_column_names,
                           int num_columns_in_key);

#ifdef __cplusplus

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL Class 
//===--------------------------------------------------------------------===//

class DDL {

 public:
  DDL(const DDL &) = delete;
  DDL& operator=(const DDL &) = delete;
  DDL(DDL &&) = delete;
  DDL& operator=(DDL &&) = delete;

  static bool CreateTable(std::string table_name,
                          DDL_ColumnInfo *schema,
                          int num_columns,
                          int *num_of_constraints_of_each_column);

  static bool DropTable(Oid table_oid);

  static bool CreateIndex(std::string index_name,
                          std::string table_name,
                          int index_type,
                          bool unique_keys,
                          char** key_column_names,
                          int num_columns_in_key);

};

} // namespace bridge
} // namespace peloton

#endif


