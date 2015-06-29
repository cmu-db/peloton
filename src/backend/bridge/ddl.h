/**
 * @brief Header for postgres ddl.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "pg_config_manual.h"
#include "c.h"

#ifdef __cplusplus
#include "backend/catalog/schema.h"
#endif

typedef struct 
{
  int type;
  int column_offset;
  int column_length;
  char name[NAMEDATALEN];
  bool allow_null;
  bool is_inlined;
} ColumnInfo;

/* ------------------------------------------------------------
 * C-style function declarations
 * ------------------------------------------------------------
 */

extern bool DDLCreateTable(char *table_name,
                           ColumnInfo *schema,
                           int num_columns);

extern bool DDLDropTable(Oid table_oid);

extern bool DDLCreateIndex(char *index_name,
                           char *table_name,
                           int index_type,
                           bool unique_keys,
                           ColumnInfo *key_column_info,
                           int num_columns_in_key);

#ifdef __cplusplus

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL Class 
//===--------------------------------------------------------------------===//

class DDL {

 public:

  static bool CreateTable(std::string table_name,
                          ColumnInfo *schema,
                          int num_columns);

  static bool DropTable(Oid table_oid);

  static bool CreateIndex(std::string index_name,
                          std::string table_name,
                          int index_type,
                          bool unique_keys,
                          ColumnInfo *key_column_info,
                          int num_columns_in_key);

};

} // namespace bridge
} // namespace peloton

#endif
