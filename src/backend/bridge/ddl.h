/**
 * @brief Header for postgres ddl.
 *
 * Copyright(c) 2015, CMU
 */

#ifdef __cplusplus
#include "backend/catalog/schema.h"
#endif

#pragma once

typedef struct 
{
   int type;
   int column_offset;
   int column_length;
   char name[256]; // TODO :: Default size should be checked by joy 
   bool allow_null;
   bool is_inlined;
} DDL_ColumnInfo;

#ifdef __cplusplus

namespace nstore {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL Class 
//===--------------------------------------------------------------------===//

class DDL {
public:
  static bool CreateTable(std::string table_name, DDL_ColumnInfo* ddl_columnInfo, int num_columns, catalog::Schema* schema);

};

extern "C" {
  bool DDL_CreateTable(char* table_name, DDL_ColumnInfo* ddl_columnInfo, int num_columns);
}

} // namespace bridge
} // namespace nstore

#endif

extern bool DDL_CreateTable(char* table_name, DDL_ColumnInfo* ddl_columnInfo, int num_columns);

