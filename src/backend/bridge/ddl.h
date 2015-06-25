/**
 * @brief Header for postgres ddl.
 *
 * Copyright(c) 2015, CMU
 */

#ifdef __cplusplus

#include <cassert>

#include "backend/bridge/bridge.h"
#include "backend/catalog/catalog.h"
#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/index/index_factory.h"
#include "backend/storage/backend_vm.h"
#include "backend/storage/table_factory.h"

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
  static bool DropTable(std::string table_name);
  static bool CreateIndex(std::string index_name, std::string table_name, int type, bool unique, DDL_ColumnInfo* ddl_columnInfoForTupleSchema,  DDL_ColumnInfo* ddl_columnInfoForKeySchema, int num_columns);
};

extern "C" {
  bool DDL_CreateTable(char* table_name, DDL_ColumnInfo* ddl_columnInfo, int num_columns);
  bool DDL_DropTable(char* table_name );
  bool DDL_CreateIndex(char* index_name, char* table_name, int type, bool unique, DDL_ColumnInfo* ddl_columnInfoForTupleSchema, DDL_ColumnInfo* ddl_columnInfoForKeySchema , int num_columns);
}

} // namespace bridge
} // namespace nstore

#endif

extern bool DDL_CreateTable(char* table_name, DDL_ColumnInfo* ddl_columnInfo, int num_columns);

extern bool DDL_DropTable(char* table_name );

extern bool DDL_CreateIndex(char* index_name, char* table_name, int type, bool unique, DDL_ColumnInfo* ddl_columnInfoForTupleSchema, DDL_ColumnInfo* ddl_columnInfoForKeySchema , int num_columns);
