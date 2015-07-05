/**
 * @brief Header for postgres ddl.
 *
 * Copyright(c) 2015, CMU
 */


#pragma once

#include "postgres.h"
#include "pg_config_manual.h"
#include "c.h"

#include "bridge/bridge.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "catalog/pg_am.h"

#include "backend/catalog/catalog.h"
#include "backend/catalog/constraint.h"
#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/index/index_factory.h"
#include "backend/storage/backend_vm.h"
#include "backend/storage/table_factory.h"


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

  //===--------------------------------------------------------------------===//
  // Function Definition
  //===--------------------------------------------------------------------===//

  static void ProcessUtility(Node *parsetree,
                             const char *queryString);

  static std::vector<catalog::ColumnInfo> ConstructColumnInfoByParsingCreateStmt( CreateStmt* Cstmt );

  //===--------------------------------------------------------------------===//
  // Create Object
  //===--------------------------------------------------------------------===//

  static bool CreateTable( std::string table_name,
                            std::vector<catalog::ColumnInfo> column_infos,
                            catalog::Schema *schema = NULL);

  static bool CreateIndex(std::string index_name,
                          std::string table_name,
                          int index_type,
                          bool unique_keys,
                          char** key_column_names,
                          int num_columns_in_key);

  static bool CreateIndex2(std::string index_name,
                           std::string table_name,
                           IndexMethodType  index_method_type,
                           IndexType  index_type,
                           bool unique_keys,
                           std::vector<std::string> key_column_names,
                           bool bootstrap = false );


  //===--------------------------------------------------------------------===//
  // Drop Object
  //===--------------------------------------------------------------------===//

  static bool DropTable(Oid table_oid);

  // TODO : DropIndex
  //static bool DropIndex(Oid index_oid);

};

} // namespace bridge
} // namespace peloton

