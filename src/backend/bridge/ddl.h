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

  // constraints 
  int* constraintType;
  char** conname;
} DDL_ColumnInfo;

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL Class 
//===--------------------------------------------------------------------===//


//TODO :: Move to other place?
class IndexInfo{
 public:
    // TODO :: Copy operator~
    //IndexInfo(const IndexInfo &) = delete;
    //IndexInfo& operator=(const IndexInfo &) = delete;
    //IndexInfo(IndexInfo &&) = delete;
    //IndexInfo& operator=(IndexInfo &&) = delete;
    IndexInfo(std::string index_name, 
              std::string table_name, 
              IndexMethodType method_type, 
              IndexType type, 
              bool unique_keys,  // TODO :: Remove..
              std::vector<std::string> key_column_names)
              : index_name(index_name),
                table_name(table_name),
                method_type(method_type),
                type(type),
                unique_keys(unique_keys),
                key_column_names(key_column_names) { }

    //===--------------------------------------------------------------------===//
    // IndexInfo accessors
    //===--------------------------------------------------------------------===//

    inline std::string GetIndexName(){
      return index_name;
    }

    inline std::string GetTableName(){
      return table_name;
    }

    inline IndexMethodType GetMethodType(){
      return method_type;
    }

    inline IndexType GetType(){
      return type;
    }

    inline bool IsUnique(){
      return unique_keys;
    }

    inline std::vector<std::string> GetKeyColumnNames(){
      return key_column_names;
    }

  private:
    std::string index_name = "";
    std::string table_name = "";
    IndexMethodType method_type = INDEX_METHOD_TYPE_BTREE_MULTIMAP; 
    IndexType type = INDEX_TYPE_NORMAL;
    bool unique_keys = false;
    std::vector<std::string> key_column_names;
};

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

  static std::vector<catalog::ColumnInfo> ConstructColumnInfoByParsingCreateStmt( CreateStmt* Cstmt, std::vector<std::string>& reference_table_names );
  static IndexInfo* ConstructIndexInfoByParsingIndexStmt( IndexStmt* Istmt );

  //===--------------------------------------------------------------------===//
  // Create Object
  //===--------------------------------------------------------------------===//

  static bool CreateTable( std::string table_name,
                            std::vector<catalog::ColumnInfo> column_infos,
                            catalog::Schema *schema = NULL);

  static bool CreateIndex(std::string index_name,
                          std::string table_name,
                          IndexMethodType  index_method_type,  /* name of access method (eg. btree) */
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

