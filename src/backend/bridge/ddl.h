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


namespace peloton {
namespace bridge {

class IndexInfo;

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
  // Create Object
  //===--------------------------------------------------------------------===//

  static bool CreateDatabase( Oid database_oid );

  static bool CreateTable( Oid relation_oid,
                           std::string table_name,
                           std::vector<catalog::ColumnInfo> column_infos,
                           catalog::Schema *schema = NULL );

  static bool CreateIndex( std::string index_name,
                           std::string table_name,
                           IndexMethodType  index_method_type,  /* name of access method (eg. btree) */
                           IndexType  index_type,
                           bool unique_keys,
                           std::vector<std::string> key_column_names,
                           Oid table_oid = INVALID_OID );

  //===--------------------------------------------------------------------===//
  // Alter Object
  //===--------------------------------------------------------------------===//

  // TODO
  //static bool AlterDatabase( );

  static bool AlterTable( Oid relation_oid,
                          AlterTableStmt* Astmt );

  // TODO
  //static bool AlterIndex( );


  //===--------------------------------------------------------------------===//
  // Drop Object
  //===--------------------------------------------------------------------===//

  // TODO 
  static bool DropDatabase( Oid database_oid );

  static bool DropTable( Oid table_oid );

  // TODO : DropIndex
  //static bool DropIndex(Oid index_oid);


  //===--------------------------------------------------------------------===//
  // Misc. 
  //===--------------------------------------------------------------------===//

  static void ProcessUtility( Node *parsetree,
                              const char *queryString );

  // Parse IndexStmt and construct ColumnInfo and ReferenceTableInfos
  static void ParsingCreateStmt( CreateStmt* Cstmt,
                                 std::vector<catalog::ColumnInfo>& column_infos,
                                 std::vector<catalog::ReferenceTableInfo>& reference_table_infos
                                 );

  // Parse IndexStmt and return IndexInfo
  static IndexInfo* ConstructIndexInfoByParsingIndexStmt( IndexStmt* Istmt );

  // Set reference tables to the table based on given relation oid
  static bool SetReferenceTables( std::vector<catalog::ReferenceTableInfo>& reference_table_infos,
                                  oid_t relation_oid );

  // Create the indexes using indexinfos and add to the table
  static bool CreateIndexesWithIndexInfos( std::vector<IndexInfo> index_infos,
                                           oid_t relation_oid = INVALID_OID );

  //Add the constraint to the table
  static bool AddConstraint( Oid relation_oid, 
                             Constraint* constraint );

};


//===--------------------------------------------------------------------===//
// IndexInfo Class 
//===--------------------------------------------------------------------===//

class IndexInfo{
 public:
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
  // Accessors
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

} // namespace bridge
} // namespace peloton

