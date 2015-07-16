/**
 * @brief Header for postgres ddl.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "backend/catalog/constraint.h"
#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/index/index_factory.h"
#include "backend/storage/backend_vm.h"
#include "backend/storage/table_factory.h"

#include "postgres.h"
#include "c.h"
#include "bridge/bridge.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "catalog/pg_am.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL
//===--------------------------------------------------------------------===//

class DDL {

 public:
  DDL(const DDL &) = delete;
  DDL& operator=(const DDL &) = delete;
  DDL(DDL &&) = delete;
  DDL& operator=(DDL &&) = delete;

  //===--------------------------------------------------------------------===//
  // Index Info
  //===--------------------------------------------------------------------===//

  // NOTE: This is different from IndexMetadata
  // We only keep track of column names, not the schema here.
  // Used only internally.

  class IndexInfo {
   public:
    IndexInfo(std::string index_name,
              oid_t index_oid,
              std::string table_name,
              IndexType method_type,
              IndexConstraintType type,
              bool unique_keys,
              std::vector<std::string> key_column_names)
   : index_name(index_name),
     index_oid(index_oid),
     table_name(table_name),
     method_type(method_type),
     constraint_type(type),
     unique_keys(unique_keys),
     key_column_names(key_column_names) { }

    //===--------------------------------------------------------------------===//
    // Accessors
    //===--------------------------------------------------------------------===//

    std::string GetIndexName(){
      return index_name;
    }

    oid_t GetIndexId(){
      return index_oid;
    }

    std::string GetTableName(){
      return table_name;
    }

    IndexType GetMethodType(){
      return method_type;
    }

    IndexConstraintType GetType(){
      return constraint_type;
    }

    bool IsUnique(){
      return unique_keys;
    }

    std::vector<std::string> GetKeyColumnNames(){
      return key_column_names;
    }

   private:
    std::string index_name;

    oid_t index_oid = INVALID_OID;

    std::string table_name;

    // Implementation type
    IndexType method_type = INDEX_TYPE_BTREE_MULTIMAP;

    IndexConstraintType constraint_type = INDEX_CONSTRAINT_TYPE_DEFAULT;

    bool unique_keys = false;

    std::vector<std::string> key_column_names;
  };

  //===--------------------------------------------------------------------===//
  // Create Object
  //===--------------------------------------------------------------------===//

  static bool CreateDatabase(Oid database_oid);

  static bool CreateTable(Oid relation_oid,
                          std::string table_name,
                          std::vector<catalog::Column> column_infos,
                          catalog::Schema *schema = NULL);

  static bool CreateIndex( IndexInfo index_info );

  //===--------------------------------------------------------------------===//
  // Alter Object
  //===--------------------------------------------------------------------===//

  // TODO
  //static bool AlterDatabase( );

  static bool AlterTable(Oid relation_oid,
                         AlterTableStmt* Astmt);

  // TODO
  //static bool AlterIndex( );


  //===--------------------------------------------------------------------===//
  // Drop Object
  //===--------------------------------------------------------------------===//

  // TODO 
  static bool DropDatabase(Oid database_oid);

  static bool DropTable(Oid table_oid);

  // TODO : DropIndex
  //static bool DropIndex(Oid index_oid);

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  static void ProcessUtility(Node *parsetree,
                             const char *queryString);

  // Parse IndexStmt and construct ColumnInfo and ReferenceTableInfos
  static void ParsingCreateStmt(CreateStmt* Cstmt,
                                std::vector<catalog::Column>& column_infos,
                                std::vector<catalog::ForeignKey>& reference_table_infos
  );

  // Parse IndexStmt and return IndexInfo
  static IndexInfo* ConstructIndexInfoByParsingIndexStmt(IndexStmt* Istmt);

  // Set reference tables to the table based on given relation oid
  static bool SetReferenceTables(std::vector<catalog::ForeignKey>& reference_table_infos,
                                 oid_t relation_oid);

  // Create the indexes using indexinfos and add to the table
  static bool CreateIndexes(std::vector<IndexInfo> index_infos);

  //Add the constraint to the table
  static bool AddConstraint(Oid relation_oid,
                            Constraint* constraint);

};

} // namespace bridge
} // namespace peloton

