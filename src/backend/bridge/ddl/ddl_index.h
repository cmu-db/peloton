//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ddl_index.h
//
// Identification: src/backend/bridge/ddl/ddl_index.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/types.h"

#include "postgres.h"
#include "c.h"
#include "nodes/parsenodes.h"

#include <vector>

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL INDEX
//===--------------------------------------------------------------------===//

class IndexInfo;

class DDLIndex {
 public:
  DDLIndex(const DDLIndex &) = delete;
  DDLIndex &operator=(const DDLIndex &) = delete;
  DDLIndex(DDLIndex &&) = delete;
  DDLIndex &operator=(DDLIndex &&) = delete;

  static bool ExecIndexStmt(Node *parsetree,
                            std::vector<Node *> &parsetree_stack);

  static bool CreateIndex(IndexInfo index_info);

  // TODO
  // static bool AlterIndex( );

  // TODO : DropIndex
  // static bool DropIndex(Oid index_oid);

  // Create the indexes using indexinfos and add to the table
  static bool CreateIndexes(std::vector<IndexInfo> &index_infos);

  // Parse IndexStmt and return IndexInfo
  static IndexInfo *ConstructIndexInfoByParsingIndexStmt(IndexStmt *Istmt);
};

//===--------------------------------------------------------------------===//
// Index Info
//===--------------------------------------------------------------------===//

// NOTE: This is different from IndexMetadata
// We only keep track of column names, not the schema here.
// Used only internally.

class IndexInfo {
 public:
  IndexInfo(std::string index_name, oid_t index_oid, std::string table_name,
            IndexType method_type, IndexConstraintType type, bool unique_keys,
            std::vector<std::string> key_column_names)
      : index_name(index_name),
        index_oid(index_oid),
        table_name(table_name),
        method_type(method_type),
        constraint_type(type),
        unique_keys(unique_keys),
        key_column_names(key_column_names) {}

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  std::string GetIndexName() { return index_name; }

  oid_t GetOid() { return index_oid; }

  std::string GetTableName() { return table_name; }

  IndexType GetMethodType() { return method_type; }

  IndexConstraintType GetType() { return constraint_type; }

  bool IsUnique() { return unique_keys; }

  std::vector<std::string> GetKeyColumnNames() { return key_column_names; }

 private:
  std::string index_name;

  oid_t index_oid;

  std::string table_name;

  // Implementation type
  IndexType method_type = INDEX_TYPE_BTREE;

  IndexConstraintType constraint_type = INDEX_CONSTRAINT_TYPE_DEFAULT;

  bool unique_keys = false;

  std::vector<std::string> key_column_names;
};

}  // namespace bridge
}  // namespace peloton
