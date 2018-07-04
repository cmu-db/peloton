//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constraint_catalog.h
//
// Identification: src/include/catalog/constraint_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_constraint
//
// Schema: (column offset: column_name)
// 0: constraint_oid (pkey)
// 1: constraint_name (name of the constraint, not unique!)
// 2: constraint_type (type of the constraint)
// 3: table_oid (table created for the constraint)
// 4: column_ids (list of column id related to the constraint)
// 5: index_oid (index created for the constraint)
// 6: fk_sink_table_oid (for FOREIGN KEY)
// 7: fk_sink_col_ids (for FOREIGN KEY)
// 8: fk_update_action (for FOREIGN KEY)
// 9: fk_delete_action (for FOREIGN KEY)
// 10: default_value (for DEFAULT)
// 11: check_cmd (for CHECK)
// 12: check_exp (for CHECK)
//
// Indexes: (index offset: indexed columns)
// 0: constraint_oid (unique & primary key)
// 1: table_oid (non-unique)
//
// Note: Exclusive constraint is not supported yet
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"
#include "executor/logical_tile.h"

namespace peloton {
namespace catalog {

class Constraint;

class ConstraintCatalogEntry {
  friend class ConstraintCatalog;

 public:
  ConstraintCatalogEntry(executor::LogicalTile *tile, int tupleId = 0);

  inline oid_t GetConstraintOid() { return constraint_oid_; }
  inline const std::string &GetConstraintName() { return constraint_name_; }
  inline ConstraintType GetConstraintType() { return constraint_type_; }
  inline oid_t GetTableOid() { return table_oid_; }
  inline const std::vector<oid_t> &GetColumnIds() { return column_ids_; }
  inline oid_t GetIndexOid() { return index_oid_; }
  inline oid_t GetFKSinkTableOid() { return fk_sink_table_oid_; }
  inline const std::vector<oid_t> &GetFKSinkColumnIds() {
    return fk_sink_col_ids_;
  }
  inline FKConstrActionType GetFKUpdateAction() { return fk_update_action_; }
  inline FKConstrActionType GetFKDeleteAction() { return fk_delete_action_; }
  inline const std::pair<ExpressionType, type::Value> &GetCheckExp() {
    return check_exp_;
  }

 private:
  // member variables
  oid_t constraint_oid_;
  std::string constraint_name_;
  ConstraintType constraint_type_;
  oid_t table_oid_;
  std::vector<oid_t> column_ids_;
  oid_t index_oid_;
  oid_t fk_sink_table_oid_;
  std::vector<oid_t> fk_sink_col_ids_;
  FKConstrActionType fk_update_action_;
  FKConstrActionType fk_delete_action_;
  std::pair<ExpressionType, type::Value> check_exp_;
};

class ConstraintCatalog : public AbstractCatalog {
  friend class ConstraintCatalogEntry;
  friend class TableCatalogEntry;
  friend class Catalog;

 public:
  ConstraintCatalog(concurrency::TransactionContext *txn,
                    storage::Database *pg_catalog,
                    type::AbstractPool *pool);

  ~ConstraintCatalog();

  inline oid_t GetNextOid() { return oid_++ | CONSTRAINT_OID_MASK; }

  void UpdateOid(oid_t add_value) { oid_ += add_value; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertConstraint(concurrency::TransactionContext *txn,
                        const std::shared_ptr<Constraint> constraint,
                        type::AbstractPool *pool);

  bool DeleteConstraints(concurrency::TransactionContext *txn,
                         oid_t table_oid);

  bool DeleteConstraint(concurrency::TransactionContext *txn,
                        oid_t table_oid,
                        oid_t constraint_oid);

 private:
  //===--------------------------------------------------------------------===//
  // Read Related API(only called within table catalog object)
  //===--------------------------------------------------------------------===//
  const std::unordered_map<oid_t, std::shared_ptr<ConstraintCatalogEntry>>
  GetConstraintCatalogEntries(concurrency::TransactionContext *txn,
                              oid_t table_oid);

  const std::shared_ptr<ConstraintCatalogEntry>
  GetConstraintCatalogEntry(concurrency::TransactionContext *txn,
                            oid_t table_oid,
                            oid_t constraint_oid);

  std::unique_ptr<catalog::Schema> InitializeSchema();

  enum ColumnId {
    CONSTRAINT_OID = 0,
    CONSTRAINT_NAME = 1,
    CONSTRAINT_TYPE = 2,
    TABLE_OID = 3,
    COLUMN_IDS = 4,
    INDEX_OID = 5,
    FK_SINK_TABLE_OID = 6,
    FK_SINK_COL_IDS = 7,
    FK_UPDATE_ACTION = 8,
    FK_DELETE_ACTION = 9,
    CHECK_EXP_SRC = 10,
    CHECK_EXP_BIN = 11,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids_ = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_TABLE_OID = 1,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
