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
class ForeignKey;

class ConstraintCatalogObject {
	friend class ConstraintCatalog;

 public:
  ConstraintCatalogObject(executor::LogicalTile *tile, int tupleId = 0);

  inline oid_t GetConstraintOid() { return constraint_oid; }
  inline const std::string &GetConstraintName() { return constraint_name; }
  inline ConstraintType GetConstraintType() { return constraint_type; }
  inline oid_t GetTableOid() { return table_oid; }
  inline const std::vector<oid_t> &GetColumnIds() { return column_ids; }
  inline oid_t GetIndexOid() { return index_oid; }
  inline oid_t GetFKSinkTableOid() { return fk_sink_table_oid; }
  inline const std::vector<oid_t> &GetFKSinkColumnIds() { return fk_sink_col_ids; }
  inline FKConstrActionType GetFKUpdateAction() { return fk_update_action; }
  inline FKConstrActionType GetFKDeleteAction() { return fk_delete_action; }
  inline const std::string &GetDefaultValue() { return default_value; }
  inline const std::string &GetCheckCmd() { return check_cmd; }
  inline const std::string &GetCheckExp() { return check_exp; }

 private:
  // member variables
  oid_t constraint_oid;
  std::string constraint_name;
  ConstraintType constraint_type;
  oid_t table_oid;
  std::vector<oid_t> column_ids;
  oid_t index_oid;
	oid_t fk_sink_table_oid;
  std::vector<oid_t> fk_sink_col_ids;
	FKConstrActionType fk_update_action;
	FKConstrActionType fk_delete_action;
	std::string default_value; // original : std::shared_ptr<type::Value>
	std::string check_cmd;
	std::string check_exp; // original : std::pair<ExpressionType, type::Value>
};

class ConstraintCatalog : public AbstractCatalog {
  friend class ConstraintCatalogObject;
  friend class TableCatalogObject;
  friend class Catalog;

 public:
  ConstraintCatalog(storage::Database *pg_catalog, type::AbstractPool *pool,
                concurrency::TransactionContext *txn);

  ~ConstraintCatalog();

  inline oid_t GetNextOid() { return oid_++ | CONSTRAINT_OID_MASK; }

  void UpdateOid(oid_t add_value) { oid_ += add_value; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  // Basic insert for primary key, unique, check, default or not null constraint
  bool InsertConstraint(oid_t table_oid, const std::vector<oid_t> &column_ids,
                        const std::shared_ptr<Constraint> constraint,
												type::AbstractPool *pool,
                        concurrency::TransactionContext *txn);

  // insert for foreign key constraint
  bool InsertConstraint(oid_t table_oid, const std::vector<oid_t> &column_ids,
                        const std::shared_ptr<Constraint> constraint,
												const ForeignKey &foreign_key, type::AbstractPool *pool,
                        concurrency::TransactionContext *txn);

  bool DeleteConstraints(oid_t table_oid, concurrency::TransactionContext *txn);

  bool DeleteConstraint(oid_t table_oid, oid_t constraint_oid,
  		                  concurrency::TransactionContext *txn);

 private:
  //===--------------------------------------------------------------------===//
  // Read Related API(only called within table catalog object)
  //===--------------------------------------------------------------------===//
  const std::unordered_map<oid_t, std::shared_ptr<ConstraintCatalogObject>>
  GetConstraintObjects(oid_t table_oid, concurrency::TransactionContext *txn);

  const std::shared_ptr<ConstraintCatalogObject>
  GetConstraintObject(oid_t table_oid, oid_t constraint_oid,
  		                concurrency::TransactionContext *txn);

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
    DEFAULT_VALUE = 10,
    CHECK_CMD = 11,
    CHECK_EXP = 12,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_TABLE_OID = 1,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
