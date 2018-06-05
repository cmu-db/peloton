//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_catalog.cpp
//
// Identification: src/catalog/column_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/constraint_catalog.h"

#include <sstream>
#include <memory>

#include "catalog/catalog.h"
#include "catalog/foreign_key.h"
#include "catalog/system_catalogs.h"
#include "catalog/table_catalog.h"
#include "concurrency/transaction_context.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/storage_manager.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

ConstraintCatalogObject::ConstraintCatalogObject(executor::LogicalTile *tile,
                                         int tupleId)
    : constraint_oid(tile->GetValue(tupleId,
    		ConstraintCatalog::ColumnId::CONSTRAINT_OID).GetAs<oid_t>()),
			constraint_name(tile->GetValue(tupleId,
				ConstraintCatalog::ColumnId::CONSTRAINT_NAME).ToString()),
			constraint_type(StringToConstraintType(tile->GetValue(tupleId,
					ConstraintCatalog::ColumnId::CONSTRAINT_TYPE).ToString())),
    	table_oid(tile->GetValue(tupleId, ConstraintCatalog::ColumnId::TABLE_OID)
                    .GetAs<oid_t>()),
			index_oid(tile->GetValue(tupleId, ConstraintCatalog::ColumnId::INDEX_OID)
			              .GetAs<oid_t>()) {
  std::string src_column_ids_str =
      tile->GetValue(tupleId, ConstraintCatalog::ColumnId::COLUMN_IDS)
          .ToString();
  std::stringstream src_ss(src_column_ids_str.c_str());
  std::string src_tok;
  while (std::getline(src_ss, src_tok, ' ')) {
  	column_ids.push_back(std::stoi(src_tok));
  }

  // create values by type of constraint
  switch(constraint_type) {
  case ConstraintType::PRIMARY:
  case ConstraintType::UNIQUE:
  case ConstraintType::NOTNULL:
  case ConstraintType::NOT_NULL:
  	// nothing to do more
  	break;

  case ConstraintType::FOREIGN: {
  	fk_sink_table_oid = tile->GetValue(tupleId,
    		ConstraintCatalog::ColumnId::FK_SINK_TABLE_OID).GetAs<oid_t>();
		std::string snk_column_ids_str = tile->GetValue(tupleId,
				ConstraintCatalog::ColumnId::FK_SINK_COL_IDS).ToString();
		std::stringstream snk_ss(snk_column_ids_str.c_str());
		std::string snk_tok;
		while (std::getline(snk_ss, snk_tok, ' ')) {
			fk_sink_col_ids.push_back(std::stoi(snk_tok));
		}
  	fk_update_action = StringToFKConstrActionType(tile->GetValue(tupleId,
				ConstraintCatalog::ColumnId::FK_UPDATE_ACTION).ToString());
  	fk_delete_action = StringToFKConstrActionType(tile->GetValue(tupleId,
				ConstraintCatalog::ColumnId::FK_DELETE_ACTION).ToString());
  	break;
  }

  case ConstraintType::DEFAULT:
  	default_value = tile->GetValue(tupleId,
				ConstraintCatalog::ColumnId::DEFAULT_VALUE).ToString();
  	break;

  case ConstraintType::CHECK:
  	check_cmd = tile->GetValue(tupleId,
				ConstraintCatalog::ColumnId::CHECK_CMD).ToString();
		check_exp = tile->GetValue(tupleId,
				ConstraintCatalog::ColumnId::CHECK_EXP).ToString();
  	break;

  case ConstraintType::EXCLUSION:
  default:
  	break;
  }
}

ConstraintCatalog::ConstraintCatalog(storage::Database *pg_catalog,
		UNUSED_ATTRIBUTE type::AbstractPool *pool,
		UNUSED_ATTRIBUTE concurrency::TransactionContext *txn)
    : AbstractCatalog(CONSTRAINT_CATALOG_OID, CONSTRAINT_CATALOG_NAME,
                      InitializeSchema().release(), pg_catalog) {
  // Add indexes for pg_constraint
  AddIndex({ColumnId::CONSTRAINT_OID},
           CONSTRAINT_CATALOG_PKEY_OID, CONSTRAINT_CATALOG_NAME "_pkey",
           IndexConstraintType::PRIMARY_KEY);
  AddIndex({ColumnId::TABLE_OID}, CONSTRAINT_CATALOG_SKEY0_OID,
  		     CONSTRAINT_CATALOG_NAME "_skey0", IndexConstraintType::DEFAULT);
}

ConstraintCatalog::~ConstraintCatalog() {}

/*@brief   private function for initialize schema of pg_constraint
 * @return  unqiue pointer to schema
 */
std::unique_ptr<catalog::Schema> ConstraintCatalog::InitializeSchema() {
  const std::string primary_key_constraint_name = "primary_key";
  const std::string not_null_constraint_name = "not_null";

  auto constraint_oid_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "constraint_oid", true);
  constraint_oid_column.AddConstraint(std::make_shared<Constraint>(
      ConstraintType::PRIMARY, primary_key_constraint_name,
			CONSTRAINT_CATALOG_PKEY_OID));
  constraint_oid_column.AddConstraint(std::make_shared<Constraint>(
  		ConstraintType::NOTNULL, not_null_constraint_name));

  auto constraint_name_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size, "constraint_name_", false);
  constraint_name_column.AddConstraint(std::make_shared<Constraint>(
  		ConstraintType::NOTNULL, not_null_constraint_name));

  auto constraint_type_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size, "constraint_type", false);
  constraint_type_column.AddConstraint(std::make_shared<Constraint>(
  		ConstraintType::NOTNULL, not_null_constraint_name));

  auto table_oid_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "table_oid", true);
  table_oid_column.AddConstraint(std::make_shared<Constraint>(
  		ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_ids_column = catalog::Column(
      type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
      "column_ids", false);
  column_ids_column.AddConstraint(std::make_shared<Constraint>(
  		ConstraintType::NOTNULL, not_null_constraint_name));

  auto index_oid_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "index_oid", true);
  index_oid_column.AddConstraint(std::make_shared<Constraint>(
  		ConstraintType::NOTNULL, not_null_constraint_name));

  auto fk_sink_table_oid_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "fk_sink_table_oid", true);

  auto fk_sink_col_ids_column = catalog::Column(
      type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
      "fk_sink_col_ids", false);

  auto fk_update_action_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size, "fk_update_action", false);

  auto fk_delete_action_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size, "fk_delete_action", false);

  auto default_value_column = catalog::Column(
      type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
      "default_value", false);

  auto check_cmd_column = catalog::Column(
      type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
      "check_cmd", false);

  auto check_exp_column = catalog::Column(
      type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
      "check_exp", false);

  std::unique_ptr<catalog::Schema> column_catalog_schema(new catalog::Schema(
      {constraint_oid_column, constraint_name_column, constraint_type_column,
  	   table_oid_column, column_ids_column, index_oid_column,
			 fk_sink_table_oid_column, fk_sink_col_ids_column,
			 fk_update_action_column, fk_delete_action_column, default_value_column,
			 check_cmd_column, check_exp_column}));

  return column_catalog_schema;
}

/*@brief    Insert a constraint into the pg_constraint table
 *          This targets PRIMARY KEY, UNIQUE, DEFAULt, CHECK or NOT NULL constraint
 * @param   table_oid  oid of the table related to this constraint
 * @param   column_ids  vector of oids of column related to this constraint
 * @param   constraint  to be inserted into pg_constraint
 * @param   pool  to allocate memory for the column_map column.
 * @param   txn  TransactionContext for adding the constraint.
 * @return  true on success.
 */
bool ConstraintCatalog::InsertConstraint(oid_t table_oid,
		const std::vector<oid_t> &column_ids,
		const std::shared_ptr<Constraint> constraint,
		type::AbstractPool *pool, concurrency::TransactionContext *txn) {
	// Check usage
	PELOTON_ASSERT(constraint->GetType() != ConstraintType::FOREIGN);

  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  // Common information of constraint
  auto val0 = type::ValueFactory::GetIntegerValue(constraint->GetConstraintOid());
  auto val1 = type::ValueFactory::GetVarcharValue(constraint->GetName(), nullptr);
  auto val2 =	type::ValueFactory::GetVarcharValue(
  		ConstraintTypeToString(constraint->GetType()), nullptr);
  auto val3 = type::ValueFactory::GetIntegerValue(table_oid);
  std::stringstream ss;
  for (auto column_oid : column_ids) ss << std::to_string(column_oid) << " ";
  auto val4 =	type::ValueFactory::GetVarcharValue(ss.str(), nullptr);
  auto val5 = type::ValueFactory::GetIntegerValue(constraint->GetIndexOid());

  tuple->SetValue(ColumnId::CONSTRAINT_OID, val0, pool);
  tuple->SetValue(ColumnId::CONSTRAINT_NAME, val1, pool);
  tuple->SetValue(ColumnId::CONSTRAINT_TYPE, val2, pool);
  tuple->SetValue(ColumnId::TABLE_OID, val3, pool);
  tuple->SetValue(ColumnId::COLUMN_IDS, val4, pool);
  tuple->SetValue(ColumnId::INDEX_OID, val5, pool);

  // create values by type of constraint
  switch(constraint->GetType()) {
  case ConstraintType::PRIMARY:
  case ConstraintType::UNIQUE:
  	// nothing to do more
  	// need to set a valid index oid
  	PELOTON_ASSERT(constraint->GetIndexOid() != INVALID_OID);
  	break;

  case ConstraintType::NOTNULL:
  case ConstraintType::NOT_NULL:
  	// nothing to do more
  	break;

  case ConstraintType::DEFAULT: {
  	// set value of default value
  	PELOTON_ASSERT(column_ids.size() == 1);
  	auto column = storage::StorageManager::GetInstance()
  	    ->GetTableWithOid(database_oid, table_oid)->GetSchema()
				->GetColumn(column_ids.at(0));
  	char default_value_str[column.GetLength()];
  	constraint->getDefaultValue()->SerializeTo(default_value_str, column.IsInlined(),
  			                                      pool);
  	auto val6 = type::ValueFactory::GetVarcharValue(default_value_str, nullptr);

    tuple->SetValue(ColumnId::DEFAULT_VALUE, val6, pool);
  	break;
  }

  case ConstraintType::CHECK: {
  	// set value of check expression
  	PELOTON_ASSERT(column_ids.size() == 1);
  	auto val6 =
  			type::ValueFactory::GetVarcharValue(constraint->GetCheckCmd(), nullptr);
  	auto exp = constraint->GetCheckExpression();
  	auto column = storage::StorageManager::GetInstance()
  	    ->GetTableWithOid(database_oid, table_oid)->GetSchema()
				->GetColumn(column_ids.at(0));
  	char exp_value_str[column.GetLength()];
  	exp.second.SerializeTo(exp_value_str, column.IsInlined(), pool);
  	std::stringstream exp_ss;
  	exp_ss << ExpressionTypeToString(exp.first) << " " << exp_value_str;
    auto val7 = type::ValueFactory::GetVarcharValue(exp_ss.str(), nullptr);

    tuple->SetValue(ColumnId::CHECK_CMD, val6, pool);
    tuple->SetValue(ColumnId::CHECK_EXP, val7, pool);
  	break;
  }

  case ConstraintType::FOREIGN:
  case ConstraintType::EXCLUSION:
  default:
  	// unexpected constraint type
  	throw CatalogException("Unexpected constraint type '" +
  			                   ConstraintTypeToString(constraint->GetType()) +
  			                   "' appears in insertion into pg_constraint ");
  	return false;
  }


  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

/*@brief    Insert a constraint into the pg_constraint table
 *          This targets only FOREIGN KEY constraint
 * @param   table_oid  oid of the table related to this constraint
 * @param   column_ids  vector of oids of column related to this constraint
 * @param   constraint  to be inserted into pg_constraint
 * @param   foreign_key  foreign key information as constraint
 * @param   pool  to allocate memory for the column_map column.
 * @param   txn  TransactionContext for adding the constraint.
 * @return  true on success.
 */
bool ConstraintCatalog::InsertConstraint(oid_t table_oid,
		const std::vector<oid_t> &column_ids,
		const std::shared_ptr<Constraint> constraint,
		const ForeignKey &foreign_key, type::AbstractPool *pool,
    concurrency::TransactionContext *txn) {
	// Check usage
	PELOTON_ASSERT(constraint->GetType() == ConstraintType::FOREIGN);
	PELOTON_ASSERT(constraint->GetIndexOid() != INVALID_OID);

  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(constraint->GetConstraintOid());
  auto val1 = type::ValueFactory::GetVarcharValue(constraint->GetName(), nullptr);
  auto val2 =
  		type::ValueFactory::GetVarcharValue(ConstraintTypeToString(constraint->GetType()),
  				                                nullptr);
  auto val3 = type::ValueFactory::GetIntegerValue(table_oid);
  std::stringstream src_ss;
  for (auto column_oid : column_ids) src_ss << std::to_string(column_oid) << " ";
  auto val4 =	type::ValueFactory::GetVarcharValue(src_ss.str(), nullptr);
  auto val5 = type::ValueFactory::GetIntegerValue(constraint->GetIndexOid());
  auto val6 = type::ValueFactory::GetIntegerValue(foreign_key.GetSinkTableOid());
  std::stringstream snk_ss;
  for (auto column_oid : foreign_key.GetSinkColumnIds())
  	snk_ss << std::to_string(column_oid) << " ";
  auto val7 =	type::ValueFactory::GetVarcharValue(snk_ss.str(), nullptr);
  auto val8 =	type::ValueFactory::GetVarcharValue(
  				FKConstrActionTypeToString(foreign_key.GetUpdateAction()), nullptr);
  auto val9 =	type::ValueFactory::GetVarcharValue(
  				FKConstrActionTypeToString(foreign_key.GetDeleteAction()), nullptr);

  tuple->SetValue(ColumnId::CONSTRAINT_OID, val0, pool);
  tuple->SetValue(ColumnId::CONSTRAINT_NAME, val1, pool);
  tuple->SetValue(ColumnId::CONSTRAINT_TYPE, val2, pool);
  tuple->SetValue(ColumnId::TABLE_OID, val3, pool);
  tuple->SetValue(ColumnId::COLUMN_IDS, val4, pool);
  tuple->SetValue(ColumnId::INDEX_OID, val5, pool);
  tuple->SetValue(ColumnId::FK_SINK_TABLE_OID, val6, pool);
  tuple->SetValue(ColumnId::FK_SINK_COL_IDS, val7, pool);
  tuple->SetValue(ColumnId::FK_UPDATE_ACTION, val8, pool);
  tuple->SetValue(ColumnId::FK_DELETE_ACTION, val9, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

/* @brief   delete all constraint records from the same table
 *          this function is useful when calling DropTable
 * @param   table_oid
 * @param   txn  TransactionContext
 * @return  a vector of table oid
 */
bool ConstraintCatalog::DeleteConstraints(oid_t table_oid,
                                  concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::SKEY_TABLE_OID;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  // delete columns from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableObject(table_oid, txn);
  table_object->EvictAllConstraintObjects();

  return DeleteWithIndexScan(index_offset, values, txn);
}

/** @brief      Delete a constraint from the pg_constraint table.
 *  @param      table_oid  oid of the table to which the old constraint belongs.
 *  @param      constraint_oid  oid of the constraint to be deleted.
 *  @param      txn TransactionContext for deleting the constraint.
 *  @return     true on success.
 */
bool ConstraintCatalog::DeleteConstraint(oid_t table_oid,
		                             oid_t constraint_oid,
                                 concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of constraint_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(constraint_oid).Copy());

  // delete column from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableObject(table_oid, txn);
  table_object->EvictConstraintObject(constraint_oid);

  return DeleteWithIndexScan(index_offset, values, txn);
}

/** @brief      Get all constraint objects correponding to a table
 *              from the pg_constraint.
 *  @param      table_oid  oid of the table to fetch all constraints.
 *  @param      txn TransactionContext for getting the constraints.
 *  @return     unordered_map containing a constraint_oid ->
 *              constraint object mapping.
 */
const std::unordered_map<oid_t, std::shared_ptr<ConstraintCatalogObject>>
ConstraintCatalog::GetConstraintObjects(oid_t table_oid,
                                concurrency::TransactionContext *txn) {
  // try get from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableObject(table_oid, txn);
  PELOTON_ASSERT(table_object && table_object->GetTableOid() == table_oid);

  auto constraint_objects = table_object->GetConstraintObjects(true);
  if (constraint_objects.size() != 0) return constraint_objects;

  // cache miss, get from pg_attribute
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::SKEY_TABLE_OID;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  for (auto &tile : (*result_tiles)) {
    for (auto tuple_id : *tile) {
      auto constraint_object =
          std::make_shared<ConstraintCatalogObject>(tile.get(), tuple_id);
      table_object->InsertConstraintObject(constraint_object);
    }
  }

  return table_object->GetConstraintObjects();
}

/** @brief      Get the constraint object by constraint_oid from
 *              the pg_constraint.
 *  @param      table_oid  oid of the table to fetch the constraint.
 *  @param      constraint_oid  oid of the constraint being queried.
 *  @param      txn TransactionContext for getting the constraint.
 *  @return     shared_ptr constraint object to the constraint_oid if found.
 *              nullptr otherwise.
 */
const std::shared_ptr<ConstraintCatalogObject>
ConstraintCatalog::GetConstraintObject(oid_t table_oid, oid_t constraint_oid,
		                                   concurrency::TransactionContext *txn) {
  // try get from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableObject(table_oid, txn);
  PELOTON_ASSERT(table_object && table_object->GetTableOid() == table_oid);

  auto constraint_object =
  		table_object->GetConstraintObject(constraint_oid, true);
  if (constraint_object != nullptr) return constraint_object;

  // cache miss, get from pg_attribute
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(constraint_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto constraint_object =
        std::make_shared<ConstraintCatalogObject>((*result_tiles)[0].get());
    table_object->InsertConstraintObject(constraint_object);
    return constraint_object;
  }

  return nullptr;
}

}  // namespace catalog
}  // namespace peloton
