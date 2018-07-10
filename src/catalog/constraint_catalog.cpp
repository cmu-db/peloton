//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constraint_catalog.cpp
//
// Identification: src/catalog/constraint_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/constraint_catalog.h"

#include <sstream>
#include <memory>

#include "catalog/catalog.h"
#include "catalog/system_catalogs.h"
#include "catalog/table_catalog.h"
#include "concurrency/transaction_context.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "storage/storage_manager.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

ConstraintCatalogEntry::ConstraintCatalogEntry(executor::LogicalTile *tile,
                                                 int tupleId)
    : constraint_oid_(
          tile->GetValue(tupleId, ConstraintCatalog::ColumnId::CONSTRAINT_OID)
              .GetAs<oid_t>()),
      constraint_name_(
          tile->GetValue(tupleId, ConstraintCatalog::ColumnId::CONSTRAINT_NAME)
              .ToString()),
      constraint_type_(StringToConstraintType(
          tile->GetValue(tupleId, ConstraintCatalog::ColumnId::CONSTRAINT_TYPE)
              .ToString())),
      table_oid_(tile->GetValue(tupleId, ConstraintCatalog::ColumnId::TABLE_OID)
                    .GetAs<oid_t>()),
      index_oid_(tile->GetValue(tupleId, ConstraintCatalog::ColumnId::INDEX_OID)
                    .GetAs<oid_t>()) {
  std::string src_column_ids_str =
      tile->GetValue(tupleId, ConstraintCatalog::ColumnId::COLUMN_IDS)
          .ToString();
  std::stringstream src_ss(src_column_ids_str.c_str());
  std::string src_tok;
  while (std::getline(src_ss, src_tok, ' ')) {
    column_ids_.push_back(std::stoi(src_tok));
  }

  // create values by type of constraint
  switch (constraint_type_) {
    case ConstraintType::PRIMARY:
    case ConstraintType::UNIQUE:
      // nothing to do more
      break;

    case ConstraintType::FOREIGN: {
      fk_sink_table_oid_ =
          tile->GetValue(tupleId,
                         ConstraintCatalog::ColumnId::FK_SINK_TABLE_OID)
              .GetAs<oid_t>();
      std::string snk_column_ids_str =
          tile->GetValue(tupleId, ConstraintCatalog::ColumnId::FK_SINK_COL_IDS)
              .ToString();
      std::stringstream snk_ss(snk_column_ids_str.c_str());
      std::string snk_tok;
      while (std::getline(snk_ss, snk_tok, ' ')) {
        fk_sink_col_ids_.push_back(std::stoi(snk_tok));
      }
      fk_update_action_ = StringToFKConstrActionType(
          tile->GetValue(tupleId, ConstraintCatalog::ColumnId::FK_UPDATE_ACTION)
              .ToString());
      fk_delete_action_ = StringToFKConstrActionType(
          tile->GetValue(tupleId, ConstraintCatalog::ColumnId::FK_DELETE_ACTION)
              .ToString());
      break;
    }

    case ConstraintType::CHECK: {
      auto dv_val =
          tile->GetValue(tupleId, ConstraintCatalog::ColumnId::CHECK_EXP_BIN);
      CopySerializeInput input_buffer(dv_val.GetData(), dv_val.GetLength());
      ExpressionType exp_type = (ExpressionType)input_buffer.ReadInt();
      type::TypeId value_type = (type::TypeId)input_buffer.ReadInt();
      auto exp_value = type::Value::DeserializeFrom(input_buffer, value_type);
      check_exp_ = std::make_pair(exp_type, exp_value);
      break;
    }

    case ConstraintType::EXCLUSION:
    default:
      LOG_ERROR("Invalid Constraint type from pg_constraint: %s",
                ConstraintTypeToString(constraint_type_).c_str());
      break;
  }
}

ConstraintCatalog::ConstraintCatalog(
    UNUSED_ATTRIBUTE concurrency::TransactionContext *txn,
    storage::Database *pg_catalog, UNUSED_ATTRIBUTE type::AbstractPool *pool)
    : AbstractCatalog(pg_catalog,
                      InitializeSchema().release(),
                      CONSTRAINT_CATALOG_OID,
                      CONSTRAINT_CATALOG_NAME) {
  // Add indexes for pg_constraint
  AddIndex(CONSTRAINT_CATALOG_NAME "_pkey",
           CONSTRAINT_CATALOG_PKEY_OID,
           {ColumnId::CONSTRAINT_OID},
           IndexConstraintType::PRIMARY_KEY);
  AddIndex(CONSTRAINT_CATALOG_NAME "_skey0",
           CONSTRAINT_CATALOG_SKEY0_OID,
           {ColumnId::TABLE_OID},
           IndexConstraintType::DEFAULT);
}

ConstraintCatalog::~ConstraintCatalog() {}

/*@brief   private function for initialize schema of pg_constraint
 * @return  unqiue pointer to schema
 */
std::unique_ptr<catalog::Schema> ConstraintCatalog::InitializeSchema() {
  auto constraint_oid_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "constraint_oid", true);
  constraint_oid_column.SetNotNull();

  auto constraint_name_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size_, "constraint_name_", false);
  constraint_name_column.SetNotNull();

  auto constraint_type_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size_, "constraint_type", false);
  constraint_type_column.SetNotNull();

  auto table_oid_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "table_oid", true);
  table_oid_column.SetNotNull();

  auto column_ids_column = catalog::Column(
      type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
      "column_ids", false);
  column_ids_column.SetNotNull();

  auto index_oid_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "index_oid", true);
  index_oid_column.SetNotNull();

  auto fk_sink_table_oid_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "fk_sink_table_oid", true);

  auto fk_sink_col_ids_column = catalog::Column(
      type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
      "fk_sink_col_ids", false);

  auto fk_update_action_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size_, "fk_update_action", false);

  auto fk_delete_action_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size_, "fk_delete_action", false);

  auto check_exp_src_column = catalog::Column(
      type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
      "check_exp_src", false);

  auto check_exp_bin_column = catalog::Column(
      type::TypeId::VARBINARY, type::Type::GetTypeSize(type::TypeId::VARBINARY),
      "check_exp_bin", false);

  std::unique_ptr<catalog::Schema> constraint_catalog_schema(
      new catalog::Schema({constraint_oid_column, constraint_name_column,
                           constraint_type_column, table_oid_column,
                           column_ids_column, index_oid_column,
                           fk_sink_table_oid_column, fk_sink_col_ids_column,
                           fk_update_action_column, fk_delete_action_column,
                           check_exp_src_column, check_exp_bin_column}));

  constraint_catalog_schema->AddConstraint(std::make_shared<Constraint>(
      CONSTRAINT_CATALOG_CON_PKEY_OID, ConstraintType::PRIMARY, "con_primary",
      CONSTRAINT_CATALOG_OID, std::vector<oid_t>{ColumnId::CONSTRAINT_OID},
      CONSTRAINT_CATALOG_PKEY_OID));

  return constraint_catalog_schema;
}

/*@brief    Insert a constraint into the pg_constraint table
 *          This targets PRIMARY KEY, FOREIGN KEY, UNIQUE or CHECK constraint
 * @param   txn  TransactionContext for adding the constraint.
 * @param   constraint  to be inserted into pg_constraint
 * @param   pool  to allocate memory for the column_map column.
 * @return  true on success.
 */
bool ConstraintCatalog::InsertConstraint(concurrency::TransactionContext *txn,
                                         const std::shared_ptr<Constraint> constraint,
                                         type::AbstractPool *pool) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  // Common information of constraint
  auto val0 =
      type::ValueFactory::GetIntegerValue(constraint->GetConstraintOid());
  auto val1 =
      type::ValueFactory::GetVarcharValue(constraint->GetName(), nullptr);
  auto val2 = type::ValueFactory::GetVarcharValue(
      ConstraintTypeToString(constraint->GetType()), nullptr);
  auto val3 = type::ValueFactory::GetIntegerValue(constraint->GetTableOid());
  std::stringstream ss;
  for (auto column_oid : constraint->GetColumnIds())
    ss << std::to_string(column_oid) << " ";
  auto val4 = type::ValueFactory::GetVarcharValue(ss.str(), nullptr);
  auto val5 = type::ValueFactory::GetIntegerValue(constraint->GetIndexOid());

  tuple->SetValue(ColumnId::CONSTRAINT_OID, val0, pool);
  tuple->SetValue(ColumnId::CONSTRAINT_NAME, val1, pool);
  tuple->SetValue(ColumnId::CONSTRAINT_TYPE, val2, pool);
  tuple->SetValue(ColumnId::TABLE_OID, val3, pool);
  tuple->SetValue(ColumnId::COLUMN_IDS, val4, pool);
  tuple->SetValue(ColumnId::INDEX_OID, val5, pool);

  // create values by type of constraint
  switch (constraint->GetType()) {
    case ConstraintType::PRIMARY:
    case ConstraintType::UNIQUE:
      // nothing to do more
      // need to set a valid index oid
      PELOTON_ASSERT(constraint->GetIndexOid() != INVALID_OID);
      break;

    case ConstraintType::FOREIGN: {
      // need to set a valid index oid
      PELOTON_ASSERT(constraint->GetIndexOid() != INVALID_OID);

      auto val6 =
          type::ValueFactory::GetIntegerValue(constraint->GetFKSinkTableOid());
      std::stringstream snk_ss;
      for (auto column_oid : constraint->GetFKSinkColumnIds())
        snk_ss << std::to_string(column_oid) << " ";
      auto val7 = type::ValueFactory::GetVarcharValue(snk_ss.str(), nullptr);
      auto val8 = type::ValueFactory::GetVarcharValue(
          FKConstrActionTypeToString(constraint->GetFKUpdateAction()), nullptr);
      auto val9 = type::ValueFactory::GetVarcharValue(
          FKConstrActionTypeToString(constraint->GetFKDeleteAction()), nullptr);

      tuple->SetValue(ColumnId::FK_SINK_TABLE_OID, val6, pool);
      tuple->SetValue(ColumnId::FK_SINK_COL_IDS, val7, pool);
      tuple->SetValue(ColumnId::FK_UPDATE_ACTION, val8, pool);
      tuple->SetValue(ColumnId::FK_DELETE_ACTION, val9, pool);
      break;
    }

    case ConstraintType::CHECK: {
      // set value of check expression
      PELOTON_ASSERT(constraint->GetColumnIds().size() == 1);
      auto exp = constraint->GetCheckExpression();
      auto column =
          storage::StorageManager::GetInstance()
              ->GetTableWithOid(database_oid_, constraint->GetTableOid())
              ->GetSchema()
              ->GetColumn(constraint->GetColumnIds().at(0));

      std::stringstream exp_ss;
      exp_ss << column.GetName() << " " << ExpressionTypeToString(exp.first)
             << " " << exp.second.ToString();
      auto val6 = type::ValueFactory::GetVarcharValue(exp_ss.str(), nullptr);

      CopySerializeOutput output_buffer;
      output_buffer.WriteInt((int)exp.first);
      output_buffer.WriteInt((int)column.GetType());
      exp.second.SerializeTo(output_buffer);
      auto val7 = type::ValueFactory::GetVarbinaryValue(
          (unsigned char *)output_buffer.Data(), output_buffer.Size(), true,
          pool);

      tuple->SetValue(ColumnId::CHECK_EXP_SRC, val6, pool);
      tuple->SetValue(ColumnId::CHECK_EXP_BIN, val7, pool);
      break;
    }

    case ConstraintType::EXCLUSION:
    default:
      // unexpected constraint type
      throw CatalogException("Unexpected constraint type '" +
                             ConstraintTypeToString(constraint->GetType()) +
                             "' appears in insertion into pg_constraint ");
      return false;
  }

  // Insert the tuple
  return InsertTuple(txn, std::move(tuple));
}

/* @brief   delete all constraint records from the same table
 *          this function is useful when calling DropTable
 * @param   txn  TransactionContext
 * @param   table_oid
 * @return  a vector of table oid
 */
bool ConstraintCatalog::DeleteConstraints(concurrency::TransactionContext *txn,
                                          oid_t table_oid) {
  oid_t index_offset = IndexId::SKEY_TABLE_OID;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  // delete constraints from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid_)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableCatalogEntry(txn, table_oid);
  table_object->EvictAllConstraintCatalogEntries();

  return DeleteWithIndexScan(txn, index_offset, values);
}

/** @brief      Delete a constraint from the pg_constraint table.
 *  @param      txn TransactionContext for deleting the constraint.
 *  @param      table_oid  oid of the table to which the old constraint belongs.
 *  @param      constraint_oid  oid of the constraint to be deleted.
 *  @return     true on success.
 */
bool ConstraintCatalog::DeleteConstraint(concurrency::TransactionContext *txn,
                                         oid_t table_oid,
                                         oid_t constraint_oid) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of constraint_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(constraint_oid).Copy());

  // delete constraint from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid_)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableCatalogEntry(txn, table_oid);
  table_object->EvictConstraintCatalogEntry(constraint_oid);

  return DeleteWithIndexScan(txn, index_offset, values);
}

/** @brief      Get all constraint objects correponding to a table
 *              from the pg_constraint.
 *  @param      txn TransactionContext for getting the constraints.
 *  @param      table_oid  oid of the table to fetch all constraints.
 *  @return     unordered_map containing a constraint_oid ->
 *              constraint object mapping.
 */
const std::unordered_map<oid_t, std::shared_ptr<ConstraintCatalogEntry>>
ConstraintCatalog::GetConstraintCatalogEntries(concurrency::TransactionContext *txn,
                                               oid_t table_oid) {
  // try get from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid_)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableCatalogEntry(txn, table_oid);
  PELOTON_ASSERT(table_object && table_object->GetTableOid() == table_oid);

  auto constraint_objects = table_object->GetConstraintCatalogEntries(true);
  if (constraint_objects.size() != 0) return constraint_objects;

  // cache miss, get from pg_constraint
  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::SKEY_TABLE_OID;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn, column_ids, index_offset, values);

  for (auto &tile : (*result_tiles)) {
    for (auto tuple_id : *tile) {
      auto constraint_object =
          std::make_shared<ConstraintCatalogEntry>(tile.get(), tuple_id);
      table_object->InsertConstraintCatalogEntry(constraint_object);
    }
  }

  table_object->SetValidConstraintCatalogEntries(true);
  return table_object->GetConstraintCatalogEntries();
}

/** @brief      Get the constraint object by constraint_oid from
 *              the pg_constraint.
 *  @param      txn TransactionContext for getting the constraint.
 *  @param      table_oid  oid of the table to fetch the constraint.
 *  @param      constraint_oid  oid of the constraint being queried.
 *  @return     shared_ptr constraint object to the constraint_oid if found.
 *              nullptr otherwise.
 */
const std::shared_ptr<ConstraintCatalogEntry>
ConstraintCatalog::GetConstraintCatalogEntry(concurrency::TransactionContext *txn,
                                             oid_t table_oid,
                                             oid_t constraint_oid) {
  // try get from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid_)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableCatalogEntry(txn, table_oid);
  PELOTON_ASSERT(table_object && table_object->GetTableOid() == table_oid);

  auto constraint_object =
      table_object->GetConstraintCatalogEntry(constraint_oid, true);
  if (constraint_object != nullptr) return constraint_object;

  // cache miss, get from pg_constraint
  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(constraint_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn, column_ids, index_offset, values);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto constraint_object =
        std::make_shared<ConstraintCatalogEntry>((*result_tiles)[0].get());
    table_object->InsertConstraintCatalogEntry(constraint_object);
    return constraint_object;
  }

  return nullptr;
}

}  // namespace catalog
}  // namespace peloton
