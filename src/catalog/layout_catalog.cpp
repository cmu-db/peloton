//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// layout_catalog.h
//
// Identification: src/include/catalog/layout_catalog.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/layout_catalog.h"

#include "catalog/catalog.h"
#include "catalog/system_catalogs.h"
#include "concurrency/transaction_context.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/layout.h"

namespace peloton {
namespace catalog {

LayoutCatalog *LayoutCatalog::GetInstance(storage::Database *pg_catalog,
                                          type::AbstractPool *pool,
                                          concurrency::TransactionContext *txn) {
  static LayoutCatalog layout_catalog{pg_catalog, pool, txn};
  return &layout_catalog;
}

LayoutCatalog::LayoutCatalog(
        storage::Database *pg_catalog,
        UNUSED_ATTRIBUTE type::AbstractPool *pool,
        UNUSED_ATTRIBUTE concurrency::TransactionContext *txn)
        : AbstractCatalog(LAYOUT_CATALOG_OID, LAYOUT_CATALOG_NAME,
                          InitializeSchema().release(), pg_catalog) {
  // Add indexes for pg_attribute
  AddIndex({ColumnId::TABLE_OID, ColumnId::LAYOUT_OID},
           LAYOUT_CATALOG_PKEY_OID, LAYOUT_CATALOG_NAME "_pkey",
           IndexConstraintType::PRIMARY_KEY);
  AddIndex({ColumnId::TABLE_OID}, LAYOUT_CATALOG_SKEY0_OID,
           LAYOUT_CATALOG_NAME "_skey0", IndexConstraintType::DEFAULT);
}

LayoutCatalog::~LayoutCatalog() {}

std::unique_ptr<catalog::Schema> LayoutCatalog::InitializeSchema() {
  const std::string primary_key_constraint_name = "primary_key";
  const std::string not_null_constraint_name = "not_null";

  auto table_id_column = catalog::Column(
          type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
          "table_oid", true);
  table_id_column.AddConstraint(catalog::Constraint(
          ConstraintType::PRIMARY, primary_key_constraint_name));
  table_id_column.AddConstraint(
          catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto layout_oid_column = catalog::Column(
          type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
          "layout_oid", true);
  layout_oid_column.AddConstraint(catalog::Constraint(
          ConstraintType::PRIMARY, primary_key_constraint_name));
  layout_oid_column.AddConstraint(
          catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto num_columns_column = catalog::Column(
          type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
          "num_columns", true);
  num_columns_column.AddConstraint(
          catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto column_map_column = catalog::Column(
          type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
          "column_map", false);
  column_map_column.AddConstraint(
          catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> column_catalog_schema(new catalog::Schema(
          {table_id_column, layout_oid_column, num_columns_column, column_map_column}));

  return column_catalog_schema;
}

bool LayoutCatalog::InsertLayout(oid_t table_oid,
                                 std::shared_ptr<const storage::Layout> layout,
                                 type::AbstractPool *pool,
                                 concurrency::TransactionContext *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
          new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val1 = type::ValueFactory::GetIntegerValue(layout->GetOid());
  auto val2 = type::ValueFactory::GetIntegerValue(layout->GetColumnCount());
  auto val3 = type::ValueFactory::GetVarcharValue(layout->SerializeColumnMap(),
                                                  nullptr);

  tuple->SetValue(LayoutCatalog::ColumnId::TABLE_OID, val0, pool);
  tuple->SetValue(LayoutCatalog::ColumnId::LAYOUT_OID, val1, pool);
  tuple->SetValue(LayoutCatalog::ColumnId::NUM_COLUMNS, val2, pool);
  tuple->SetValue(LayoutCatalog::ColumnId::COLUMN_MAP, val3, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool LayoutCatalog::DeleteLayout(oid_t table_oid, oid_t layout_id,
                                 concurrency::TransactionContext *txn) {
  oid_t index_offset =
          IndexId::PRIMARY_KEY;  // Index of table_oid & layout_oid

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(layout_id).Copy());

  auto pg_table = Catalog::GetInstance()
          ->GetSystemCatalogs(database_oid)
          ->GetTableCatalog();

  // delete column from cache
  auto table_object = pg_table->GetTableObject(table_oid, txn);
  table_object->EvictLayout(layout_id);

  return DeleteWithIndexScan(index_offset, values, txn);
}

bool LayoutCatalog::DeleteLayouts(oid_t table_oid,
                                  concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::SKEY_TABLE_OID;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  // delete layouts from cache
  auto pg_table = Catalog::GetInstance()
          ->GetSystemCatalogs(database_oid)
          ->GetTableCatalog();
  auto table_object = pg_table->GetTableObject(table_oid, txn);
  table_object->EvictAllLayouts();

  return DeleteWithIndexScan(index_offset, values, txn);
}

const std::unordered_map<oid_t, std::shared_ptr<const storage::Layout>>
LayoutCatalog::GetLayouts(oid_t table_oid,
                          concurrency::TransactionContext *txn) {
  // Try to find the layouts in the cache
  auto pg_table = Catalog::GetInstance()
          ->GetSystemCatalogs(database_oid)
          ->GetTableCatalog();
  auto table_object = pg_table->GetTableObject(table_oid, txn);
  PELOTON_ASSERT(table_object && table_object->GetTableOid() == table_oid);
  auto layout_objects = table_object->GetLayouts(true);
  if (layout_objects.size() != 0) {
    return layout_objects;
  }

  // Cache miss, get from pg_catalog
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::SKEY_TABLE_OID;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
          GetResultWithIndexScan(column_ids, index_offset, values, txn);

  // result_tiles should contain only 1 LogicalTile per table
  auto result_size = (*result_tiles).size();
  PELOTON_ASSERT(result_size <= 1);
  if (result_size == 0) {
    LOG_DEBUG("No entry for table %u in pg_layout", table_oid);
    return table_object->GetLayouts();
  }

  // Get the LogicalTile
  auto tile = (*result_tiles)[0].get();
  auto tuple_count = tile->GetTupleCount();
  for (oid_t tuple_id = 0; tuple_id < tuple_count; tuple_id++) {
    oid_t layout_oid =
            tile->GetValue(tuple_id, LayoutCatalog::ColumnId::LAYOUT_OID)
                    .GetAs<oid_t>();
    oid_t num_columns =
            tile->GetValue(tuple_id, LayoutCatalog::ColumnId::NUM_COLUMNS)
                    .GetAs<oid_t>();
    std::string column_map_str =
            tile->GetValue(tuple_id, LayoutCatalog::ColumnId::COLUMN_MAP)
                    .ToString();
    auto column_map = storage::Layout::DeserializeColumnMap(num_columns,
                                                            column_map_str);
    auto layout_object =
            std::make_shared<const storage::Layout>(column_map, layout_oid);
    table_object->InsertLayout(layout_object);
  }

  return table_object->GetLayouts();
}

std::shared_ptr<const storage::Layout>
LayoutCatalog::GetLayoutWithOid(oid_t table_oid, oid_t layout_oid,
                 concurrency::TransactionContext *txn) {
  auto table_layouts = GetLayouts(table_oid, txn);
  for (const auto &layout_entry : table_layouts) {
    if (layout_entry.second->GetOid() == layout_oid) {
      return layout_entry.second;
    }
  }
  return nullptr;
}

}  // namespace catalog
}  // namespace peloton