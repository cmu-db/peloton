//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// layout_catalog.cpp
//
// Identification: src/catalog/layout_catalog.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/layout_catalog.h"

#include "catalog/catalog.h"
#include "catalog/system_catalogs.h"
#include "concurrency/transaction_context.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/layout.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

/** @brief   Constructor invoked by the SystemsCatalog constructor.
 *  @param   pg_catalog  The database to which this pg_layout belongs.
 */
LayoutCatalog::LayoutCatalog(concurrency::TransactionContext *,
                             storage::Database *pg_catalog,
                             type::AbstractPool *)
    : AbstractCatalog(pg_catalog,
                      InitializeSchema().release(),
                      LAYOUT_CATALOG_OID,
                      LAYOUT_CATALOG_NAME) {
  // Add indexes for pg_attribute
  AddIndex(LAYOUT_CATALOG_NAME "_pkey",
           LAYOUT_CATALOG_PKEY_OID,
           {ColumnId::TABLE_OID, ColumnId::LAYOUT_OID},
           IndexConstraintType::PRIMARY_KEY);
  AddIndex(LAYOUT_CATALOG_NAME "_skey0",
           LAYOUT_CATALOG_SKEY0_OID,
           {ColumnId::TABLE_OID},
           IndexConstraintType::DEFAULT);
}
/** @brief   Destructor. Do nothing. Layouts will be dropped by DropTable. */
LayoutCatalog::~LayoutCatalog() {}

/** @brief   Initilailizes the schema for the pg_layout table.
 *  @return  unique_ptr of the schema for pg_layout.
 */
std::unique_ptr<catalog::Schema> LayoutCatalog::InitializeSchema() {
  auto table_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "table_oid", true);
  table_id_column.SetNotNull();

  auto layout_oid_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "layout_oid", true);
  layout_oid_column.SetNotNull();

  auto num_columns_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "num_columns", true);
  num_columns_column.SetNotNull();

  auto column_map_column = catalog::Column(
      type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
      "column_map", false);
  column_map_column.SetNotNull();

  std::unique_ptr<catalog::Schema> layout_catalog_schema(
      new catalog::Schema({table_id_column, layout_oid_column,
                           num_columns_column, column_map_column}));

  layout_catalog_schema->AddConstraint(std::make_shared<catalog::Constraint>(
      LAYOUT_CATALOG_CON_PKEY_OID, ConstraintType::PRIMARY, "con_primary",
      LAYOUT_CATALOG_OID, std::vector<oid_t>{ColumnId::TABLE_OID, ColumnId::LAYOUT_OID},
      LAYOUT_CATALOG_PKEY_OID));

  return layout_catalog_schema;
}

/** @brief      Insert a layout into the pg_layout table.
 *  @param      table_oid  oid of the table to which the new layout belongs.
 *  @param      layout  layout to be added to the pg_layout table.
 *  @param      pool to allocate memory for the column_map column.
 *  @param      txn TransactionContext for adding the layout.
 *  @return     true on success.
 */
bool LayoutCatalog::InsertLayout(concurrency::TransactionContext *txn,
                                 oid_t table_oid,
                                 std::shared_ptr<const storage::Layout> layout,
                                 type::AbstractPool *pool) {
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
  return InsertTuple(txn, std::move(tuple));
}

/** @brief      Delete a layout from the pg_layout table.
 *  @param      table_oid  oid of the table to which the old layout belongs.
 *  @param      layout_oid  oid of the layout to be deleted.
 *  @param      txn TransactionContext for deleting the layout.
 *  @return     true on success.
 */
bool LayoutCatalog::DeleteLayout(concurrency::TransactionContext *txn,
                                 oid_t table_oid,
                                 oid_t layout_oid) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of table_oid & layout_oid

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(layout_oid).Copy());

  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid_)
                      ->GetTableCatalog();

  // delete column from cache
  auto table_object = pg_table->GetTableCatalogEntry(txn, table_oid);
  table_object->EvictLayout(layout_oid);

  return DeleteWithIndexScan(txn, index_offset, values);
}

/** @brief      Delete all layouts correponding to a table from the pg_layout.
 *  @param      table_oid  oid of the table to delete all layouts.
 *  @param      txn TransactionContext for deleting the layouts.
 *  @return     true on success.
 */
bool LayoutCatalog::DeleteLayouts(concurrency::TransactionContext *txn, oid_t table_oid) {
  oid_t index_offset = IndexId::SKEY_TABLE_OID;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  // delete layouts from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid_)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableCatalogEntry(txn, table_oid);
  table_object->EvictAllLayouts();

  return DeleteWithIndexScan(txn, index_offset, values);
}

/** @brief      Get all layouts correponding to a table from the pg_layout.
 *  @param      table_oid  oid of the table to fetch all layouts.
 *  @param      txn TransactionContext for getting the layouts.
 *  @return     unordered_map containing a layout_oid -> layout mapping.
 */
const std::unordered_map<oid_t,
                         std::shared_ptr<const storage::Layout>>
LayoutCatalog::GetLayouts(
concurrency::TransactionContext *txn,
oid_t table_oid) {
  // Try to find the layouts in the cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid_)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableCatalogEntry(txn, table_oid);
  PELOTON_ASSERT(table_object && table_object->GetTableOid() == table_oid);
  auto layout_objects = table_object->GetLayouts(true);
  if (layout_objects.size() != 0) {
    return layout_objects;
  }

  // Cache miss, get from pg_catalog
  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::SKEY_TABLE_OID;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);

  for (auto &tile : (*result_tiles)) {  // Iterate through the result_tiles
    for (auto tuple_id : *tile) {
      oid_t layout_oid =
          tile->GetValue(tuple_id, LayoutCatalog::ColumnId::LAYOUT_OID)
              .GetAs<oid_t>();
      oid_t num_columns =
          tile->GetValue(tuple_id, LayoutCatalog::ColumnId::NUM_COLUMNS)
              .GetAs<oid_t>();
      std::string column_map_str =
          tile->GetValue(tuple_id, LayoutCatalog::ColumnId::COLUMN_MAP)
              .ToString();

      column_map_type column_map;
      if (column_map_str.length() != 0) {
        column_map =
            storage::Layout::DeserializeColumnMap(num_columns, column_map_str);
      }
      auto layout_object =
          std::make_shared<const storage::Layout>(column_map, num_columns,
                                                  layout_oid);
      table_object->InsertLayout(layout_object);
    }
  }

  return table_object->GetLayouts();
}

/** @brief      Get the layout by layout_oid from the pg_layout.
 *  @param      table_oid  oid of the table to fetch the layout.
 *  @param      layout_oid  oid of the layout being queried.
 *  @param      txn TransactionContext for getting the layout.
 *  @return     shared_ptr corresponding to the layout_oid if found.
 *              nullptr otherwise.
 */
std::shared_ptr<const storage::Layout> LayoutCatalog::GetLayoutWithOid(concurrency::TransactionContext *txn,
                                                                       oid_t table_oid,
                                                                       oid_t layout_oid) {
  auto table_layouts = GetLayouts(txn, table_oid);
  for (const auto &layout_entry : table_layouts) {
    if (layout_entry.second->GetOid() == layout_oid) {
      return layout_entry.second;
    }
  }
  return nullptr;
}

}  // namespace catalog
}  // namespace peloton
