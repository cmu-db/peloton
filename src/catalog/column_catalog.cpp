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

#include "catalog/column_catalog.h"

#include "catalog/catalog.h"
#include "catalog/system_catalogs.h"
#include "catalog/table_catalog.h"
#include "concurrency/transaction_context.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "type/ephemeral_pool.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

ColumnCatalogObject::ColumnCatalogObject(executor::LogicalTile *tile,
                                         int tupleId)
    : table_oid(tile->GetValue(tupleId, ColumnCatalog::ColumnId::TABLE_OID)
                    .GetAs<oid_t>()),
      column_name(tile->GetValue(tupleId, ColumnCatalog::ColumnId::COLUMN_NAME)
                      .ToString()),
      column_id(tile->GetValue(tupleId, ColumnCatalog::ColumnId::COLUMN_ID)
                    .GetAs<oid_t>()),
      column_offset(
          tile->GetValue(tupleId, ColumnCatalog::ColumnId::COLUMN_OFFSET)
              .GetAs<oid_t>()),
      column_type(StringToTypeId(
          tile->GetValue(tupleId, ColumnCatalog::ColumnId::COLUMN_TYPE)
              .ToString())),
      column_length(
          tile->GetValue(tupleId, ColumnCatalog::ColumnId::COLUMN_LENGTH)
              .GetAs<uint32_t>()),
      is_inlined(tile->GetValue(tupleId, ColumnCatalog::ColumnId::IS_INLINED)
                     .GetAs<bool>()),
      is_not_null(tile->GetValue(tupleId, ColumnCatalog::ColumnId::IS_NOT_NULL)
                      .GetAs<bool>()),
      has_default(tile->GetValue(tupleId, ColumnCatalog::ColumnId::HAS_DEFAULT)
                      .GetAs<bool>()) {
  // deserialize default value if the column has default constraint
  if (has_default) {
    auto dv_val =
        tile->GetValue(tupleId, ColumnCatalog::ColumnId::DEFAULT_VALUE_BIN);
    CopySerializeInput input_buffer(dv_val.GetData(), dv_val.GetLength());
    default_value = type::Value::DeserializeFrom(input_buffer, column_type);
  }
}

ColumnCatalog::ColumnCatalog(storage::Database *pg_catalog,
                             type::AbstractPool *pool,
                             concurrency::TransactionContext *txn)
    : AbstractCatalog(COLUMN_CATALOG_OID, COLUMN_CATALOG_NAME,
                      InitializeSchema().release(), pg_catalog) {
  // Add indexes for pg_attribute
  AddIndex({ColumnId::TABLE_OID, ColumnId::COLUMN_NAME},
           COLUMN_CATALOG_PKEY_OID, COLUMN_CATALOG_NAME "_pkey",
           IndexConstraintType::PRIMARY_KEY);
  AddIndex({ColumnId::TABLE_OID, ColumnId::COLUMN_ID}, COLUMN_CATALOG_SKEY0_OID,
           COLUMN_CATALOG_NAME "_skey0", IndexConstraintType::UNIQUE);
  AddIndex({ColumnId::TABLE_OID}, COLUMN_CATALOG_SKEY1_OID,
           COLUMN_CATALOG_NAME "_skey1", IndexConstraintType::DEFAULT);

  // Insert columns of pg_attribute table into pg_attribute itself
  uint32_t column_id = 0;
  for (auto column : catalog_table_->GetSchema()->GetColumns()) {
    InsertColumn(COLUMN_CATALOG_OID, column.GetName(), column_id,
                 column.GetOffset(), column.GetType(), column.GetLength(),
                 column.IsInlined(), column.IsNotNull(), column.HasDefault(),
                 column.GetDefaultValue(), pool, txn);
    column_id++;
  }
}

ColumnCatalog::~ColumnCatalog() {}

/*@brief   private function for initialize schema of pg_attribute
 * @return  unqiue pointer to schema
 */
std::unique_ptr<catalog::Schema> ColumnCatalog::InitializeSchema() {
  auto table_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "table_oid", true);
  table_id_column.SetNotNull();

  auto column_name_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size, "column_name", false);
  column_name_column.SetNotNull();

  auto column_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "column_id", true);
  column_id_column.SetNotNull();

  auto column_offset_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "column_offset", true);
  column_offset_column.SetNotNull();

  auto column_type_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size, "column_type", false);
  column_type_column.SetNotNull();

  auto column_length_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "column_length", true);
  column_length_column.SetNotNull();

  auto is_inlined_column = catalog::Column(
      type::TypeId::BOOLEAN, type::Type::GetTypeSize(type::TypeId::BOOLEAN),
      "is_inlined", true);
  is_inlined_column.SetNotNull();

  auto is_not_null_column = catalog::Column(
      type::TypeId::BOOLEAN, type::Type::GetTypeSize(type::TypeId::BOOLEAN),
      "is_not_null", true);
  is_not_null_column.SetNotNull();

  auto has_default_column = catalog::Column(
      type::TypeId::BOOLEAN, type::Type::GetTypeSize(type::TypeId::BOOLEAN),
      "has_default", true);
  has_default_column.SetNotNull();

  auto default_value_src_column = catalog::Column(
      type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
      "default_value_src", false);

  auto default_value_bin_column = catalog::Column(
      type::TypeId::VARBINARY, type::Type::GetTypeSize(type::TypeId::VARBINARY),
      "default_value_bin", false);

  std::unique_ptr<catalog::Schema> column_catalog_schema(new catalog::Schema(
      {table_id_column, column_name_column, column_id_column,
       column_offset_column, column_type_column, column_length_column,
       is_inlined_column, is_not_null_column, has_default_column,
       default_value_src_column, default_value_bin_column}));

  column_catalog_schema->AddConstraint(std::make_shared<Constraint>(
      COLUMN_CATALOG_CON_PKEY_OID, ConstraintType::PRIMARY, "con_primary",
      COLUMN_CATALOG_OID,
      std::vector<oid_t>{ColumnId::TABLE_OID, ColumnId::COLUMN_NAME},
      COLUMN_CATALOG_PKEY_OID));

  column_catalog_schema->AddConstraint(std::make_shared<Constraint>(
      COLUMN_CATALOG_CON_UNI0_OID, ConstraintType::UNIQUE, "con_unique",
      COLUMN_CATALOG_OID,
      std::vector<oid_t>{ColumnId::TABLE_OID, ColumnId::COLUMN_ID},
      COLUMN_CATALOG_SKEY0_OID));

  return column_catalog_schema;
}

bool ColumnCatalog::InsertColumn(
    oid_t table_oid, const std::string &column_name, oid_t column_id,
    oid_t column_offset, type::TypeId column_type, size_t column_length,
    bool is_inlined, bool is_not_null, bool is_default,
    const std::shared_ptr<type::Value> default_value, type::AbstractPool *pool,
    concurrency::TransactionContext *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(column_name, nullptr);
  auto val2 = type::ValueFactory::GetIntegerValue(column_id);
  auto val3 = type::ValueFactory::GetIntegerValue(column_offset);
  auto val4 =
      type::ValueFactory::GetVarcharValue(TypeIdToString(column_type), nullptr);
  auto val5 = type::ValueFactory::GetIntegerValue(column_length);
  auto val6 = type::ValueFactory::GetBooleanValue(is_inlined);
  auto val7 = type::ValueFactory::GetBooleanValue(is_not_null);
  auto val8 = type::ValueFactory::GetBooleanValue(is_default);

  tuple->SetValue(ColumnId::TABLE_OID, val0, pool);
  tuple->SetValue(ColumnId::COLUMN_NAME, val1, pool);
  tuple->SetValue(ColumnId::COLUMN_ID, val2, pool);
  tuple->SetValue(ColumnId::COLUMN_OFFSET, val3, pool);
  tuple->SetValue(ColumnId::COLUMN_TYPE, val4, pool);
  tuple->SetValue(ColumnId::COLUMN_LENGTH, val5, pool);
  tuple->SetValue(ColumnId::IS_INLINED, val6, pool);
  tuple->SetValue(ColumnId::IS_NOT_NULL, val7, pool);
  tuple->SetValue(ColumnId::HAS_DEFAULT, val8, pool);

  // set default value if the column has default constraint
  if (is_default) {
    auto val9 =
        type::ValueFactory::GetVarcharValue(default_value->ToString(), nullptr);
    CopySerializeOutput output_buffer;
    default_value->SerializeTo(output_buffer);
    auto val10 = type::ValueFactory::GetVarbinaryValue(
        (unsigned char *)output_buffer.Data(), output_buffer.Size(), true,
        pool);

    tuple->SetValue(ColumnId::DEFAULT_VALUE_SRC, val9, pool);
    tuple->SetValue(ColumnId::DEFAULT_VALUE_BIN, val10, pool);
  }

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool ColumnCatalog::DeleteColumn(oid_t table_oid,
                                 const std::string &column_name,
                                 concurrency::TransactionContext *txn) {
  oid_t index_offset =
      IndexId::PRIMARY_KEY;  // Index of table_oid & column_name
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(
      type::ValueFactory::GetVarcharValue(column_name, nullptr).Copy());

  // delete column from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableObject(table_oid, txn);
  table_object->EvictColumnObject(column_name);

  return DeleteWithIndexScan(index_offset, values, txn);
}

/* @brief   delete all column records from the same table
 * this function is useful when calling DropTable
 * @param   table_oid
 * @param   txn  TransactionContext
 * @return  a vector of table oid
 */
bool ColumnCatalog::DeleteColumns(oid_t table_oid,
                                  concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::SKEY_TABLE_OID;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  // delete columns from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableObject(table_oid, txn);
  table_object->EvictAllColumnObjects();

  return DeleteWithIndexScan(index_offset, values, txn);
}

bool ColumnCatalog::UpdateNotNullConstraint(
    oid_t table_oid, const std::string &column_name, bool is_not_null,
    concurrency::TransactionContext *txn) {
  std::vector<oid_t> update_columns({ColumnId::IS_NOT_NULL});
  oid_t index_offset =
      IndexId::PRIMARY_KEY;  // Index of table_oid & column_name
  // values to execute index scan
  std::vector<type::Value> scan_values;
  scan_values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  scan_values.push_back(
      type::ValueFactory::GetVarcharValue(column_name, nullptr).Copy());

  // values to update
  std::vector<type::Value> update_values;
  update_values.push_back(
      type::ValueFactory::GetBooleanValue(is_not_null).Copy());

  // delete column from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableObject(table_oid, txn);
  table_object->EvictColumnObject(column_name);

  return UpdateWithIndexScan(update_columns, update_values, scan_values,
                             index_offset, txn);
}

bool ColumnCatalog::UpdateDefaultConstraint(
    oid_t table_oid, const std::string &column_name, bool has_default,
    const type::Value *default_value, concurrency::TransactionContext *txn) {
  std::vector<oid_t> update_columns({ColumnId::HAS_DEFAULT,
                                     ColumnId::DEFAULT_VALUE_SRC,
                                     ColumnId::DEFAULT_VALUE_BIN});
  oid_t index_offset =
      IndexId::PRIMARY_KEY;  // Index of table_oid & column_name
  // values to execute index scan
  std::vector<type::Value> scan_values;
  scan_values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  scan_values.push_back(
      type::ValueFactory::GetVarcharValue(column_name, nullptr).Copy());

  // values to update
  std::vector<type::Value> update_values;
  update_values.push_back(
      type::ValueFactory::GetBooleanValue(has_default).Copy());
  if (has_default) {
    PELOTON_ASSERT(default_value != nullptr);
    update_values.push_back(
        type::ValueFactory::GetVarcharValue(default_value->ToString()).Copy());
    CopySerializeOutput output_buffer;
    default_value->SerializeTo(output_buffer);
    update_values.push_back(type::ValueFactory::GetVarbinaryValue(
                                (unsigned char *)output_buffer.Data(),
                                output_buffer.Size(), true).Copy());
  } else {
    update_values.push_back(
        type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR));
    update_values.push_back(
        type::ValueFactory::GetNullValueByType(type::TypeId::VARBINARY));
  }

  // delete column from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableObject(table_oid, txn);
  table_object->EvictColumnObject(column_name);

  return UpdateWithIndexScan(update_columns, update_values, scan_values,
                             index_offset, txn);
}

const std::unordered_map<oid_t, std::shared_ptr<ColumnCatalogObject>>
ColumnCatalog::GetColumnObjects(oid_t table_oid,
                                concurrency::TransactionContext *txn) {
  // try get from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableObject(table_oid, txn);
  PELOTON_ASSERT(table_object && table_object->GetTableOid() == table_oid);
  auto column_objects = table_object->GetColumnObjects(true);
  if (column_objects.size() != 0) return column_objects;

  // cache miss, get from pg_attribute
  std::vector<oid_t> column_ids(all_column_ids);
  oid_t index_offset = IndexId::SKEY_TABLE_OID;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  for (auto &tile : (*result_tiles)) {
    for (auto tuple_id : *tile) {
      auto column_object =
          std::make_shared<ColumnCatalogObject>(tile.get(), tuple_id);
      table_object->InsertColumnObject(column_object);
    }
  }

  return table_object->GetColumnObjects();
}

}  // namespace catalog
}  // namespace peloton
