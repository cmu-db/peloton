//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_catalog.cpp
//
// Identification: src/catalog/table_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/table_catalog.h"

#include <memory>

#include "catalog/catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/layout_catalog.h"
#include "catalog/system_catalogs.h"
#include "concurrency/transaction_context.h"
#include "storage/data_table.h"
#include "storage/database.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

TableCatalogEntry::TableCatalogEntry(concurrency::TransactionContext *txn,
                                       executor::LogicalTile *tile,
                                       int tupleId)
    : table_oid(tile->GetValue(tupleId, TableCatalog::ColumnId::TABLE_OID)
                    .GetAs<oid_t>()),
      table_name(tile->GetValue(tupleId, TableCatalog::ColumnId::TABLE_NAME)
                     .ToString()),
      schema_name(tile->GetValue(tupleId, TableCatalog::ColumnId::SCHEMA_NAME)
                      .ToString()),
      database_oid(tile->GetValue(tupleId, TableCatalog::ColumnId::DATABASE_OID)
                       .GetAs<oid_t>()),
      version_id(tile->GetValue(tupleId, TableCatalog::ColumnId::VERSION_ID)
                     .GetAs<uint32_t>()),
      default_layout_oid(tile->GetValue(tupleId,
      		TableCatalog::ColumnId::DEFAULT_LAYOUT_OID).GetAs<oid_t>()),
      index_catalog_entries(),
      index_catalog_entries_by_name_(),
      valid_index_catalog_entries_(false),
      column_catalog_entries_(),
      column_names_(),
      valid_column_catalog_entries_(false),
			valid_layout_catalog_entries_(false),
      txn_(txn) {}

/* @brief   insert index catalog object into cache
 * @param   index_object
 * @return  false if index_name already exists in cache
 */
bool TableCatalogEntry::InsertIndexCatalogEntry(
    std::shared_ptr<IndexCatalogEntry> index_catalog_entry) {
  if (!index_catalog_entry || index_catalog_entry->GetIndexOid() == INVALID_OID) {
    return false;  // invalid object
  }

  // check if already in cache
  if (index_catalog_entries.find(index_catalog_entry->GetIndexOid()) != index_catalog_entries.end()) {
    LOG_DEBUG("Index %u already exists in cache!", index_catalog_entry->GetIndexOid());
    return false;
  }

  if (index_catalog_entries_by_name_.find(index_catalog_entry->GetIndexName()) != index_catalog_entries_by_name_.end()) {
    LOG_DEBUG("Index %s already exists in cache!",
              index_catalog_entry->GetIndexName().c_str());
    return false;
  }

  valid_index_catalog_entries_ = true;
  index_catalog_entries.insert(
      std::make_pair(index_catalog_entry->GetIndexOid(), index_catalog_entry));
  index_catalog_entries_by_name_.insert(
      std::make_pair(index_catalog_entry->GetIndexName(), index_catalog_entry));
  return true;
}

/* @brief   evict index catalog object from cache
 * @param   index_oid
 * @return  true if index_oid is found and evicted; false if not found
 */
bool TableCatalogEntry::EvictIndexCatalogEntry(oid_t index_oid) {
  if (!valid_index_catalog_entries_) return false;

  // find index name from index name cache
  auto it = index_catalog_entries.find(index_oid);
  if (it == index_catalog_entries.end()) {
    return false;  // index oid not found in cache
  }

  auto index_object = it->second;
  PELOTON_ASSERT(index_object);
  index_catalog_entries.erase(it);
  index_catalog_entries_by_name_.erase(index_object->GetIndexName());
  return true;
}

/* @brief   evict index catalog object from cache
 * @param   index_name
 * @return  true if index_name is found and evicted; false if not found
 */
bool TableCatalogEntry::EvictIndexCatalogEntry(const std::string &index_name) {
  if (!valid_index_catalog_entries_) return false;

  // find index name from index name cache
  auto it = index_catalog_entries_by_name_.find(index_name);
  if (it == index_catalog_entries_by_name_.end()) {
    return false;  // index name not found in cache
  }

  auto index_object = it->second;
  PELOTON_ASSERT(index_object);
  index_catalog_entries_by_name_.erase(it);
  index_catalog_entries.erase(index_object->GetIndexOid());
  return true;
}

/* @brief   evict all index catalog objects from cache
 */
void TableCatalogEntry::EvictAllIndexCatalogEntries() {
  index_catalog_entries.clear();
  index_catalog_entries_by_name_.clear();
  valid_index_catalog_entries_ = false;
}

/* @brief   get all index objects of this table into cache
 * @return  map from index oid to cached index object
 */
std::unordered_map<oid_t, std::shared_ptr<IndexCatalogEntry>>
TableCatalogEntry::GetIndexCatalogEntries(bool cached_only) {
  if (!valid_index_catalog_entries_ && !cached_only) {
    // get index catalog objects from pg_index
    valid_index_catalog_entries_ = true;
    auto pg_index = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid)
                        ->GetIndexCatalog();
    index_catalog_entries = pg_index->GetIndexCatalogEntries(txn_, table_oid);
  }
  return index_catalog_entries;
}

/* @brief   get index object with index oid from cache
 * @param   index_oid
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  shared pointer to the cached index object, nullptr if not found
 */
std::shared_ptr<IndexCatalogEntry> TableCatalogEntry::GetIndexCatalogEntries(
    oid_t index_oid, bool cached_only) {
  GetIndexCatalogEntries(cached_only);  // fetch index objects in case we have not
  auto it = index_catalog_entries.find(index_oid);
  if (it != index_catalog_entries.end()) {
    return it->second;
  }
  return nullptr;
}

/* @brief   get index object with index oid from cache
 * @param   index_name
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  shared pointer to the cached index object, nullptr if not found
 */
std::shared_ptr<IndexCatalogEntry> TableCatalogEntry::GetIndexCatalogEntry(
    const std::string &index_name, bool cached_only) {
  GetIndexCatalogEntries(cached_only);  // fetch index objects in case we have not
  auto it = index_catalog_entries_by_name_.find(index_name);
  if (it != index_catalog_entries_by_name_.end()) {
    return it->second;
  }
  return nullptr;
}

/* @brief   insert column catalog object into cache
 * @param   column_object
 * @return  false if column_name already exists in cache
 */
bool TableCatalogEntry::InsertColumnCatalogEntry(
    std::shared_ptr<ColumnCatalogEntry> column_catalog_entry) {
  if (!column_catalog_entry || column_catalog_entry->GetTableOid() == INVALID_OID) {
    return false;  // invalid object
  }

  // check if already in cache
  if (column_catalog_entries_.find(column_catalog_entry->GetColumnId()) !=
      column_catalog_entries_.end()) {
    LOG_DEBUG("Column %u already exists in cache!",
              column_catalog_entry->GetColumnId());
    return false;
  }

  if (column_names_.find(column_catalog_entry->GetColumnName()) != column_names_.end()) {
    LOG_DEBUG("Column %s already exists in cache!",
              column_catalog_entry->GetColumnName().c_str());
    return false;
  }

  valid_column_catalog_entries_ = true;
  column_catalog_entries_.insert(
      std::make_pair(column_catalog_entry->GetColumnId(), column_catalog_entry));
  column_names_.insert(
      std::make_pair(column_catalog_entry->GetColumnName(), column_catalog_entry));
  return true;
}

/* @brief   evict column catalog object from cache
 * @param   column_id
 * @return  true if column_id is found and evicted; false if not found
 */
bool TableCatalogEntry::EvictColumnCatalogEntry(oid_t column_id) {
  if (!valid_column_catalog_entries_) return false;

  // find column name from column name cache
  auto it = column_catalog_entries_.find(column_id);
  if (it == column_catalog_entries_.end()) {
    return false;  // column id not found in cache
  }

  auto column_object = it->second;
  PELOTON_ASSERT(column_object);
  column_catalog_entries_.erase(it);
  column_names_.erase(column_object->GetColumnName());
  return true;
}

/* @brief   evict column catalog object from cache
 * @param   column_name
 * @return  true if column_name is found and evicted; false if not found
 */
bool TableCatalogEntry::EvictColumnCatalogEntry(const std::string &column_name) {
  if (!valid_column_catalog_entries_) return false;

  // find column name from column name cache
  auto it = column_names_.find(column_name);
  if (it == column_names_.end()) {
    return false;  // column name not found in cache
  }

  auto column_object = it->second;
  PELOTON_ASSERT(column_object);
  column_names_.erase(it);
  column_catalog_entries_.erase(column_object->GetColumnId());
  return true;
}

/* @brief   evict all column catalog objects from cache
 * @return  true if column_name is found and evicted; false if not found
 */
void TableCatalogEntry::EvictAllColumnCatalogEntries() {
  column_catalog_entries_.clear();
  column_names_.clear();
  valid_column_catalog_entries_ = false;
}

/* @brief   get all column objects of this table into cache
 * @return  map from column id to cached column object
 */
std::unordered_map<oid_t, std::shared_ptr<ColumnCatalogEntry>>
TableCatalogEntry::GetColumnCatalogEntries(bool cached_only) {
  if (!valid_column_catalog_entries_ && !cached_only) {
    // get column catalog objects from pg_column
    auto pg_attribute = Catalog::GetInstance()
                            ->GetSystemCatalogs(database_oid)
                            ->GetColumnCatalog();
    pg_attribute->GetColumnCatalogEntries(txn_, table_oid);
    valid_column_catalog_entries_ = true;
  }
  return column_catalog_entries_;
}

/* @brief   get all column objects of this table into cache
 * @return  map from column name to cached column object
 */
std::unordered_map<std::string, std::shared_ptr<ColumnCatalogEntry>>
TableCatalogEntry::GetColumnCatalogEntriesByName(bool cached_only) {
  if (!valid_column_catalog_entries_ && !cached_only) {
    auto column_objects = GetColumnCatalogEntries();
    std::unordered_map<std::string, std::shared_ptr<ColumnCatalogEntry>>
        column_names;
    for (auto &pair : column_objects) {
      auto column = pair.second;
      column_names[column->GetColumnName()] = column;
    }
  }
  return column_names_;
}

/* @brief   get column object with column id from cache
 * @param   column_id
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  shared pointer to the cached column object, nullptr if not found
 */
std::shared_ptr<ColumnCatalogEntry> TableCatalogEntry::GetColumnCatalogEntry(
    oid_t column_id, bool cached_only) {
  GetColumnCatalogEntries(cached_only);  // fetch column objects in case we have not
  auto it = column_catalog_entries_.find(column_id);
  if (it != column_catalog_entries_.end()) {
    return it->second;
  }
  return nullptr;
}

/* @brief   get column object with column id from cache
 * @param   column_name
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  shared pointer to the cached column object, nullptr if not found
 */
std::shared_ptr<ColumnCatalogEntry> TableCatalogEntry::GetColumnCatalogEntry(
    const std::string &column_name, bool cached_only) {
  GetColumnCatalogEntries(cached_only);  // fetch column objects in case we have not
  auto it = column_names_.find(column_name);
  if (it != column_names_.end()) {
    return it->second;
  }
  return nullptr;
}

TableCatalog::TableCatalog(concurrency::TransactionContext *,
                           storage::Database *database,
                           type::AbstractPool *)
    : AbstractCatalog(database,
                      InitializeSchema().release(),
                      TABLE_CATALOG_OID,
                      TABLE_CATALOG_NAME) {
  // Add indexes for pg_namespace
  AddIndex(TABLE_CATALOG_NAME "_pkey",
           TABLE_CATALOG_PKEY_OID,
           {0},
           IndexConstraintType::PRIMARY_KEY);
  AddIndex(TABLE_CATALOG_NAME "_skey0",
           TABLE_CATALOG_SKEY0_OID,
           {1, 2},
           IndexConstraintType::UNIQUE);
  AddIndex(TABLE_CATALOG_NAME "_skey1",
           TABLE_CATALOG_SKEY1_OID,
           {3},
           IndexConstraintType::DEFAULT);
}

/** @brief   Insert layout object into the cache.
 *  @param   layout  Layout object to be inserted
 *  @return  false if layout already exists in cache
 */
bool TableCatalogEntry::InsertLayout(
    std::shared_ptr<const storage::Layout> layout) {
  // Invalid object
  if (!layout || (layout->GetOid() == INVALID_OID)) {
    return false;
  }

  oid_t layout_id = layout->GetOid();
  // layout is already present in the cache.
  if (layout_catalog_entries_.find(layout_id) != layout_catalog_entries_.end()) {
    LOG_DEBUG("Layout %u already exists in cache!", layout_id);
    return false;
  }

  layout_catalog_entries_.insert(std::make_pair(layout_id, layout));
  return true;
}

/** @brief   evict all layout objects from cache. */
void TableCatalogEntry::EvictAllLayouts() {
  layout_catalog_entries_.clear();
  valid_layout_catalog_entries_ = false;
}

/** @brief   Get all layout objects of this table.
 *           Add it to the cache if necessary.
 *  @param   cached_only If set to true, don't fetch the layout objects.
 *  @return  Map from layout_oid to cached layout object.
 */
std::unordered_map<oid_t, std::shared_ptr<const storage::Layout>>
TableCatalogEntry::GetLayouts(bool cached_only) {
  if (!valid_layout_catalog_entries_ && !cached_only) {
    // get layout catalog objects from pg_layout
    auto pg_layout = Catalog::GetInstance()
                         ->GetSystemCatalogs(database_oid)
                         ->GetLayoutCatalog();
    pg_layout->GetLayouts(txn_, table_oid);
    valid_column_catalog_entries_ = true;
  }
  return layout_catalog_entries_;
}

/** @brief   Get the layout object of the given layout_id.
 *  @param   layout_id The id of the layout to be fetched.
 *  @param   cached_only If set to true, don't fetch the layout objects.
 *  @return  Layout object of corresponding to the layout_id if present.
 */
std::shared_ptr<const storage::Layout> TableCatalogEntry::GetLayout(
    oid_t layout_id, bool cached_entry) {
  // fetch layout objects in case we have not
  GetLayouts(cached_entry);
  auto it = layout_catalog_entries_.find(layout_id);
  if (it != layout_catalog_entries_.end()) {
    return it->second;
  }
  return nullptr;
}

/** @brief   Evict layout from the cache.
 *  @param   layout_id Id of the layout to be deleted.
 *  @return  true if layout_id is found and evicted; false if not found.
 */
bool TableCatalogEntry::EvictLayout(oid_t layout_id) {
  if (!valid_layout_catalog_entries_) return false;

  // find layout from the cache
  auto it = layout_catalog_entries_.find(layout_id);
  if (it == layout_catalog_entries_.end()) {
    return false;  // layout_id not found in cache
  }

  auto layout = it->second;
  PELOTON_ASSERT(layout);
  layout_catalog_entries_.erase(it);
  return true;
}

TableCatalog::~TableCatalog() {}

/*@brief   private function for initialize schema of pg_table
 * @return  unqiue pointer to schema
 */
std::unique_ptr<catalog::Schema> TableCatalog::InitializeSchema() {
  const std::string primary_key_constraint_name = "primary_key";
  const std::string not_null_constraint_name = "not_null";

  auto table_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "table_oid", true);
  table_id_column.AddConstraint(catalog::Constraint(
      ConstraintType::PRIMARY, primary_key_constraint_name));
  table_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto table_name_column = catalog::Column(type::TypeId::VARCHAR, max_name_size_,
                                           "table_name", false);
  table_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto schema_name_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size_, "schema_name", false);
  schema_name_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto database_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "database_oid", true);
  database_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto version_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "version_id", true);
  version_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  auto default_layout_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "default_layout_oid", true);
  default_layout_id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, not_null_constraint_name));

  std::unique_ptr<catalog::Schema> table_catalog_schema(new catalog::Schema(
      {table_id_column, table_name_column, schema_name_column,
       database_id_column, version_id_column, default_layout_id_column}));

  return table_catalog_schema;
}

/*@brief   insert a tuple about table info into pg_table
 * @param   table_oid
 * @param   table_name
 * @param   database_oid
 * @param   txn     TransactionContext
 * @return  Whether insertion is Successful
 */
bool TableCatalog::InsertTable(concurrency::TransactionContext *txn,
                               oid_t database_oid,
                               const std::string &schema_name,
                               oid_t table_oid,
                               const std::string &table_name,
                               oid_t layout_oid,
                               type::AbstractPool *pool) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(table_name, nullptr);
  auto val2 = type::ValueFactory::GetVarcharValue(schema_name, nullptr);
  auto val3 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val4 = type::ValueFactory::GetIntegerValue(0);
  auto val5 = type::ValueFactory::GetIntegerValue(layout_oid);

  tuple->SetValue(TableCatalog::ColumnId::TABLE_OID, val0, pool);
  tuple->SetValue(TableCatalog::ColumnId::TABLE_NAME, val1, pool);
  tuple->SetValue(TableCatalog::ColumnId::SCHEMA_NAME, val2, pool);
  tuple->SetValue(TableCatalog::ColumnId::DATABASE_OID, val3, pool);
  tuple->SetValue(TableCatalog::ColumnId::VERSION_ID, val4, pool);
  tuple->SetValue(TableCatalog::ColumnId::DEFAULT_LAYOUT_OID, val5, pool);

  // Insert the tuple
  return InsertTuple(txn, std::move(tuple));
}

/*@brief   delete a tuple about table info from pg_table(using index scan)
 * @param   table_oid
 * @param   txn     TransactionContext
 * @return  Whether deletion is successful
 */
bool TableCatalog::DeleteTable(concurrency::TransactionContext *txn, oid_t table_oid) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  // evict from cache
  auto table_object = txn->catalog_cache.GetCachedTableObject(database_oid_,
  		                                                        table_oid);
  if (table_object) {
    auto database_object =
        DatabaseCatalog::GetInstance(nullptr,
                                     nullptr,
                                     nullptr)->GetDatabaseCatalogEntry(txn,
                                                                       database_oid_);
    database_object->EvictTableCatalogEntry(table_oid);
  }

  return DeleteWithIndexScan(txn, index_offset, values);
}

/*@brief   read table catalog object from pg_table using table oid
 * @param   table_oid
 * @param   txn     TransactionContext
 * @return  table catalog object
 */
std::shared_ptr<TableCatalogEntry> TableCatalog::GetTableCatalogEntry(
    concurrency::TransactionContext *txn,
    oid_t table_oid) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto table_object = txn->catalog_cache.GetCachedTableObject(database_oid_,
  		                                                        table_oid);
  if (table_object) return table_object;

  // cache miss, get from pg_table
  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of table_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto table_object =
        std::make_shared<TableCatalogEntry>(txn, (*result_tiles)[0].get());
    // insert into cache
    auto database_object =
        DatabaseCatalog::GetInstance(nullptr,
                                     nullptr,
                                     nullptr)->GetDatabaseCatalogEntry(txn,
                                                                       database_oid_);
    PELOTON_ASSERT(database_object);
    bool success = database_object->InsertTableCatalogEntry(table_object);
    PELOTON_ASSERT(success == true);
    (void)success;
    return table_object;
  } else {
    LOG_DEBUG("Found %lu table with oid %u", result_tiles->size(), table_oid);
  }

  // return empty object if not found
  return nullptr;
}

/*@brief   read table catalog object from pg_table using table name + database
 * oid
 * @param   table_name
 * @param   schema_name
 * @param   database_oid
 * @param   txn     TransactionContext
 * @return  table catalog object
 */
std::shared_ptr<TableCatalogEntry> TableCatalog::GetTableCatalogEntry(
    concurrency::TransactionContext *txn,
    const std::string &schema_name,
    const std::string &table_name) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_oid_);
  if (database_object) {
    auto table_object =
        database_object->GetTableCatalogEntry(table_name, schema_name, true);
    if (table_object) return table_object;
  }

  // cache miss, get from pg_table
  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::SKEY_TABLE_NAME;  // Index of table_name
  std::vector<type::Value> values;
  values.push_back(
      type::ValueFactory::GetVarcharValue(table_name, nullptr).Copy());
  values.push_back(
      type::ValueFactory::GetVarcharValue(schema_name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto table_object =
        std::make_shared<TableCatalogEntry>(txn, (*result_tiles)[0].get());
    // insert into cache
    auto database_object =
        DatabaseCatalog::GetInstance(nullptr,
                                     nullptr,
                                     nullptr)->GetDatabaseCatalogEntry(txn,
                                                                       database_oid_);
    PELOTON_ASSERT(database_object);
    bool success = database_object->InsertTableCatalogEntry(table_object);
    PELOTON_ASSERT(success == true);
    (void)success;
    return table_object;
  }

  // return empty object if not found
  return nullptr;
}

/*@brief   read table catalog objects from pg_table using database oid
 * @param   database_oid
 * @param   txn     TransactionContext
 * @return  table catalog objects
 */
std::unordered_map<oid_t, std::shared_ptr<TableCatalogEntry>>
TableCatalog::GetTableCatalogEntries(concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object =
      DatabaseCatalog::GetInstance(nullptr,
                                   nullptr,
                                   nullptr)->GetDatabaseCatalogEntry(txn,
                                                                     database_oid_);
  PELOTON_ASSERT(database_object != nullptr);
  if (database_object->IsValidTableCatalogEntries()) {
    return database_object->GetTableCatalogEntries(true);
  }

  // cache miss, get from pg_table
  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::SKEY_DATABASE_OID;  // Index of database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid_).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);

  for (auto &tile : (*result_tiles)) {
    for (auto tuple_id : *tile) {
      auto table_object =
          std::make_shared<TableCatalogEntry>(txn, tile.get(), tuple_id);
      database_object->InsertTableCatalogEntry(table_object);
    }
  }

  database_object->SetValidTableCatalogEntries(true);
  return database_object->GetTableCatalogEntries();
}

/*@brief    update version id column within pg_table
 * @param   update_val   the new(updated) version id
 * @param   table_oid    which table to be updated
 * @param   txn          TransactionContext
 * @return  Whether update is successful
 */
bool TableCatalog::UpdateVersionId(concurrency::TransactionContext *txn,
                                   oid_t table_oid,
                                   oid_t update_val) {
  std::vector<oid_t> update_columns({ColumnId::VERSION_ID});  // version_id
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of table_oid
  // values to execute index scan
  std::vector<type::Value> scan_values;
  scan_values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  // values to update
  std::vector<type::Value> update_values;
  update_values.push_back(
      type::ValueFactory::GetIntegerValue(update_val).Copy());

  // get table object, then evict table object
  auto table_object = txn->catalog_cache.GetCachedTableObject(database_oid_,
  		                                                        table_oid);
  if (table_object) {
    auto database_object =
        DatabaseCatalog::GetInstance(nullptr,
                                     nullptr,
                                     nullptr)->GetDatabaseCatalogEntry(txn,
                                                                       database_oid_);
    database_object->EvictTableCatalogEntry(table_oid);
  }

  return UpdateWithIndexScan(txn,
                             index_offset,
                             scan_values,
                             update_columns,
                             update_values);
}

/*@brief    update default layout oid column within pg_table
 * @param   update_val   the new(updated) default layout oid
 * @param   table_oid    which table to be updated
 * @param   txn          TransactionContext
 * @return  Whether update is successful
 */
bool TableCatalog::UpdateDefaultLayoutOid(concurrency::TransactionContext *txn,
                                          oid_t table_oid,
                                          oid_t update_val) {
  std::vector<oid_t> update_columns({ColumnId::DEFAULT_LAYOUT_OID});  // defalut_layout_oid
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of table_oid
  // values to execute index scan
  std::vector<type::Value> scan_values;
  scan_values.push_back(type::ValueFactory::GetIntegerValue(table_oid).Copy());
  // values to update
  std::vector<type::Value> update_values;
  update_values.push_back(
      type::ValueFactory::GetIntegerValue(update_val).Copy());

  // get table object, then evict table object
  auto table_object = txn->catalog_cache.GetCachedTableObject(database_oid_,
  		                                                        table_oid);
  if (table_object) {
    auto database_object =
        DatabaseCatalog::GetInstance(nullptr,
                                     nullptr,
                                     nullptr)->GetDatabaseCatalogEntry(txn,
                                                                       database_oid_);
    database_object->EvictTableCatalogEntry(table_oid);
  }

  return UpdateWithIndexScan(txn,
                             index_offset,
                             scan_values,
                             update_columns,
                             update_values);
}


}  // namespace catalog
}  // namespace peloton
