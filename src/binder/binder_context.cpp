//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// binder_context.cpp
//
// Identification: src/binder/binder_context.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "binder/binder_context.h"

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "storage/data_table.h"
#include "storage/storage_manager.h"
#include "parser/table_ref.h"

namespace peloton {
namespace binder {

void BinderContext::AddTable(const parser::TableRef* table_ref) {
  storage::DataTable* table = catalog::Catalog::GetInstance()->GetTableWithName(
      table_ref->GetDatabaseName(), table_ref->GetTableName());

  auto id_tuple = std::make_tuple(table->GetDatabaseOid(), table->GetOid());

  std::string alias = table_ref->GetTableAlias();

  if (table_alias_map.find(alias) != table_alias_map.end()) {
    throw Exception("Duplicate alias " + alias);
  }
  table_alias_map[alias] = id_tuple;
}

void BinderContext::AddTable(const std::string db_name,
                             const std::string table_name) {
  storage::DataTable* table =
      catalog::Catalog::GetInstance()->GetTableWithName(db_name, table_name);

  auto id_tuple = std::make_tuple(table->GetDatabaseOid(), table->GetOid());

  if (table_alias_map.find(table_name) != table_alias_map.end()) {
    throw Exception("Duplicate alias " + table_name);
  }
  table_alias_map[table_name] = id_tuple;
}

bool BinderContext::GetColumnPosTuple(
    std::string& col_name, std::tuple<oid_t, oid_t>& table_id_tuple,
    std::tuple<oid_t, oid_t, oid_t>& col_pos_tuple, type::TypeId& value_type) {
    try{
        auto db_id = std::get<0>(table_id_tuple);
        auto table_id = std::get<1>(table_id_tuple);
        auto schema = storage::StorageManager::GetInstance()
                    ->GetTableWithOid(db_id, table_id)
                    ->GetSchema();
        auto col_pos = schema->GetColumnID(col_name);
        if (col_pos == (oid_t)-1) return false;
        col_pos_tuple = std::make_tuple(db_id, table_id, col_pos);
        value_type = schema->GetColumn(col_pos).GetType();
        return true;
    } catch (CatalogException &e) {
        LOG_TRACE("Can't find table %d! Return false", std::get<1>(table_id_tuple));
        return false;
    }
}

bool BinderContext::GetColumnPosTuple(
    std::shared_ptr<BinderContext> current_context, std::string& col_name,
    std::tuple<oid_t, oid_t, oid_t>& col_pos_tuple, std::string& table_alias,
    type::TypeId& value_type) {
  bool find_matched = false;
  while (current_context != nullptr && !find_matched) {
    for (auto entry : current_context->table_alias_map) {
      bool get_matched =
          GetColumnPosTuple(col_name, entry.second, col_pos_tuple, value_type);
      if (get_matched) {
        if (!find_matched) {
          find_matched = true;
          table_alias = entry.first;
        } else {
          throw Exception("Ambiguous column name " + col_name);
        }
      }
    }
    current_context = current_context->upper_context;
  }
  return find_matched;
}

bool BinderContext::GetTableIdTuple(
    std::shared_ptr<BinderContext> current_context, std::string& alias,
    std::tuple<oid_t, oid_t>* id_tuple_ptr) {
  while (current_context != nullptr) {
    auto iter = current_context->table_alias_map.find(alias);
    if (iter != current_context->table_alias_map.end()) {
      *id_tuple_ptr = iter->second;
      return true;
    }
    current_context = current_context->upper_context;
  }
  return false;
}

}  // namespace binder
}  // namespace peloton
