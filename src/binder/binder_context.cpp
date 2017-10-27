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
#include "catalog/column_catalog.h"
#include "catalog/table_catalog.h"
#include "parser/table_ref.h"

namespace peloton {
namespace binder {

void BinderContext::AddTable(const parser::TableRef* table_ref,
                             concurrency::Transaction* txn) {
  // using catalog object to retrieve meta-data
  auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
      table_ref->GetDatabaseName(), table_ref->GetTableName(), txn);

  auto id_tuple =
      std::make_tuple(table_object->database_oid, table_object->table_oid);

  std::string alias = table_ref->GetTableAlias();

  if (table_alias_map.find(alias) != table_alias_map.end()) {
    throw Exception("Duplicate alias " + alias);
  }
  table_alias_map[alias] = id_tuple;
}

void BinderContext::AddTable(const std::string db_name,
                             const std::string table_name,
                             concurrency::Transaction* txn) {
  // using catalog object to retrieve meta-data
  auto table_object =
      catalog::Catalog::GetInstance()->GetTableObject(db_name, table_name, txn);

  auto id_tuple =
      std::make_tuple(table_object->database_oid, table_object->table_oid);

  if (table_alias_map.find(table_name) != table_alias_map.end()) {
    throw Exception("Duplicate alias " + table_name);
  }
  table_alias_map[table_name] = id_tuple;
}

bool BinderContext::GetColumnPosTuple(
    std::string& col_name, std::tuple<oid_t, oid_t>& table_id_tuple,
    std::tuple<oid_t, oid_t, oid_t>& col_pos_tuple, type::TypeId& value_type,
    concurrency::Transaction* txn) {
  try {
    auto db_oid = std::get<0>(table_id_tuple);
    auto table_oid = std::get<1>(table_id_tuple);

    // get table object first and use the column name to get column object
    auto table_object =
        catalog::Catalog::GetInstance()->GetTableObject(db_oid, table_oid, txn);
    auto column_object = table_object->GetColumnObject(col_name);
    if (column_object == nullptr) return false;

    oid_t col_pos = column_object->column_id;
    col_pos_tuple = std::make_tuple(db_oid, table_oid, col_pos);
    value_type = column_object->column_type;
    return true;
  } catch (CatalogException& e) {
    LOG_TRACE("Can't find table %d! Return false", std::get<1>(table_id_tuple));
    return false;
  }
}

bool BinderContext::GetColumnPosTuple(
    std::shared_ptr<BinderContext> current_context, std::string& col_name,
    std::tuple<oid_t, oid_t, oid_t>& col_pos_tuple, std::string& table_alias,
    type::TypeId& value_type, concurrency::Transaction* txn) {
  bool find_matched = false;
  while (current_context != nullptr && !find_matched) {
    for (auto entry : current_context->table_alias_map) {
      bool get_matched = GetColumnPosTuple(col_name, entry.second,
                                           col_pos_tuple, value_type, txn);
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
