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
#include "expression/tuple_value_expression.h"
#include "storage/storage_manager.h"

namespace peloton {
namespace binder {

void BinderContext::AddRegularTable(parser::TableRef *table_ref,
                                    const std::string default_database_name,
                                    concurrency::Transaction *txn) {
  table_ref->TryBindDatabaseName(default_database_name);
  auto table_alias = table_ref->GetTableAlias();
  if (table_alias == nullptr)
    table_alias = table_ref->GetTableName();
  AddRegularTable(table_ref->GetDatabaseName(), table_ref->GetTableName(), table_alias, txn);
}

void BinderContext::AddRegularTable(const std::string db_name,
                                    const std::string table_name,
                                    const std::string table_alias,
                                    concurrency::Transaction *txn) {
  // using catalog object to retrieve meta-data
  auto table_object =
      catalog::Catalog::GetInstance()->GetTableObject(db_name, table_name, txn);

  if (regular_table_alias_map_.find(table_alias) != regular_table_alias_map_.end() ||
      nested_table_alias_map_.find(table_alias) != nested_table_alias_map_.end()) {
    throw Exception("Duplicate alias " + table_alias);
  }
  regular_table_alias_map_[table_alias] = table_object;
}

void BinderContext::AddNestedTable(const std::string table_alias,
                                   std::vector<expression::AbstractExpression *> *select_list) {
  if (regular_table_alias_map_.find(table_alias) != regular_table_alias_map_.end() ||
      nested_table_alias_map_.find(table_alias) != nested_table_alias_map_.end())
    throw Exception("Duplicate alias " + table_alias);

  std::unordered_map<std::string, type::TypeId> column_alias_map;
  for (auto& expr : *select_list) {
    std::string alias;
    if (!expr->alias.empty()) {
      alias = expr->alias;
    } else if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
      auto tv_expr = reinterpret_cast<expression::TupleValueExpression*>(expr);
      alias = tv_expr->GetColumnName();
    }
    std::transform(alias.begin(), alias.end(), alias.begin(),
                   ::tolower);
    column_alias_map[alias] = expr->GetValueType();
  }
  nested_table_alias_map_[table_alias] = column_alias_map;
}

bool BinderContext::GetColumnPosTuple(
    const std::string& col_name, std::shared_ptr<catalog::TableCatalogObject> table_obj,
    std::tuple<oid_t, oid_t, oid_t>& col_pos_tuple, type::TypeId& value_type) {
  try {
    auto column_object = table_obj->GetColumnObject(col_name);
    if (column_object == nullptr) return false;

    oid_t col_pos = column_object->column_id;
    col_pos_tuple = std::make_tuple(table_obj->database_oid, table_obj->table_oid, col_pos);
    value_type = column_object->column_type;
    return true;
  } catch (CatalogException& e) {
    LOG_TRACE("Can't find table %d! Return false", std::get<1>(table_id_tuple));
    return false;
  }
}

bool BinderContext::GetColumnPosTuple(
    std::shared_ptr<BinderContext> current_context, const std::string& col_name,
    std::tuple<oid_t, oid_t, oid_t>& col_pos_tuple, std::string& table_alias,
    type::TypeId& value_type) {
  bool find_matched = false;
  while (current_context != nullptr && !find_matched) {
    // Check regular table
    for (auto entry : current_context->regular_table_alias_map_) {
      bool get_matched = GetColumnPosTuple(col_name, entry.second,
                                           col_pos_tuple, value_type);
      if (get_matched) {
        if (!find_matched) {
          // First match
          find_matched = true;
          table_alias = entry.first;
        } else {
          throw Exception("Ambiguous column name " + col_name);
        }
      }
    }
    // Check nested table
    for (auto entry : current_context->nested_table_alias_map_) {
      bool get_match = entry.second.find(col_name) != entry.second.end();
      if (get_match) {
        if (!find_matched) {
          // First match
          find_matched = true;
          table_alias = entry.first;
          value_type = entry.second[col_name];
        } else {
          throw Exception("Ambiguous column name " + col_name);
        }
      }
    }
    current_context = current_context->GetUpperContext();
  }
  return find_matched;
}

bool BinderContext::GetRegularTableObj(
    std::shared_ptr<BinderContext> current_context, std::string &alias,
    std::shared_ptr<catalog::TableCatalogObject> &table_obj) {
  while (current_context != nullptr) {
    auto iter = current_context->regular_table_alias_map_.find(alias);
    if (iter != current_context->regular_table_alias_map_.end()) {
      table_obj = iter->second;
      return true;
    }
    current_context = current_context->GetUpperContext();
  }
  return false;
}

bool BinderContext::CheckNestedTableColumn(std::shared_ptr<BinderContext> current_context,
                                           std::string &alias,
                                           std::string &col_name,
                                           type::TypeId& value_type) {
  while (current_context != nullptr) {
    auto iter = current_context->nested_table_alias_map_.find(alias);
    if (iter != current_context->nested_table_alias_map_.end()) {
      auto col_iter = iter->second.find(col_name);
      if (col_iter == iter->second.end()) {
        throw Exception("Cannot find column " + col_name);
      }
      value_type = col_iter->second;
      return true;
    }
    current_context = current_context->GetUpperContext();
  }
  return false;
}

}  // namespace binder
}  // namespace peloton
