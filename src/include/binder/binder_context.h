//
// Created by patrick on 15/02/17.
//

#include <unordered_map>
#include <string>
#include "catalog/catalog.h"
#include "type/types.h"


namespace peloton {
namespace binder {

class BinderContext;

class BinderContext {
 public:
  std::shared_ptr<BinderContext> upper_context;

  BinderContext() {
    upper_context = nullptr;
  }

  // Update the table alias map given a table reference (in the from clause)
  void AddTable(const parser::TableRef* table_ref) {
    storage::DataTable *table =
        catalog::Catalog::GetInstance()->GetTableWithName(
            table_ref->GetDatabaseName(),
            table_ref->GetTableName());

    auto id_tuple = std::make_tuple(table->GetDatabaseOid(), table->GetOid());

    std::string alias = table_ref->GetTableAlias();
    if (table_ref->alias != nullptr) {
      alias = table_ref->alias;
    }
    else {
      alias = table_ref->GetTableName();
    }

    if (table_alias_map.find(alias) != table_alias_map.end()) {
      throw Exception("Duplicate alias "+alias);
    }
    table_alias_map[alias] = id_tuple;
  }

  // Construct the column position tuple given column name and the
  // corresponding tabld id tuple.
  // Note that this is just a helper function and it is independent of
  // the context.
  static bool GetColumnPosTuple(std::string& col_name,
                         std::tuple<oid_t, oid_t>& table_id_tuple,
                         std::tuple<oid_t, oid_t, oid_t>* col_pos_tuple) {
    auto db_id = std::get<0>(table_id_tuple);
    auto table_id = std::get<1>(table_id_tuple);
    auto schema = catalog::Catalog::GetInstance()->
        GetTableWithOid(db_id, table_id)->GetSchema();
    auto col_pos = schema->GetColumnID(col_name);
    if (col_pos == (oid_t)-1)
      return false;
    *col_pos_tuple = std::make_tuple(db_id, table_id, col_pos);
    return true;
  }

  // Construct the column position tuple given only the column name and the context.
  // This function is used when the table alias is absent in the
  // TupleValueExpression
  static bool GetColumnPosTuple(std::shared_ptr<BinderContext> current_context,
                                std::string& col_name,
                                std::tuple<oid_t, oid_t, oid_t>* col_pos_tuple) {
    bool find_matched = false;
    while (current_context != nullptr && !find_matched) {
      for (auto entry: current_context->table_alias_map) {
        bool get_matched = GetColumnPosTuple(col_name, entry.second, col_pos_tuple);
        if (!find_matched) {
          find_matched = get_matched;
        } else if (get_matched)
          throw Exception("Ambiguous column name " + col_name);
      }
      current_context = current_context->upper_context;
    }
    return find_matched;
  }

  // Construct the table id tuple given the table alias
  static bool GetTableIdTuple(std::shared_ptr<BinderContext> current_context,
                       std::string& alias,
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


 private:
  // When alias is not set, set its table name
  std::unordered_map<std::string, std::tuple<oid_t, oid_t>> table_alias_map;
};
}
}

