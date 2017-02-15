//
// Created by patrick on 15/02/17.
//

#include <map>
#include <string>
#include "catalog/catalog.h"


namespace peloton {
namespace binder {

class BinderContext;

class BinderContext {
 public:
  BinderContext() {}
  std::shared_ptr<BinderContext> upper_context;

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
      throw Exception("Ambiguous alias");
    }
    table_alias_map[alias] = id_tuple;
  }

  bool GetTableIdTuple(std::string alias, std::tuple<oid_t, oid_t>* id_tuple_ptr) {
    auto iter = table_alias_map.find(alias);
    if (iter == table_alias_map.end()) {
      return false;
    }
    *id_tuple_ptr = iter->second;
    return true;
  }


 private:
  // When alias is not set, set its table name
  std::map<std::string, std::tuple<oid_t, oid_t>> table_alias_map;
};
}
}

