//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// binder_context.h
//
// Identification: src/include/binder/binder_context.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unordered_map>
#include <string>
#include "type/types.h"

namespace parser {
struct TableRef;
}

namespace peloton {
namespace binder {

class BinderContext;

// Store the visible table alias and the corresponding <db_id, table_id> tuple.
// Also record the upper level context when traversing into the nested query.
// This context keep track of all the table alias that the column in the current
// level
// can bind to.
class BinderContext {
 public:
  std::shared_ptr<BinderContext> upper_context;

  BinderContext() { upper_context = nullptr; }

  // Update the table alias map given a table reference (in the from clause)
  void AddTable(const parser::TableRef* table_ref);

  // Update the table alias map given a table reference (in the from clause)
  void AddTable(const std::string db_name, const std::string table_name);

  // Construct the column position tuple given column name and the
  // corresponding tabld id tuple. Also set the value type
  // Note that this is just a helper function and it is independent of
  // the context.
  static bool GetColumnPosTuple(std::string& col_name,
                                std::tuple<oid_t, oid_t>& table_id_tuple,
                                std::tuple<oid_t, oid_t, oid_t>& col_pos_tuple,
                                type::Type::TypeId& value_type);

  // Construct the column position tuple given only the column name and the
  // context. Also set the value type based on column type
  // This function is used when the table alias is absent in the
  // TupleValueExpression
  static bool GetColumnPosTuple(std::shared_ptr<BinderContext> current_context,
                                std::string& col_name,
                                std::tuple<oid_t, oid_t, oid_t>& col_pos_tuple,
                                std::string& table_alias,
                                type::Type::TypeId& value_type);

  // Construct the table id tuple given the table alias
  static bool GetTableIdTuple(std::shared_ptr<BinderContext> current_context,
                              std::string& alias,
                              std::tuple<oid_t, oid_t>* id_tuple_ptr);

 private:
  // Map table alias to <db_id, table_id>
  std::unordered_map<std::string, std::tuple<oid_t, oid_t>> table_alias_map;
};
}  // binder
}  // peloton
