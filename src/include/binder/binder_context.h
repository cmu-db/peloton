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

#include <string>
#include <unordered_map>

#include "type/types.h"

namespace peloton {

namespace parser {
struct TableRef;
}

namespace concurrency {
class TransactionContext;
}

namespace catalog{
class TableCatalogObject;
}

namespace binder {

// Store the visible table alias and the corresponding <db_id, table_id> tuple.
// Also record the upper level context when traversing into the nested query.
// This context keep track of all the table alias that the column in the current
// level
// can bind to.
class BinderContext {
 public:
  BinderContext(std::shared_ptr<BinderContext> upper_context = nullptr) : upper_context_(upper_context) {
    if (upper_context != nullptr)
      depth_ = upper_context->depth_ + 1;
  }

  // Update the table alias map given a table reference (in the from clause)
  void AddRegularTable(parser::TableRef *table_ref,
                       const std::string default_database_name,
                       concurrency::TransactionContext *txn);

  // Update the table alias map given a table reference (in the from clause)
  void AddRegularTable(const std::string db_name,
                       const std::string table_name,
                       const std::string table_alias,
                       concurrency::TransactionContext *txn);

  // Update the nested table alias map
  void AddNestedTable(const std::string table_alias, std::vector<std::unique_ptr<expression::AbstractExpression>>& select_list);

  static bool HasTables(std::shared_ptr<BinderContext> current_context) {
    if (current_context == nullptr) return false;
    if (!current_context->regular_table_alias_map_.empty() || 
        !current_context->nested_table_alias_map_.empty()) return true;
    return false;
  }

  // Construct the column position tuple given column name and the
  // corresponding table obj. Also set the value type
  // Note that this is just a helper function and it is independent of
  // the context.
  static bool GetColumnPosTuple(
      const std::string& col_name, std::shared_ptr<catalog::TableCatalogObject> table_obj,
      std::tuple<oid_t, oid_t, oid_t>& col_pos_tuple, type::TypeId& value_type);

  // Construct the column position tuple given only the column name and the
  // context. Also set the value type based on column type
  // This function is used when the table alias is absent in the
  // TupleValueExpression
  static bool GetColumnPosTuple(std::shared_ptr<BinderContext> current_context,
                                const std::string& col_name,
                                std::tuple<oid_t, oid_t, oid_t>& col_pos_tuple,
                                std::string& table_alias,
                                type::TypeId& value_type ,int& depth);

  // Construct the table obj given the table alias
  static bool GetRegularTableObj(
      std::shared_ptr<BinderContext> current_context, std::string &alias,
      std::shared_ptr<catalog::TableCatalogObject> &table_obj, int& depth);

  static bool CheckNestedTableColumn(
      std::shared_ptr<BinderContext> current_context, std::string& alias,
      std::string& col_name, type::TypeId& value_type, int& depth);


  std::shared_ptr<BinderContext> GetUpperContext() { return upper_context_; }

  void SetUpperContext(std::shared_ptr<BinderContext> upper_context) {
    upper_context_ = upper_context;
  }
  
  void inline SetDepth(int depth) { depth_ = depth; }
  
  int inline GetDepth() { return depth_; }

  void GenerateAllColumnExpressions(
      std::vector<std::unique_ptr<expression::AbstractExpression>>& exprs);

 private:
  // Map table alias to table obj
  std::unordered_map<std::string, std::shared_ptr<catalog::TableCatalogObject>> regular_table_alias_map_;
  std::unordered_map<std::string, std::unordered_map<std::string, type::TypeId>> nested_table_alias_map_;
  std::shared_ptr<BinderContext> upper_context_;
  int depth_ = 0;
};

}  // namespace binder
}  // namespace peloton
