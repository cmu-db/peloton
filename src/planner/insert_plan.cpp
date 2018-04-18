//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_plan.cpp
//
// Identification: src/planner/insert_plan.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/insert_plan.h"

#include "catalog/catalog.h"
#include "expression/constant_value_expression.h"
#include "storage/data_table.h"
#include "type/ephemeral_pool.h"
#include "type/value_factory.h"

namespace peloton {
namespace planner {

InsertPlan::InsertPlan(
    storage::DataTable *table, const std::vector<std::string> *columns,
    const std::vector<std::vector<
        std::unique_ptr<expression::AbstractExpression>>> *insert_values)
    : target_table_(table), bulk_insert_count_(insert_values->size()) {
  LOG_TRACE("Creating an Insert Plan with multiple expressions");
  PELOTON_ASSERT(target_table_);

  // We assume we are not processing a prepared statement insert.
  // Only after we have finished processing, do we know if it is a
  // PS or not a PS.
  bool is_prepared_stmt = false;
  auto *schema = target_table_->GetSchema();
  auto schema_col_count = schema->GetColumnCount();
  insert_to_schema_.resize(columns->size());
  // initialize mapping from schema cols to insert values vector.
  // will be updated later based on insert columns and values
  schema_to_insert_.resize(schema_col_count);
  for (uint32_t idx = 0; idx < schema_col_count; idx++) {
    schema_to_insert_[idx].in_insert_cols = false;
    schema_to_insert_[idx].set_value = false;
    schema_to_insert_[idx].val_idx = 0;
    // remember the column types
    schema_to_insert_[idx].type = schema->GetType(idx);
  }

  if (columns->empty()) {
    // INSERT INTO table_name VALUES (val1, val2, ...), (val1, val2, ...)
    for (uint32_t tuple_idx = 0; tuple_idx < insert_values->size();
         tuple_idx++) {
      auto &values = (*insert_values)[tuple_idx];
      PELOTON_ASSERT(values.size() <= schema_col_count);
      // uint32_t param_idx = 0;
      for (uint32_t column_id = 0; column_id < values.size(); column_id++) {
        auto &exp = values[column_id];
        auto exp_ptr = exp.get();
        auto ret_bool = ProcessValueExpr(exp_ptr, column_id);
        // there is no column specification, so we have a
        // direct mapping between schema cols and the value vector
        schema_to_insert_[column_id].in_insert_cols = true;
        schema_to_insert_[column_id].val_idx = column_id;
        if (ret_bool == true) {
          is_prepared_stmt = true;
        }
      }
      // for remaining columns, insert defaults
      for (uint32_t column_id = values.size(); column_id != schema_col_count;
           ++column_id) {
        SetDefaultValue(column_id);
      }
    }
  } else {
    // INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...);
    // Columns may be in any order. Values may include constants.
    PELOTON_ASSERT(columns->size() <= schema_col_count);
    // construct the mapping between schema cols and insert cols
    ProcessColumnSpec(columns);
    for (uint32_t tuple_idx = 0; tuple_idx < insert_values->size();
         tuple_idx++) {
      auto &values = (*insert_values)[tuple_idx];
      PELOTON_ASSERT(values.size() <= schema_col_count);

      for (uint32_t idx = 0; idx < schema_col_count; idx++) {
        if (schema_to_insert_[idx].in_insert_cols) {
          // this schema column is present in the insert columns spec.
          // get index into values
          auto val_idx = schema_to_insert_[idx].val_idx;
          auto &exp = values[val_idx];
          auto exp_ptr = exp.get();
          bool ret_bool = ProcessValueExpr(exp_ptr, idx);
          if (ret_bool) {
            is_prepared_stmt = true;
          }
        } else {
          // schema column not present in insert columns spec. Set
          // column to its default value
          SetDefaultValue(idx);
        }
      }

      if (is_prepared_stmt) {
        // Adjust indexes into values. When constants are present in the
        // value tuple spec., the value vector supplied by the prepared
        // statement when SetParameterValues is called, will be smaller.
        // It will not include any of the constants.
        // Adjust the mapping from schema cols -> values vector to exclude
        // the constant columns. If there are no constants, this is a no-op.
        uint32_t adjust = 0;
        for (uint32_t idx = 0; idx < columns->size(); idx++) {
          uint32_t stov_idx = insert_to_schema_[idx];
          if (schema_to_insert_[stov_idx].set_value) {
            // constant, not present in PS values
            adjust++;
          } else {
            // adjust the index
            schema_to_insert_[stov_idx].val_idx -= adjust;
          }
        }
      }
    }
  }
  if (is_prepared_stmt) {
    // We've been assuming it is not a PS and saving into the values_
    // vector. Now that we know it is a PS, we must clear those values
    // so SetParameterValues will operate correctly.
    ClearParameterValues();
  }
}

bool InsertPlan::FindSchemaColIndex(
    std::string col_name, const std::vector<catalog::Column> &tbl_columns,
    uint32_t &index) {
  for (auto tcol = tbl_columns.begin(); tcol != tbl_columns.end(); tcol++) {
    if (tcol->GetName() == col_name) {
      index = std::distance(tbl_columns.begin(), tcol);
      return true;
    }
  }
  return false;
}

void InsertPlan::ProcessColumnSpec(const std::vector<std::string> *columns) {
  auto *schema = target_table_->GetSchema();
  auto &table_columns = schema->GetColumns();
  auto usr_col_count = columns->size();

  // iterate over supplied columns
  for (size_t usr_col_id = 0; usr_col_id < usr_col_count; usr_col_id++) {
    uint32_t idx;
    auto col_name = columns->at(usr_col_id);

    // determine index of column in schema
    bool found_col = FindSchemaColIndex(col_name, table_columns, idx);
    if (not found_col) {
      throw Exception("column " + col_name + " not in table " +
                      target_table_->GetName() + " columns");
    }
    // we have values for this column
    schema_to_insert_[idx].in_insert_cols = true;
    // remember how to map schema col -> value for col in tuple
    schema_to_insert_[idx].val_idx = usr_col_id;
    // and the reverse
    insert_to_schema_[usr_col_id] = idx;
  }
}

bool InsertPlan::ProcessValueExpr(expression::AbstractExpression *expr,
                                  uint32_t schema_idx) {
  auto type = schema_to_insert_[schema_idx].type;

  if (expr == nullptr) {
    SetDefaultValue(schema_idx);
  } else if (expr->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
    auto *const_expr =
        dynamic_cast<expression::ConstantValueExpression *>(expr);
    type::Value value = const_expr->GetValue().CastAs(type);

    schema_to_insert_[schema_idx].set_value = true;
    schema_to_insert_[schema_idx].value = value;
    // save it, in case this is not a PS
    values_.push_back(value);

    return false;
  } else {
    PELOTON_ASSERT(expr->GetExpressionType() ==
                   ExpressionType::VALUE_PARAMETER);
    return true;
  }
  return false;
}

void InsertPlan::SetDefaultValue(uint32_t idx) {
  auto *schema = target_table_->GetSchema();
  type::Value *v = schema->GetDefaultValue(idx);
  type::TypeId type = schema_to_insert_[idx].type;

  if (v == nullptr)
    // null default value
    values_.push_back(type::ValueFactory::GetNullValueByType(type));
  else
    // non-null default value
    values_.push_back(*v);
}

type::AbstractPool *InsertPlan::GetPlanPool() {
  if (pool_.get() == nullptr) pool_.reset(new type::EphemeralPool());
  return pool_.get();
}

void InsertPlan::SetParameterValues(std::vector<type::Value> *values) {
  LOG_TRACE("Set Parameter Values in Insert");
  auto *schema = target_table_->GetSchema();
  auto schema_col_count = schema->GetColumnCount();

  PELOTON_ASSERT(values->size() <= schema_col_count);
  for (uint32_t idx = 0; idx < schema_col_count; idx++) {
    if (schema_to_insert_[idx].set_value) {
      values_.push_back(schema_to_insert_[idx].value);
    } else if (schema_to_insert_[idx].in_insert_cols) {
      // get index into values
      auto val_idx = schema_to_insert_[idx].val_idx;
      auto type = schema_to_insert_[idx].type;
      type::Value value = values->at(val_idx).CastAs(type);
      values_.push_back(value);
    } else {
      // not in insert cols, set default value
      SetDefaultValue(idx);
    }
  }
}

void InsertPlan::PerformBinding(BindingContext &binding_context) {
  const auto &children = GetChildren();

  if (children.size() == 1) {
    children[0]->PerformBinding(binding_context);

    auto *scan = static_cast<planner::AbstractScan *>(children[0].get());
    auto &col_ids = scan->GetColumnIds();
    for (oid_t col_id = 0; col_id < col_ids.size(); col_id++) {
      ais_.push_back(binding_context.Find(col_id));
    }
  }
  // Binding is not required if there is no child
}

hash_t InsertPlan::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);

  hash = HashUtil::CombineHashes(hash, GetTable()->Hash());
  if (GetChildren().size() == 0) {
    auto bulk_insert_count = GetBulkInsertCount();
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&bulk_insert_count));
  }

  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

bool InsertPlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = static_cast<const planner::InsertPlan &>(rhs);

  auto *table = GetTable();
  auto *other_table = other.GetTable();
  PELOTON_ASSERT(table && other_table);
  if (*table != *other_table) return false;

  if (GetChildren().size() == 0) {
    if (other.GetChildren().size() != 0) return false;

    if (GetBulkInsertCount() != other.GetBulkInsertCount()) return false;
  }

  return AbstractPlan::operator==(rhs);
}

void InsertPlan::VisitParameters(
    codegen::QueryParametersMap &map, std::vector<peloton::type::Value> &values,
    const std::vector<peloton::type::Value> &values_from_user) {
  if (GetChildren().size() == 0) {
    auto *schema = target_table_->GetSchema();
    auto columns_num = schema->GetColumnCount();

    for (uint32_t i = 0; i < values_.size(); i++) {
      auto value = values_[i];
      auto column_id = i % columns_num;
      map.Insert(expression::Parameter::CreateConstParameter(
                     value.GetTypeId(), schema->AllowNull(column_id)),
                 nullptr);
      values.push_back(value);
    }
  } else {
    PELOTON_ASSERT(GetChildren().size() == 1);
    auto *plan = const_cast<planner::AbstractPlan *>(GetChild(0));
    plan->VisitParameters(map, values, values_from_user);
  }
}

}  // namespace planner
}  // namespace peloton
