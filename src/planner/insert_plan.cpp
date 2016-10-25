
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// insert_plan.cpp
//
// Identification: /peloton/src/planner/insert_plan.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/insert_plan.h"
#include "planner/project_info.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "parser/statement_select.h"
#include "parser/statement_insert.h"
#include "catalog/column.h"
#include "catalog/catalog.h"
#include "common/value.h"

namespace peloton {
namespace planner {
InsertPlan::InsertPlan(storage::DataTable *table, oid_t bulk_insert_count)
    : target_table_(table), bulk_insert_count(bulk_insert_count) {}

// This constructor takes in a project info
InsertPlan::InsertPlan(
    storage::DataTable *table,
    std::unique_ptr<const planner::ProjectInfo> &&project_info,
    oid_t bulk_insert_count)
    : target_table_(table),
      project_info_(std::move(project_info)),
      bulk_insert_count(bulk_insert_count) {}

// This constructor takes in a tuple
InsertPlan::InsertPlan(storage::DataTable *table,
                       std::unique_ptr<storage::Tuple> &&tuple,
                       oid_t bulk_insert_count)
    : target_table_(table),
      tuple_(std::move(tuple)),
      bulk_insert_count(bulk_insert_count) {
  LOG_TRACE("Creating an Insert Plan");
}

InsertPlan::InsertPlan(parser::InsertStatement *parse_tree,
                       oid_t bulk_insert_count)
    : bulk_insert_count(bulk_insert_count) {

  LOG_TRACE("Creating an Insert Plan");
  auto values = parse_tree->values;
  auto cols = parse_tree->columns;
  parameter_vector_.reset(new std::vector<std::pair<oid_t, oid_t>>());
  params_value_type_.reset(new std::vector<common::Type::TypeId>);

  target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
      parse_tree->GetDatabaseName(), parse_tree->GetTableName());
  if (target_table_) {
    catalog::Schema *table_schema = target_table_->GetSchema();
    // INSERT INTO table_name VALUES (val2, val2, ...)
    if (cols == NULL) {
      PL_ASSERT(values->size() == table_schema->GetColumnCount());
      std::unique_ptr<storage::Tuple> tuple(
          new storage::Tuple(table_schema, true));
      int col_cntr = 0;
      int param_index = 0;
      for (expression::AbstractExpression *elem : *values) {
        if (elem->GetExpressionType() == EXPRESSION_TYPE_VALUE_PARAMETER) {
          std::pair<oid_t, oid_t> pair =
              std::make_pair(col_cntr, param_index++);
          parameter_vector_->push_back(pair);
          params_value_type_->push_back(
              table_schema->GetColumn(col_cntr).GetType());
        } else {
          expression::ConstantValueExpression *const_expr_elem =
              dynamic_cast<expression::ConstantValueExpression *>(elem);
          common::Value const_expr_elem_val = (const_expr_elem->GetValue());
          switch (const_expr_elem->GetValueType()) {
            case common::Type::VARCHAR:
            case common::Type::VARBINARY:
              tuple->SetValue(col_cntr, const_expr_elem_val,
                              GetPlanPool());
              break;
            default: {
              tuple->SetValue(col_cntr, const_expr_elem_val, nullptr);
            }
          }
        }
        ++col_cntr;
      }
      tuple_ = std::move(tuple);
    }
    // INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...);
    else {
      // columns has to be less than or equal that of schema
      PL_ASSERT(cols->size() <= table_schema->GetColumnCount());
      std::unique_ptr<storage::Tuple> tuple(
          new storage::Tuple(table_schema, true));
      int col_cntr = 0;
      auto &table_columns = table_schema->GetColumns();
      auto query_columns = parse_tree->columns;
      for (catalog::Column const &elem : table_columns) {
        std::size_t pos = std::find(query_columns->begin(),
                                    query_columns->end(), elem.GetName()) -
                          query_columns->begin();

        // If the column does not exist, insert a null value
        if (pos >= query_columns->size()) {
          //switch (elem.GetType()) {
            //case common::Type::VARCHAR:
            //case common::Type::VARBINARY: {
            //  tuple->SetValue(col_cntr, *common::ValueFactory::GetNullStringValue(),
            //                  GetPlanPool());
            //  break;
            //}
            //default:
              tuple->SetValue(col_cntr,
                  common::ValueFactory::GetNullValueByType(elem.GetType()),nullptr);
          //}
        } else {
          // If it's varchar or varbinary then use data pool, otherwise allocate
          // inline
          auto data_pool = GetPlanPool();
          if (elem.GetType() != common::Type::VARCHAR &&
              elem.GetType() != common::Type::VARBINARY)
            data_pool = nullptr;

          LOG_TRACE("Column %d found in INSERT query, ExpressionType: %s",
                    col_cntr,
                    ExpressionTypeToString(values->at(pos)->GetExpressionType())
                        .c_str());

          if (values->at(pos)->GetExpressionType() ==
              EXPRESSION_TYPE_VALUE_PARAMETER) {
            std::pair<oid_t, oid_t> pair = std::make_pair(col_cntr, pos);
            parameter_vector_->push_back(pair);
            params_value_type_->push_back(
                table_schema->GetColumn(col_cntr).GetType());
          } else {
            expression::ConstantValueExpression *const_expr_elem =
                dynamic_cast<expression::ConstantValueExpression *>(
                    values->at(pos));
            common::Value val = (const_expr_elem->GetValue());
            tuple->SetValue(col_cntr, val, data_pool);
          }
        }

        ++col_cntr;
      }
      tuple_ = std::move(tuple);
    }
    LOG_TRACE("Tuple to be inserted: %s", tuple_->GetInfo().c_str());
  } else {
    LOG_TRACE("Table does not exist!");
  }
}

common::VarlenPool *InsertPlan::GetPlanPool() {
  // construct pool if needed
  if (pool_.get() == nullptr) pool_.reset(
    new common::VarlenPool(BACKEND_TYPE_MM));
  // return pool
  return pool_.get();
}

void InsertPlan::SetParameterValues(std::vector<common::Value> *values) {
  PL_ASSERT(values->size() == parameter_vector_->size());
  LOG_TRACE("Set Parameter Values in Insert");
  for (unsigned int i = 0; i < values->size(); ++i) {
    auto param_type = params_value_type_->at(i);
    auto value = values->at(parameter_vector_->at(i).second);
    // LOG_TRACE("Setting value of type %s",
    // ValueTypeToString(param_type).c_str());
    switch (param_type) {
      case common::Type::VARBINARY:
      case common::Type::VARCHAR: {
        common::Value val = (value.CastAs(param_type));
        tuple_->SetValue(parameter_vector_->at(i).first, val, GetPlanPool());
        break;
      }
      default: {
        common::Value val = (value.CastAs(param_type));
        tuple_->SetValue(parameter_vector_->at(i).first, val, nullptr);
      }
    }
  }
}
}
}
