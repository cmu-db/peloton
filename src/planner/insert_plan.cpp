
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
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "type/value.h"
#include "parser/insert_statement.h"
#include "parser/select_statement.h"
#include "planner/project_info.h"
#include "storage/data_table.h"
#include "storage/tuple.h"

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
    : target_table_(table), bulk_insert_count(bulk_insert_count) {
  tuples_.push_back(std::move(tuple));
  LOG_TRACE("Creating an Insert Plan");
}

InsertPlan::InsertPlan(
    storage::DataTable *table, std::vector<char *> *columns,
    std::vector<std::vector<peloton::expression::AbstractExpression *> *> *
        insert_values)
    : bulk_insert_count(insert_values->size()) {

  parameter_vector_.reset(new std::vector<std::tuple<oid_t, oid_t, oid_t>>());
  params_value_type_.reset(new std::vector<type::Type::TypeId>);

  target_table_ = table;

  if (target_table_) {
    const catalog::Schema *table_schema = target_table_->GetSchema();
    // INSERT INTO table_name VALUES (val2, val2, ...)
    if (columns == NULL) {
      for (uint32_t tuple_idx = 0; tuple_idx < insert_values->size();
           tuple_idx++) {
        auto values = (*insert_values)[tuple_idx];
        PL_ASSERT(values->size() <= table_schema->GetColumnCount());
        std::unique_ptr<storage::Tuple> tuple(
            new storage::Tuple(table_schema, true));
        int col_cntr = 0;
        int param_index = 0;
        for (expression::AbstractExpression *elem : *values) {
          // Default value to be inserted
          if (elem == nullptr) {
            type::Value *v = table_schema->GetDefaultValue(col_cntr);
            tuple->SetValue(col_cntr, std::move(*v), nullptr);
          } else if (elem->GetExpressionType() == ExpressionType::VALUE_PARAMETER) {
            std::tuple<oid_t, oid_t, oid_t> pair =
                std::make_tuple(tuple_idx, col_cntr, param_index++);
            parameter_vector_->push_back(pair);
            params_value_type_->push_back(
                table_schema->GetColumn(col_cntr).GetType());
          } else {
            expression::ConstantValueExpression *const_expr_elem =
                dynamic_cast<expression::ConstantValueExpression *>(elem);
            type::Value const_expr_elem_val = (const_expr_elem->GetValue());
            switch (const_expr_elem->GetValueType()) {
              case type::Type::VARCHAR:
              case type::Type::VARBINARY:
                tuple->SetValue(col_cntr, const_expr_elem_val, GetPlanPool());
                break;
              default: {
                tuple->SetValue(col_cntr, const_expr_elem_val, nullptr);
              }
            }
          }
          ++col_cntr;
        }
        tuples_.push_back(std::move(tuple));
      }
    }
    // INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...);
    else {
      // columns has to be less than or equal that of schema
      for (uint32_t tuple_idx = 0; tuple_idx < insert_values->size();
           tuple_idx++) {
        auto values = (*insert_values)[tuple_idx];
        PL_ASSERT(columns->size() <= table_schema->GetColumnCount());
        std::unique_ptr<storage::Tuple> tuple(
            new storage::Tuple(table_schema, true));
        int col_cntr = 0;
        int param_index = 0;
        auto &table_columns = table_schema->GetColumns();
        auto query_columns = columns;
        for (catalog::Column const &elem : table_columns) {
          std::size_t pos = std::find(query_columns->begin(),
                                      query_columns->end(), elem.GetName()) -
                            query_columns->begin();

          // If the column does not exist, insert a null value
          if (pos >= query_columns->size()) {
            tuple->SetValue(col_cntr, type::ValueFactory::GetNullValueByType(
                                          elem.GetType()),
                            nullptr);
          } else {
            // If it's varchar or varbinary then use data pool, otherwise
            // allocate
            // inline
            auto data_pool = GetPlanPool();
            if (elem.GetType() != type::Type::VARCHAR &&
                elem.GetType() != type::Type::VARBINARY)
              data_pool = nullptr;

            LOG_TRACE(
                "Column %d found in INSERT query, ExpressionType: %s", col_cntr,
                ExpressionTypeToString(values->at(pos)->GetExpressionType())
                    .c_str());

            if (values->at(pos)->GetExpressionType() ==
                ExpressionType::VALUE_PARAMETER) {
              std::tuple<oid_t, oid_t, oid_t> pair =
                  std::make_tuple(tuple_idx, col_cntr, param_index);
              parameter_vector_->push_back(pair);
              params_value_type_->push_back(
                  table_schema->GetColumn(col_cntr).GetType());
              ++param_index;
            } else {
              expression::ConstantValueExpression *const_expr_elem =
                  dynamic_cast<expression::ConstantValueExpression *>(
                      values->at(pos));
              type::Value val = (const_expr_elem->GetValue());
              tuple->SetValue(col_cntr, val, data_pool);
            }
          }

          ++col_cntr;
        }
        LOG_TRACE("Tuple to be inserted: %s", tuple->GetInfo().c_str());
        tuples_.push_back(std::move(tuple));
      }
    }
  } else {
    LOG_TRACE("Table does not exist!");
  }
}

type::AbstractPool *InsertPlan::GetPlanPool() {
  // construct pool if needed
  if (pool_.get() == nullptr)
    pool_.reset(new type::EphemeralPool());
  // return pool
  return pool_.get();
}

void InsertPlan::SetParameterValues(std::vector<type::Value> *values) {
  PL_ASSERT(values->size() == parameter_vector_->size());
  LOG_TRACE("Set Parameter Values in Insert");
  for (unsigned int i = 0; i < values->size(); ++i) {
    auto param_type = params_value_type_->at(i);
    auto &put_loc = parameter_vector_->at(i);
    auto value = values->at(std::get<2>(put_loc));
    // LOG_TRACE("Setting value of type %s",
    // ValueTypeToString(param_type).c_str());

    switch (param_type) {
      case type::Type::VARBINARY:
      case type::Type::VARCHAR: {
        type::Value val = (value.CastAs(param_type));
        tuples_[std::get<0>(put_loc)]
            ->SetValue(std::get<1>(put_loc), val, GetPlanPool());
        break;
      }
      default: {
        type::Value val = (value.CastAs(param_type));
        tuples_[std::get<0>(put_loc)]
            ->SetValue(std::get<1>(put_loc), val, nullptr);
      }
    }
  }
}
}
}
