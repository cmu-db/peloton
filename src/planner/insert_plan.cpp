
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
#include "expression/constant_value_expression.h"
#include "storage/data_table.h"
#include "type/ephemeral_pool.h"

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
    storage::DataTable *table, std::vector<std::unique_ptr<char[]>> *columns,
    std::vector<std::unique_ptr<std::vector<std::unique_ptr<peloton::expression::AbstractExpression>>>> *
        insert_values)
    : bulk_insert_count(insert_values->size()) {

  parameter_vector_.reset(new std::vector<std::tuple<oid_t, oid_t, oid_t>>());
  params_value_type_.reset(new std::vector<type::TypeId>);

  target_table_ = table;

  if (target_table_) {
    const catalog::Schema *table_schema = target_table_->GetSchema();
    // INSERT INTO table_name VALUES (val2, val2, ...)
    if (columns == NULL) {
      for (uint32_t tuple_idx = 0; tuple_idx < insert_values->size();
           tuple_idx++) {
        auto values = (*insert_values)[tuple_idx].get();
        PL_ASSERT(values->size() <= table_schema->GetColumnCount());
        std::unique_ptr<storage::Tuple> tuple(
            new storage::Tuple(table_schema, true));
        int col_cntr = 0;
        int param_index = 0;
        for (expression::AbstractExpression *elem : *values) {
          // Default value to be inserted
          if (elem == nullptr) {
            // No default value
            if (table_schema->GetDefaultValue(col_cntr) == nullptr) {
              // exception handling ?
              throw ConstraintException("No DEFAULT constraint !");
            }
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
                dynamic_cast<expression::ConstantValueExpression *>(elem.get());
            type::Value const_expr_elem_val = (const_expr_elem->GetValue());
            switch (const_expr_elem->GetValueType()) {
              case type::TypeId::VARCHAR:
              case type::TypeId::VARBINARY:
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
        auto values = (*insert_values)[tuple_idx].get();
        PL_ASSERT(columns->size() <= table_schema->GetColumnCount());
        std::unique_ptr<storage::Tuple> tuple(
            new storage::Tuple(table_schema, true));
        int param_index = 0;
        auto &table_columns = table_schema->GetColumns();
        auto query_columns = columns;
        auto query_columns_cnt = query_columns->size();

        // Update parameter info in the specified columns order
        for (size_t pos = 0; pos < query_columns_cnt; pos++) {
          auto col_name = query_columns->at(pos).get();
          auto col_cntr = table_schema->GetColumnID(col_name);

          PL_ASSERT(col_cntr != INVALID_OID);

          // If it's varchar or varbinary then use data pool, otherwise
          // allocate
          // inline
          auto col_type = table_schema->GetColumn(col_cntr).GetType();
          type::AbstractPool * data_pool = nullptr;
          if (col_type == type::TypeId::VARCHAR ||
              col_type == type::TypeId::VARBINARY)
            data_pool = GetPlanPool();

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
                    values->at(pos).get());
            type::Value val = (const_expr_elem->GetValue());
            tuple->SetValue(col_cntr, val, data_pool);
          }
        }
        // Insert a null value for non-specified columns
        auto table_columns_cnt = table_schema->GetColumnCount();
        if (query_columns_cnt < table_columns_cnt) {
          for (size_t col_cntr = 0; col_cntr < table_columns_cnt; col_cntr++) {
            auto col = table_columns[col_cntr];
            if (std::find_if(query_columns->begin(), query_columns->end(), [&col](std::unique_ptr<char[]>& x){return col.GetName() == x.get();})
                == query_columns->end()) {
              tuple->SetValue(col_cntr, type::ValueFactory::GetNullValueByType(
                  col.GetType()),
                              nullptr);
            }
          }
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
      case type::TypeId::VARBINARY:
      case type::TypeId::VARCHAR: {
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
