//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// insert_plan.cpp
//
// Identification: src/planner/insert_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
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

InsertPlan::InsertPlan(storage::DataTable *table,
    const std::vector<std::string> *columns,
    const std::vector<std::vector<
        std::unique_ptr<expression::AbstractExpression>>> *insert_values)
    : target_table_(table), bulk_insert_count_(insert_values->size()) {
  LOG_TRACE("Creating an Insert Plan with multiple expressions");
  PL_ASSERT(target_table_);
  parameter_vector_.reset(new std::vector<std::tuple<oid_t, oid_t, oid_t>>());
  params_value_type_.reset(new std::vector<type::TypeId>);

  auto *schema = target_table_->GetSchema();
  // INSERT INTO table_name VALUES (val1, val2, ...)
  if (columns->empty()) {
    for (uint32_t tuple_idx = 0; tuple_idx < insert_values->size();
         tuple_idx++) {
      auto &values = (*insert_values)[tuple_idx];
      PL_ASSERT(values.size() <= schema->GetColumnCount());
      uint32_t param_idx = 0;
      for (uint32_t column_id = 0; column_id < values.size(); column_id++) {
        auto &exp = values[column_id];
        const type::TypeId type = schema->GetType(column_id);
        if (exp == nullptr) {
          type::Value *v = schema->GetDefaultValue(column_id);
          if (v == nullptr)
            values_.push_back(type::ValueFactory::GetNullValueByType(type));
          else
            values_.push_back(*v);
        } else if (exp->GetExpressionType() ==
                   ExpressionType::VALUE_PARAMETER) {
          std::tuple<oid_t, oid_t, oid_t> pair =
              std::make_tuple(tuple_idx, column_id, param_idx++);
          parameter_vector_->push_back(pair);
          params_value_type_->push_back(type);
        } else {
          PL_ASSERT(exp->GetExpressionType() == ExpressionType::VALUE_CONSTANT);
          auto *const_exp =
              dynamic_cast<expression::ConstantValueExpression *>(exp.get());
          type::Value value = const_exp->GetValue().CastAs(type);
          values_.push_back(value);
        }
      }
    }
  }
  // INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...);
  else {
    PL_ASSERT(columns->size() <= schema->GetColumnCount());
    for (uint32_t tuple_idx = 0; tuple_idx < insert_values->size();
         tuple_idx++) {
      auto &values = (*insert_values)[tuple_idx];

      auto &table_columns = schema->GetColumns();
      auto table_columns_num = schema->GetColumnCount();
      uint32_t param_idx = 0;
      for (size_t column_id = 0; column_id < table_columns_num; column_id++) {
        auto col = table_columns[column_id];
        const type::TypeId type = schema->GetType(column_id);
        auto found = std::find_if(columns->begin(), columns->end(),
            [&col](const std::string &x) { return col.GetName() == x; });
        if (found == columns->end()) {
          type::Value *v = schema->GetDefaultValue(column_id);
          if (v == nullptr)
            values_.push_back(type::ValueFactory::GetNullValueByType(type));
          else
            values_.push_back(*v);
          continue;
        }
        auto idx = std::distance(columns->begin(), found);
        auto &exp = values[idx];
        if (exp->GetExpressionType() == ExpressionType::VALUE_PARAMETER) {
          std::tuple<oid_t, oid_t, oid_t> pair =
              std::make_tuple(tuple_idx, column_id, param_idx++);
          parameter_vector_->push_back(pair);
          params_value_type_->push_back(type);
        } else {
          PL_ASSERT(exp->GetExpressionType() == ExpressionType::VALUE_CONSTANT);
          auto *const_exp =
              dynamic_cast<expression::ConstantValueExpression *>(exp.get());
          type::Value value = const_exp->GetValue().CastAs(type);
          values_.push_back(value);
        }
      }
    }
  }
}

type::AbstractPool *InsertPlan::GetPlanPool() {
  if (pool_.get() == nullptr)
    pool_.reset(new type::EphemeralPool());
  return pool_.get();
}

void InsertPlan::SetParameterValues(std::vector<type::Value> *values) {
  LOG_TRACE("Set Parameter Values in Insert");
  PL_ASSERT(values->size() == parameter_vector_->size());
  PL_ASSERT(values->size() == params_value_type_->size());
  for (unsigned int i = 0; i < values->size(); i++) {
    auto type = params_value_type_->at(i);
    auto &param_info = parameter_vector_->at(i);
    auto tuple_idx = std::get<0>(param_info);
    auto column_id = std::get<1>(param_info);
    auto param_idx = std::get<2>(param_info);
    type::Value value = values->at(param_idx).CastAs(type);
    auto it = values_.begin();
    values_.insert(it + (tuple_idx + 1) * column_id, value);
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
  if (GetPlanNodeType() != rhs.GetPlanNodeType())
    return false;

  auto &other = static_cast<const planner::InsertPlan &>(rhs);

  auto *table = GetTable();
  auto *other_table = other.GetTable();
  PL_ASSERT(table && other_table);
  if (*table != *other_table)
    return false;

  if (GetChildren().size() == 0) {
    if (other.GetChildren().size() != 0)
      return false;

    if (GetBulkInsertCount() != other.GetBulkInsertCount())
      return false;
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
      map.Insert(expression::Parameter::CreateConstParameter(value.GetTypeId(),
          schema->AllowNull(column_id)), nullptr);
      values.push_back(value);
    }
  } else {
    PL_ASSERT(GetChildren().size() == 1);
    auto *plan = const_cast<planner::AbstractPlan *>(GetChild(0));
    plan->VisitParameters(map, values, values_from_user);
  }
}

}  // namespace planner
}  // namespace peloton
