//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// child_property_generator.cpp
//
// Identification: src/include/optimizer/child_property_generator.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/child_property_generator.h"
#include "optimizer/column_manager.h"
#include "optimizer/properties.h"
#include "expression/expression_util.h"
#include "expression/star_expression.h"

using std::move;
using std::vector;
using std::make_pair;
using std::vector;
using std::shared_ptr;
using std::pair;
using std::make_shared;

namespace peloton {
namespace optimizer {

vector<pair<PropertySet, vector<PropertySet>>>
ChildPropertyGenerator::GetProperties(shared_ptr<GroupExpression> gexpr,
                                      PropertySet requirements) {
  requirements_ = requirements;
  output_.clear();

  gexpr->Op().Accept(this);

  if (output_.empty()) {
    output_.push_back(make_pair(requirements_, vector<PropertySet>()));
  }

  return move(output_);
}

void ChildPropertyGenerator::Visit(const PhysicalLimit *) {
  // Let child fulfil all the required properties
  vector<PropertySet> child_input_properties{requirements_};
  output_.push_back(make_pair(requirements_, move(child_input_properties)));
}

void ChildPropertyGenerator::Visit(const PhysicalScan *) {
  PropertySet provided_property;

  // Scan will provide PropertyPredicate
  auto predicate_prop =
      requirements_.GetPropertyOfType(PropertyType::PREDICATE);
  if (predicate_prop != nullptr) provided_property.AddProperty(predicate_prop);

  // AbstractExpression -> offset when insert
  ExprMap columns;
  vector<shared_ptr<expression::AbstractExpression>> column_exprs;
  auto columns_prop = requirements_.GetPropertyOfType(PropertyType::COLUMNS)
                          ->As<PropertyColumns>();
  if (columns_prop->HasStarExpression()) {
    column_exprs.emplace_back(new expression::StarExpression());
  } else {
    // Add all the columns in PropertyColumn
    // Note: columns from PropertyColumn has to be inserted before PropertySort
    // to ensure we don't change the original column order in PropertyColumn
    for (size_t i = 0; i < columns_prop->GetSize(); i++) {
      auto expr = columns_prop->GetColumn(i);
      expression::ExpressionUtil::GetTupleValueExprs(columns, expr.get());
    }

    // Add all the columns from PropertySort to column_set
    auto sort_prop =
        requirements_.GetPropertyOfType(PropertyType::SORT)->As<PropertySort>();
    if (sort_prop != nullptr) {
      for (size_t i = 0; i < sort_prop->GetSortColumnSize(); i++) {
        auto expr = sort_prop->GetSortColumn(i);
        expression::ExpressionUtil::GetTupleValueExprs(columns, expr.get());
      }
    }

    // Generate the provided PropertyColumn
    column_exprs.resize(columns.size());
    for (auto iter : columns) column_exprs[iter.second] = iter.first;
  }
  provided_property.AddProperty(
      shared_ptr<Property>(new PropertyColumns(move(column_exprs))));
  output_.push_back(make_pair(move(provided_property), vector<PropertySet>()));
};

/**
 * Note:
 * Fulfill the entire projection property in the aggregation. Should
 * enumerate different combination of the aggregation functions and other
 * projection.
 */
void ChildPropertyGenerator::Visit(const PhysicalHashGroupBy *op) {
  PropertySet child_input_property_set;
  PropertySet provided_property;

  ExprSet child_col;
  ExprSet provided_col;
  for (auto prop : requirements_.Properties()) {
    switch (prop->Type()) {
      // Generate output columns for the child
      // Aggregation will break sort property
      case PropertyType::DISTINCT: {
        // DISTINCT columns is in select list
        // don't need to add
        break;
      }
      case PropertyType::PROJECT:
      case PropertyType::SORT: {
        // SORT property will be fullfilled after Hash GroupBy
        // We need to make sure that sort columns are in
        // in the output of the physical scan.
        auto sort_prop = prop->As<PropertySort>();
        size_t sort_col_len = sort_prop->GetSortColumnSize();

        for (size_t col_idx = 0; col_idx < sort_col_len; ++col_idx) {
          auto expr = sort_prop->GetSortColumn(col_idx);
          // sort column will be provided
          // by group by
          child_col.insert(expr);
          provided_col.insert(expr);
        }
        break;
      }
      case PropertyType::COLUMNS: {
        // Check group by columns and union it with the
        // PropertyColumn to generate child property
        auto col_prop = prop->As<PropertyColumns>();
        size_t col_len = col_prop->GetSize();

        for (size_t col_idx = 0; col_idx < col_len; col_idx++) {
          auto expr = col_prop->GetColumn(col_idx);
          expression::ExpressionUtil::GetTupleValueExprs(child_col, expr.get());
          provided_col.insert(expr);
        }
        // Add group by columns
        for (auto group_by_col : op->columns)
          child_col.emplace(group_by_col->Copy());

        break;
      }
      case PropertyType::PREDICATE:
        // PropertyPredicate will be fulfilled by the child operator
        child_input_property_set.AddProperty(prop);
        provided_property.AddProperty(prop);
        break;
    }
  }

  // Group by columns are distint
  // Add distinct to output properties.
  // TODO add test for this feature
  ExprSet group_by_cols;

  for (auto group_by_col : op->columns)
    group_by_cols.emplace(group_by_col->Copy());
  // Add child PropertyDistinct
  provided_property.AddProperty(make_shared<PropertyDistinct>(
      vector<shared_ptr<expression::AbstractExpression>>(group_by_cols.begin(),
                                                         group_by_cols.end())));

  // Add child PropertyColumn
  child_input_property_set.AddProperty(make_shared<PropertyColumns>(
      vector<shared_ptr<expression::AbstractExpression>>(child_col.begin(),
                                                         child_col.end())));

  provided_property.AddProperty(make_shared<PropertyColumns>(
      vector<shared_ptr<expression::AbstractExpression>>(provided_col.begin(),
                                                         provided_col.end())));

  vector<PropertySet> child_input_properties{child_input_property_set};
  output_.push_back(make_pair(provided_property, move(child_input_properties)));
}

void ChildPropertyGenerator::Visit(const PhysicalSortGroupBy *op) {
  PropertySet child_input_property_set;
  PropertySet provided_property;
  std::vector<bool> sort_ascending;
  auto group_by_col_len = op->columns.size();

  for (auto prop : requirements_.Properties()) {
    switch (prop->Type()) {
      // Generate output columns for the child
      // Aggregation will break sort property
      case PropertyType::DISTINCT: {
        // DISTINCT columns is in select list
        // don't need to add
        break;
      }
      case PropertyType::PROJECT:
        break;
      case PropertyType::SORT: {
        bool sort_fulfilled = true;
        auto sort_prop = prop->As<PropertySort>();
        auto sort_col_len = sort_prop->GetSortColumnSize();
        // if (sort_col_len > group_by_col_len) break;
        // for (size_t col_idx = 0; col_idx < group)
        for (size_t col_idx = 0; col_idx < sort_col_len; col_idx++) {
          if (!sort_prop->GetSortColumn(col_idx)
                   ->Equals(op->columns[col_idx].get())) {
            sort_fulfilled = false;
            break;
          }
        }
        if (sort_fulfilled) {
          provided_property.AddProperty(prop);
          for (size_t i = 0; i < sort_col_len; i++)
            sort_ascending.push_back(sort_prop->GetSortAscending(i));
        }
        break;
      }
      case PropertyType::COLUMNS: {
        provided_property.AddProperty(prop);

        // Check group by columns and union it with the
        // PropertyColumn to generate child property
        auto col_prop = prop->As<PropertyColumns>();
        size_t col_len = col_prop->GetSize();
        ExprSet child_col;
        for (size_t col_idx = 0; col_idx < col_len; col_idx++) {
          auto expr = col_prop->GetColumn(col_idx);
          expression::ExpressionUtil::GetTupleValueExprs(child_col, expr.get());
        }
        // Add group by columns
        for (auto group_by_col : op->columns)
          child_col.emplace(group_by_col->Copy());

        // Add child PropertyColumn
        child_input_property_set.AddProperty(make_shared<PropertyColumns>(
            vector<shared_ptr<expression::AbstractExpression>>(
                child_col.begin(), child_col.end())));
        break;
      }
      case PropertyType::PREDICATE:
        // PropertyPredicate will be fulfilled by the child operator
        child_input_property_set.AddProperty(prop);
        provided_property.AddProperty(prop);
        break;
    }
  }

  // Group by columns are distint
  // Add distinct to output properties.
  // TODO add test for this feature
  ExprSet group_by_cols;

  for (auto group_by_col : op->columns)
    group_by_cols.emplace(group_by_col->Copy());
  // Add child PropertyDistinct
  provided_property.AddProperty(make_shared<PropertyDistinct>(
      vector<shared_ptr<expression::AbstractExpression>>(group_by_cols.begin(),
                                                         group_by_cols.end())));

  // Start from the idx of the next elements in sort_ascending
  // because it can be filled when the sort property is fulfilled
  for (size_t i = sort_ascending.size(); i < group_by_col_len; i++)
    sort_ascending.push_back(true);
  child_input_property_set.AddProperty(
      make_shared<PropertySort>(op->columns, sort_ascending));
  vector<PropertySet> child_input_properties{child_input_property_set};
  output_.push_back(make_pair(provided_property, move(child_input_properties)));
}

void ChildPropertyGenerator::Visit(const PhysicalDistinct *) {
  PropertySet child_input_property_set;
  PropertySet provided_property = requirements_;
  for (auto prop : requirements_.Properties()) {
    // Sort must be performed after Distinct
    if (prop->Type() != PropertyType::DISTINCT &&
        prop->Type() != PropertyType::SORT) {
      child_input_property_set.AddProperty(prop);
    }
  }

  vector<PropertySet> child_input_properties{child_input_property_set};
  output_.push_back(make_pair(provided_property, move(child_input_properties)));
}

void ChildPropertyGenerator::Visit(const PhysicalAggregate *) {
  PropertySet child_input_property_set;
  PropertySet provided_property;

  ExprSet child_col;
  ExprSet provided_col;
  for (auto prop : requirements_.Properties()) {
    switch (prop->Type()) {
      // Generate output columns for the child
      // Aggregation will break sort property
      case PropertyType::DISTINCT:
        break;
      case PropertyType::PROJECT:
      case PropertyType::SORT: {
        // SORT property will be fullfilled after Aggregation
        // We need to make sure that sort columns are in
        // the output of the physical scan.
        auto sort_prop = prop->As<PropertySort>();
        size_t sort_col_len = sort_prop->GetSortColumnSize();

        for (size_t col_idx = 0; col_idx < sort_col_len; ++col_idx) {
          auto expr = sort_prop->GetSortColumn(col_idx);
          // sort column will be pass to upper
          child_col.insert(expr);
          provided_col.insert(expr);
        }
        break;
      }
      case PropertyType::COLUMNS: {
        provided_property.AddProperty(prop);

        // Check group by columns and union it with the
        // PropertyColumn to generate child property
        auto col_prop = prop->As<PropertyColumns>();
        size_t col_len = col_prop->GetSize();
        for (size_t col_idx = 0; col_idx < col_len; col_idx++) {
          auto expr = col_prop->GetColumn(col_idx);
          expression::ExpressionUtil::GetTupleValueExprs(child_col, expr.get());
          provided_col.insert(expr);
        }

        break;
      }
      case PropertyType::PREDICATE:
        // PropertyPredicate will be fulfilled by the child operator
        child_input_property_set.AddProperty(prop);
        provided_property.AddProperty(prop);
        break;
    }
  }

  // Add child PropertyColumn
  child_input_property_set.AddProperty(make_shared<PropertyColumns>(
      vector<shared_ptr<expression::AbstractExpression>>(child_col.begin(),
                                                         child_col.end())));

  provided_property.AddProperty(make_shared<PropertyColumns>(
      vector<shared_ptr<expression::AbstractExpression>>(provided_col.begin(),
                                                         provided_col.end())));

  vector<PropertySet> child_input_properties{child_input_property_set};
  output_.push_back(make_pair(provided_property, move(child_input_properties)));
}

void ChildPropertyGenerator::Visit(const PhysicalProject *) {
  PropertySet child_input_property_set;
  PropertySet provided_property;

  // Child scan will provide PropertyPredicate
  auto predicate_prop =
      requirements_.GetPropertyOfType(PropertyType::PREDICATE);
  if (predicate_prop != nullptr) {
    child_input_property_set.AddProperty(predicate_prop);
    provided_property.AddProperty(predicate_prop);
  }

  // AbstractExpression -> offset when insert
  ExprMap columns;
  vector<shared_ptr<expression::AbstractExpression>> column_exprs;
  auto columns_prop_shared_p =
      requirements_.GetPropertyOfType(PropertyType::COLUMNS);

  provided_property.AddProperty(columns_prop_shared_p);

  auto columns_prop = columns_prop_shared_p->As<PropertyColumns>();

  // Projection will provide property column

  if (columns_prop->HasStarExpression()) {
    column_exprs.emplace_back(new expression::StarExpression());
  } else {
    // Add all the columns in PropertyColumn
    // Note: columns from PropertyColumn has to be inserted before PropertySort
    // to ensure we don't change the origin column order in PropertyColumn
    for (size_t i = 0; i < columns_prop->GetSize(); i++) {
      auto expr = columns_prop->GetColumn(i);
      expression::ExpressionUtil::GetTupleValueExprs(columns, expr.get());
    }

    // Add all the columns from PropertySort to column_set
    auto sort_prop =
        requirements_.GetPropertyOfType(PropertyType::SORT)->As<PropertySort>();
    if (sort_prop != nullptr) {
      for (size_t i = 0; i < sort_prop->GetSortColumnSize(); i++) {
        auto expr = sort_prop->GetSortColumn(i);
        expression::ExpressionUtil::GetTupleValueExprs(columns, expr.get());
      }
    }

    // Generate the provided PropertyColumn
    column_exprs.resize(columns.size());
    for (auto iter : columns) column_exprs[iter.second] = iter.first;
  }
  // child must have all the columns need by projection and sort
  child_input_property_set.AddProperty(
      shared_ptr<Property>(new PropertyColumns(move(column_exprs))));

  vector<PropertySet> child_input_properties{child_input_property_set};
  output_.push_back(
      make_pair(move(provided_property), move(child_input_properties)));
};

void ChildPropertyGenerator::Visit(const PhysicalOrderBy *) {
  PropertySet child_input_property_set;
  PropertySet provided_property;

  ExprSet child_col;
  for (auto prop : requirements_.Properties()) {
    switch (prop->Type()) {
      // Distinct will be fulfilled by child property
      case PropertyType::DISTINCT: {
        provided_property.AddProperty(prop);
        child_input_property_set.AddProperty(prop);
        break;
      }
      case PropertyType::PROJECT:
      case PropertyType::SORT: {
        auto sort_prop = prop->As<PropertySort>();
        size_t col_len = sort_prop->GetSortColumnSize();
        for (size_t col_idx = 0; col_idx < col_len; ++col_idx) {
          auto expr = sort_prop->GetSortColumn(col_idx);
          child_col.insert(expr);
        }
        break;
      }
      case PropertyType::COLUMNS: {
        provided_property.AddProperty(prop);

        // Check group by columns and union it with the
        // PropertyColumn to generate child property
        auto col_prop = prop->As<PropertyColumns>();
        size_t col_len = col_prop->GetSize();
        for (size_t col_idx = 0; col_idx < col_len; col_idx++) {
          auto expr = col_prop->GetColumn(col_idx);
          expression::ExpressionUtil::GetTupleValueExprs(child_col, expr.get());
        }
        break;
      }
      case PropertyType::PREDICATE:
        // PropertyPredicate will be fulfilled by the child operator
        child_input_property_set.AddProperty(prop);
        provided_property.AddProperty(prop);
        break;
    }
  }

  // Add child PropertyColumn
  child_input_property_set.AddProperty(make_shared<PropertyColumns>(
      std::vector<std::shared_ptr<expression::AbstractExpression>>(
          child_col.begin(), child_col.end())));

  vector<PropertySet> child_input_properties{child_input_property_set};
  output_.push_back(
      make_pair(move(provided_property), move(child_input_properties)));
}

void ChildPropertyGenerator::Visit(const PhysicalFilter *){};
void ChildPropertyGenerator::Visit(const PhysicalInnerNLJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalLeftNLJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalRightNLJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalOuterNLJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalInnerHashJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalLeftHashJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalRightHashJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalOuterHashJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalInsert *){};
void ChildPropertyGenerator::Visit(const PhysicalUpdate *) {
  // Let child fulfil all the required properties
  vector<PropertySet> child_input_properties{requirements_};

  output_.push_back(make_pair(requirements_, move(child_input_properties)));
}
void ChildPropertyGenerator::Visit(const PhysicalDelete *) {
  // Let child fulfil all the required properties
  vector<PropertySet> child_input_properties{requirements_};

  output_.push_back(make_pair(requirements_, move(child_input_properties)));
};

} /* namespace optimizer */
} /* namespace peloton */
