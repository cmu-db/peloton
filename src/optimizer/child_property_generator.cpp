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
#include "optimizer/memo.h"

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
                                      PropertySet requirements, Memo *memo) {
  requirements_ = requirements;

  for (auto child_group_id : gexpr->GetChildGroupIDs())
    child_groups_.push_back(memo->GetGroupByID(child_group_id));

  output_.clear();

  gexpr->Op().Accept(this);

  if (output_.empty()) {
    output_.push_back(make_pair(requirements_, vector<PropertySet>()));
  }

  return move(output_);
}

void ChildPropertyGenerator::Visit(const PhysicalSeqScan *) { ScanHelper(); };

void ChildPropertyGenerator::Visit(const PhysicalIndexScan *) { ScanHelper(); };

/**
 * Note:
 * Fulfill the entire projection property in the aggregation. Should
 * enumerate different combination of the aggregation functions and other
 * projection.
 */
void ChildPropertyGenerator::Visit(const PhysicalHashGroupBy *op) {
  AggregateHelper(op);
}

void ChildPropertyGenerator::Visit(const PhysicalSortGroupBy *op) {
  AggregateHelper(op);
}

void ChildPropertyGenerator::Visit(const PhysicalAggregate *op) {
  AggregateHelper(op);
}

void ChildPropertyGenerator::Visit(const PhysicalLimit *) {}
void ChildPropertyGenerator::Visit(const PhysicalDistinct *) {}
void ChildPropertyGenerator::Visit(const PhysicalProject *){};
void ChildPropertyGenerator::Visit(const PhysicalOrderBy *) {}
void ChildPropertyGenerator::Visit(const PhysicalFilter *){};
void ChildPropertyGenerator::Visit(const PhysicalInnerNLJoin *op){
  JoinHelper(op);
};
void ChildPropertyGenerator::Visit(const PhysicalLeftNLJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalRightNLJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalOuterNLJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalInnerHashJoin *op) {
  JoinHelper(op);
};

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

void ChildPropertyGenerator::Visit(const DummyScan *) {
  // Provide nothing
  output_.push_back(make_pair(PropertySet(), vector<PropertySet>()));
}

/************************ Private helper Functions ****************************/

void ChildPropertyGenerator::ScanHelper() {
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
}

void ChildPropertyGenerator::AggregateHelper(const BaseOperatorNode *op) {
  // Get group by columns
  vector<shared_ptr<expression::AbstractExpression>> groupby_cols;
  if (op->type() == OpType::HashGroupBy) {
    groupby_cols = ((const PhysicalHashGroupBy *)op)->columns;
  } else if (op->type() == OpType::SortGroupBy) {
    groupby_cols = ((const PhysicalSortGroupBy *)op)->columns;
  } else if (op->type() == OpType::Aggregate) {
    // Plain Aggregation does not have group by columns
    groupby_cols = vector<shared_ptr<expression::AbstractExpression>>();
  }

  PropertySet child_input_property_set;
  PropertySet provided_property;

  ExprSet child_col;
  ExprSet provided_col;
  bool sort_added = false;
  for (auto prop : requirements_.Properties()) {
    switch (prop->Type()) {
      case PropertyType::DISTINCT: {
        // Optimization for GroupBy. GroupBy may provide PropertyDistinct
        if (op->type() == OpType::HashGroupBy ||
            op->type() == OpType::SortGroupBy) {
          // If DISTINCT columns are a superset of GROUP BY columns.
          // PropertyDistinct can be satisfied
          ExprSet distinct_cols;
          auto distinct_prop = prop->As<PropertyDistinct>();
          size_t distinct_col_size = distinct_prop->GetSize();
          for (size_t i = 0; i < distinct_col_size; i++)
            distinct_cols.insert(distinct_prop->GetDistinctColumn(i));

          bool satisfied = true;
          for (auto &group_by_col : groupby_cols) {
            if (distinct_cols.count(group_by_col) == 0) {
              satisfied = false;
              break;
            }
          }
          if (satisfied) provided_property.AddProperty(prop);
        }
        break;
      }
      case PropertyType::LIMIT: {
        // LIMIT only provided by enforcer
        break;
      }
      case PropertyType::SORT: {
        // Add the ORDER BY columns to the output columns
        // Also add base columns to the child output columns
        auto sort_prop = prop->As<PropertySort>();
        size_t sort_col_len = sort_prop->GetSortColumnSize();

        for (size_t col_idx = 0; col_idx < sort_col_len; ++col_idx) {
          auto expr = sort_prop->GetSortColumn(col_idx);
          // sort column will be provided by group by
          expression::ExpressionUtil::GetTupleValueExprs(child_col, expr.get());
          provided_col.insert(expr);
        }

        // OPTIMIZATION for Sort Group By:
        // TODO: Considering ORDER BY duplicate columns and aggregation
        if (op->type() == OpType::SortGroupBy) {
          ExprSet group_by_cols;
          for (auto &group_by_col : groupby_cols)
            group_by_cols.insert(group_by_col);

          auto sort_prop = prop->As<PropertySort>();
          auto sort_col_len = sort_prop->GetSortColumnSize();
          vector<shared_ptr<expression::AbstractExpression>> sort_columns;
          vector<bool> sort_ascending;
          size_t col_idx = 0;
          for (; col_idx < sort_col_len; col_idx++) {
            auto sort_expr = sort_prop->GetSortColumn(col_idx);
            sort_columns.push_back(sort_expr);
            sort_ascending.push_back(sort_prop->GetSortAscending(col_idx));
            if (group_by_cols.count(sort_expr) > 0) {
              group_by_cols.erase(sort_expr);
            } else {
              // Find a non group by column. Break
              break;
            }
          }

          if (group_by_cols.size() == 0) {
            // Case 1: All the group by columns are in PropertySort and
            // they are at the begining.
            child_input_property_set.AddProperty(prop);
            provided_property.AddProperty(prop);
            sort_added = true;
          } else if (group_by_cols.size() > 0 && col_idx == sort_col_len) {
            // Case 2: Group By columns are a superset of Order By columns.
            // Insert the missing columns and push down to the child.
            for (auto &group_by_col : group_by_cols) {
              sort_columns.push_back(group_by_col);
              sort_ascending.push_back(true);
            }
            child_input_property_set.AddProperty(
                make_shared<PropertySort>(sort_columns, sort_ascending));
            provided_property.AddProperty(prop);
            sort_added = true;
          }
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
          // Expressions like sum(a) + max(b) + c needs another projection
          // on top of aggregation. In this case, aggregation only needs to
          // provide sum(a), max(b) and c
          vector<shared_ptr<expression::AggregateExpression>> aggr_exprs;
          vector<shared_ptr<expression::TupleValueExpression>> tv_exprs;
          expression::ExpressionUtil::GetAggregateExprs(aggr_exprs, tv_exprs,
                                                        expr.get());
          if (!expression::ExpressionUtil::IsAggregateExpression(expr.get()) &&
              aggr_exprs.size() > 0) {
            for (auto &agg_expr : aggr_exprs) provided_col.insert(agg_expr);
            for (auto &tv_expr : tv_exprs) provided_col.insert(tv_expr);
          } else {
            provided_col.insert(expr);
          }
        }

        // Add group by columns
        for (auto &group_by_col : groupby_cols) child_col.insert(group_by_col);

        break;
      }
      case PropertyType::PREDICATE: {
        // PropertyPredicate will be fulfilled by the child operator
        child_input_property_set.AddProperty(prop);
        provided_property.AddProperty(prop);
        break;
      }
    }
  }
  // Add child PropertySort for SortGroupBy if not already do so
  if (op->type() == OpType::SortGroupBy && !sort_added) {
    child_input_property_set.AddProperty(make_shared<PropertySort>(
        groupby_cols, vector<bool>(groupby_cols.size(), true)));
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

void ChildPropertyGenerator::JoinHelper(const BaseOperatorNode *op) {
  vector<PropertySet> child_input_propertys;
  PropertySet provided_property;

  expression::AbstractExpression *join_cond = nullptr;
  if (op->type() == OpType::InnerHashJoin)
    join_cond = ((PhysicalInnerHashJoin *)op)->join_predicate.get();
  else if (op->type() == OpType::InnerNLJoin)
    join_cond = ((PhysicalInnerNLJoin *)op)->join_predicate.get();

  ExprSet child_cols;
  ExprSet provided_cols;

  bool has_star = false;
  vector<shared_ptr<expression::AbstractExpression>> left_cols;
  vector<shared_ptr<expression::AbstractExpression>> right_cols;

  for (auto prop : requirements_.Properties()) {
    switch (prop->Type()) {
      case PropertyType::DISTINCT: {
        // Distinct will be enforced after join
        break;
      }
      case PropertyType::SORT: {
        // TODO: later when we add sort merge join. This need to be changed
        // For now, hash join cannot provide sort. Will enforce after join
        auto sort_prop = prop->As<PropertySort>();
        size_t sort_col_len = sort_prop->GetSortColumnSize();

        // Join needs to generate expressions needed by sort
        for (size_t col_idx = 0; col_idx < sort_col_len; ++col_idx) {
          auto expr = sort_prop->GetSortColumn(col_idx);
          expression::ExpressionUtil::GetTupleValueExprs(child_cols,
                                                         expr.get());
          provided_cols.insert(child_cols.begin(), child_cols.end());
        }
        break;
      }
      case PropertyType::LIMIT: {
        // Limit needs to be enforced after join
        break;
      }
      case PropertyType::PREDICATE: {
        // TODO: later we need to modify this to support predicate push down.
        // For now, the predicate will be evaluate in join executor

        auto pred_prop = prop->As<PropertyPredicate>();
        // Columns needed by predicate should be generated by child
        expression::ExpressionUtil::GetTupleValueExprs(
            child_cols, pred_prop->GetPredicate());

        provided_property.AddProperty(prop);
        break;
      }
      case PropertyType::COLUMNS: {
        auto col_prop = prop->As<PropertyColumns>();
        size_t col_len = col_prop->GetSize();
        for (size_t col_idx = 0; col_idx < col_len; col_idx++) {
          auto expr = col_prop->GetColumn(col_idx);
          if (expr->GetExpressionType() == ExpressionType::STAR) {
            left_cols.push_back(expr);
            right_cols.push_back(expr);
            has_star = true;
            provided_cols.insert(expr);
          }
          // All base columns needs to be generated by the child
          else {
            expression::ExpressionUtil::GetTupleValueExprs(child_cols,
                                                           expr.get());
            provided_cols.insert(child_cols.begin(), child_cols.end());
          }
        }

        break;
      }
    }
  }

  if (!has_star) {
    // All base columns in join condition needs to be generated by the child
    expression::ExpressionUtil::GetTupleValueExprs(child_cols, join_cond);

    // Decide which base columns should be provided by which child based on
    // the table alias.
    PL_ASSERT(child_groups_.size() == 2);
    auto &left_table_alias = child_groups_[0]->GetTableAliases();
    UNUSED_ATTRIBUTE auto &right_table_alias = child_groups_[1]->GetTableAliases();

    for (auto child_col : child_cols) {
      auto tv_expr = (expression::TupleValueExpression *) child_col.get();
      if (left_table_alias.count(tv_expr->GetTableName()) > 0) {
        // This column should be provided by the left child
        left_cols.push_back(child_col);
      } else {
        PL_ASSERT(right_table_alias.count(tv_expr->GetTableName()) > 0);
        right_cols.push_back(child_col);
      }
    }
  }

  provided_property.AddProperty(make_shared<PropertyColumns>(
      vector<shared_ptr<expression::AbstractExpression>>(provided_cols.begin(),
                                                         provided_cols.end())));

  auto l_property_set =
      PropertySet({make_shared<PropertyColumns>(std::move(left_cols))});
  auto r_property_set =
      PropertySet({make_shared<PropertyColumns>(std::move(right_cols))});
  child_input_propertys.push_back(l_property_set);
  child_input_propertys.emplace_back(r_property_set);

  output_.push_back(make_pair(provided_property, child_input_propertys));
}
} /* namespace optimizer */
} /* namespace peloton */
