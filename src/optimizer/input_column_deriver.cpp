//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_to_plan_transformer.cpp
//
// Identification: src/optimizer/operator_to_plan_transformer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "expression/expression_util.h"
#include "optimizer/input_column_deriver.h"
#include "optimizer/memo.h"
#include "optimizer/operator_expression.h"
#include "optimizer/operators.h"
#include "optimizer/properties.h"
#include "storage/data_table.h"

using std::vector;
using std::make_pair;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::shared_ptr;
using std::move;
using std::make_tuple;
using std::make_pair;
using std::pair;

using peloton::expression::AbstractExpression;
namespace peloton {
namespace optimizer {

InputColumnDeriver::InputColumnDeriver() {}

pair<vector<AbstractExpression *>, vector<vector<AbstractExpression *>>>
InputColumnDeriver::DeriveInputColumns(
    GroupExpression *gexpr, shared_ptr<PropertySet> properties,
    vector<AbstractExpression *> required_cols, Memo *memo) {
  properties_ = properties;
  gexpr_ = gexpr;
  required_cols_ = move(required_cols);
  memo_ = memo;
  gexpr->Op().Accept(this);

  return move(output_input_cols_);
}

// Doesn't have output column at all
void InputColumnDeriver::Visit(const DummyScan *) {}

void InputColumnDeriver::Visit(const PhysicalSeqScan *) { ScanHelper(); }

void InputColumnDeriver::Visit(const PhysicalIndexScan *) { ScanHelper(); }

void InputColumnDeriver::Visit(const QueryDerivedScan *op) {
  // QueryDerivedScan should only be a renaming layer
  ExprMap output_cols_map;
  for (auto expr : required_cols_) {
    expression::ExpressionUtil::GetTupleValueExprs(output_cols_map, expr);
  }
  vector<AbstractExpression *> output_cols =
      vector<AbstractExpression *>(output_cols_map.size());
  ExprMap input_cols_map;
  vector<AbstractExpression *> input_cols(output_cols.size(), nullptr);
  for (auto &entry : output_cols_map) {
    auto tv_expr =
        reinterpret_cast<expression::TupleValueExpression *>(entry.first);
    output_cols[entry.second] = tv_expr;
    // Get the actual expression
    auto input_col = const_cast<QueryDerivedScan *>(op)
                         ->alias_to_expr_map[tv_expr->GetColumnName()]
                         .get();
    // QueryDerivedScan only modify the column name to be a tv_expr, does not
    // change the mapping
    input_cols[entry.second] = input_col;
  }
  output_input_cols_ =
      pair<vector<AbstractExpression *>, vector<vector<AbstractExpression *>>>{
          output_cols, {input_cols}};
}

void InputColumnDeriver::Visit(const PhysicalLimit *) { Passdown(); }

void InputColumnDeriver::Visit(const PhysicalOrderBy *) {
  // we need to pass down both required columns and sort columns
  auto prop = properties_->GetPropertyOfType(PropertyType::SORT);
  PL_ASSERT(prop.get() != nullptr);
  ExprSet input_cols_set;
  for (auto expr : required_cols_) {
    if (expression::ExpressionUtil::IsAggregateExpression(expr))
      input_cols_set.insert(expr);
    else
      expression::ExpressionUtil::GetTupleValueExprs(input_cols_set, expr);
  }
  auto sort_prop = prop->As<PropertySort>();
  size_t sort_col_size = sort_prop->GetSortColumnSize();
  for (size_t idx = 0; idx < sort_col_size; ++idx) {
    input_cols_set.insert(sort_prop->GetSortColumn(idx));
  }
  vector<AbstractExpression *> cols;
  for (auto &expr : input_cols_set) {
    cols.push_back(expr);
  }
  output_input_cols_ =
      pair<vector<AbstractExpression *>, vector<vector<AbstractExpression *>>>{
          cols, {cols}};
}

void InputColumnDeriver::Visit(const PhysicalHashGroupBy *op) {
  AggregateHelper(op);
}

void InputColumnDeriver::Visit(const PhysicalSortGroupBy *op) {
  AggregateHelper(op);
}

void InputColumnDeriver::Visit(const PhysicalAggregate *op) {
  AggregateHelper(op);
}

void InputColumnDeriver::Visit(const PhysicalDistinct *) { Passdown(); }

void InputColumnDeriver::Visit(const PhysicalInnerNLJoin *op) {
  JoinHelper(op);
}

void InputColumnDeriver::Visit(const PhysicalLeftNLJoin *) {}

void InputColumnDeriver::Visit(const PhysicalRightNLJoin *) {}

void InputColumnDeriver::Visit(const PhysicalOuterNLJoin *) {}

void InputColumnDeriver::Visit(const PhysicalInnerHashJoin *op) {
  JoinHelper(op);
}

void InputColumnDeriver::Visit(const PhysicalLeftHashJoin *) {}

void InputColumnDeriver::Visit(const PhysicalRightHashJoin *) {}

void InputColumnDeriver::Visit(const PhysicalOuterHashJoin *) {}

void InputColumnDeriver::Visit(const PhysicalInsert *) {
  output_input_cols_ =
      pair<vector<AbstractExpression *>, vector<vector<AbstractExpression *>>>{
          required_cols_, {}};
}

void InputColumnDeriver::Visit(const PhysicalInsertSelect *) { Passdown(); }

void InputColumnDeriver::Visit(const PhysicalDelete *) { Passdown(); }

void InputColumnDeriver::Visit(const PhysicalUpdate *) { Passdown(); }

void InputColumnDeriver::ScanHelper() {
  // Scan does not have input column, output columns should contain all tuple
  // value expressions needed
  ExprMap output_cols_map;
  for (auto expr : required_cols_) {
    expression::ExpressionUtil::GetTupleValueExprs(output_cols_map, expr);
  }
  vector<AbstractExpression *> output_cols =
      vector<AbstractExpression *>(output_cols_map.size());
  for (auto &entry : output_cols_map) {
    output_cols[entry.second] = entry.first;
  }
  output_input_cols_ =
      pair<vector<AbstractExpression *>, vector<vector<AbstractExpression *>>>{
          output_cols, {}};
}

void InputColumnDeriver::AggregateHelper(const BaseOperatorNode *op) {
  ExprSet input_cols_set;
  ExprMap output_cols_map;
  oid_t output_col_idx = 0;
  for (size_t idx = 0; idx < required_cols_.size(); ++idx) {
    auto &expr = required_cols_[idx];
    vector<expression::AggregateExpression *> aggr_exprs;
    vector<expression::TupleValueExpression *> tv_exprs;
    expression::ExpressionUtil::GetAggregateExprs(aggr_exprs, tv_exprs, expr);
    for (auto &aggr_expr : aggr_exprs) {
      if (!output_cols_map.count(aggr_expr)) {
        output_cols_map[aggr_expr] = output_col_idx++;
        size_t child_size = aggr_expr->GetChildrenSize();
        for (size_t idx = 0; idx < child_size; ++idx) {
          expression::ExpressionUtil::GetTupleValueExprs(
              input_cols_set, aggr_expr->GetModifiableChild(idx));
        }
      }
    }
    // TV expr not in aggregation (must be in grouby, so we do not need to add
    // to add to input columns
    for (auto &tv_expr : tv_exprs) {
      if (!output_cols_map.count(tv_expr)) {
        output_cols_map[tv_expr] = output_col_idx++;
      }
      // input_cols_set.insert(tv_expr);
    }
  }
  vector<AbstractExpression *> output_cols(output_col_idx, nullptr);
  for (auto &expr_idx_pair : output_cols_map) {
    output_cols[expr_idx_pair.second] = expr_idx_pair.first;
  }

  // TODO(boweic): do not use shared_ptr
  vector<shared_ptr<AbstractExpression>> groupby_cols;
  if (op->type() == OpType::HashGroupBy) {
    groupby_cols = reinterpret_cast<const PhysicalHashGroupBy *>(op)->columns;
  } else if (op->type() == OpType::SortGroupBy) {
    groupby_cols = reinterpret_cast<const PhysicalSortGroupBy *>(op)->columns;
  }
  for (auto &groupby_col : groupby_cols) {
    input_cols_set.insert(groupby_col.get());
  }
  vector<AbstractExpression *> input_cols;
  for (auto &col : input_cols_set) {
    input_cols.push_back(col);
  }

  output_input_cols_ =
      pair<vector<AbstractExpression *>, vector<vector<AbstractExpression *>>>{
          output_cols, {input_cols}};
}

void InputColumnDeriver::JoinHelper(const BaseOperatorNode *op) {
  const vector<AnnotatedExpression> *join_conds = nullptr;
  const vector<unique_ptr<expression::AbstractExpression>> *left_keys = nullptr;
  const vector<unique_ptr<expression::AbstractExpression>> *right_keys =
      nullptr;
  if (op->type() == OpType::InnerHashJoin) {
    auto join_op = reinterpret_cast<const PhysicalInnerHashJoin *>(op);
    join_conds = &(join_op->join_predicates);
    left_keys = &(join_op->left_keys);
    right_keys = &(join_op->right_keys);
  } else if (op->type() == OpType::InnerNLJoin) {
    auto join_op = reinterpret_cast<const PhysicalInnerNLJoin *>(op);
    join_conds = &(join_op->join_predicates);
    left_keys = &(join_op->left_keys);
    right_keys = &(join_op->right_keys);
  }

  ExprSet input_cols_set;

  PL_ASSERT(left_keys != nullptr);
  PL_ASSERT(right_keys != nullptr);
  PL_ASSERT(join_conds != nullptr);
  for (auto &left_key : *left_keys) {
    expression::ExpressionUtil::GetTupleValueExprs(input_cols_set,
                                                   left_key.get());
  }
  for (auto &right_key : *right_keys) {
    expression::ExpressionUtil::GetTupleValueExprs(input_cols_set,
                                                   right_key.get());
  }
  for (auto &join_cond : *join_conds) {
    expression::ExpressionUtil::GetTupleValueExprs(input_cols_set,
                                                   join_cond.expr.get());
  }
  ExprMap output_cols_map;
  for (auto expr : required_cols_) {
    expression::ExpressionUtil::GetTupleValueExprs(output_cols_map, expr);
  }
  for (auto &expr_idx_pair : output_cols_map) {
    input_cols_set.insert(expr_idx_pair.first);
  }
  // Construct input columns
  ExprSet build_table_cols_set;
  ExprSet probe_table_cols_set;
  auto &build_table_aliases =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetTableAliases();
  UNUSED_ATTRIBUTE auto &probe_table_aliases =
      memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetTableAliases();
  for (auto &col : input_cols_set) {
    PL_ASSERT(col->GetExpressionType() == ExpressionType::VALUE_TUPLE);
    auto tv_expr = reinterpret_cast<expression::TupleValueExpression *>(col);
    if (build_table_aliases.count(tv_expr->GetTableName())) {
      build_table_cols_set.insert(col);
    } else {
      PL_ASSERT(probe_table_aliases.count(tv_expr->GetTableName()));
      probe_table_cols_set.insert(col);
    }
  }
  vector<AbstractExpression *> output_cols(output_cols_map.size(), nullptr);
  for (auto &expr_idx_pair : output_cols_map) {
    output_cols[expr_idx_pair.second] = expr_idx_pair.first;
  }
  vector<AbstractExpression *> build_cols;
  for (auto &col : build_table_cols_set) {
    build_cols.push_back(col);
  }
  vector<AbstractExpression *> probe_cols;
  for (auto &col : probe_table_cols_set) {
    probe_cols.push_back(col);
  }
  output_input_cols_ =
      pair<vector<AbstractExpression *>, vector<vector<AbstractExpression *>>>{
          output_cols, {build_cols, probe_cols}};
}
void InputColumnDeriver::Passdown() {
  output_input_cols_ =
      pair<vector<AbstractExpression *>, vector<vector<AbstractExpression *>>>{
          required_cols_, {required_cols_}};
}

}  // namespace optimizer
}  // namespace peloton
