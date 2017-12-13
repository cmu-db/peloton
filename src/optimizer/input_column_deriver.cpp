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

#include "optimizer/input_column_deriver.h"

#include "optimizer/operator_expression.h"
#include "optimizer/operators.h"
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
// TODO QueryDerivedScan should be deprecated
void InputColumnDeriver::Visit(const QueryDerivedScan *) { PL_ASSERT(false); }
// project should be added after this step
void InputColumnDeriver::Visit(const PhysicalProject *) { PL_ASSERT(false); }

void InputColumnDeriver::Visit(const PhysicalLimit *) { Passdown(); }

void InputColumnDeriver::Visit(const PhysicalOrderBy *) {
  // we need to pass down both required columns and sort columns
  auto prop = properties_->GetPropertyOfType(PropertyType::SORT);
  PL_ASSERT(prop.get() != nullptr);
  auto sort_prop = prop->As<PropertySort>();
  ExprSet input_cols_set;
  size_t sort_col_size = sort_prop->GetSortColumnSize();
  for (size_t idx = 0; idx < sort_col_size; ++idx) {
    input_cols_set.insert(sort_prop->GetSortColumn(idx));
  }
  for (auto expr : required_cols_) {
    expression::ExpressionUtil::GetTupleValueExprs(input_cols_set, expr);
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

void InputColumnDeriver::Visit(const PhysicalFilter *) { PL_ASSERT(false); }

void InputColumnDeriver::Visit(const PhysicalInnerNLJoin *) {}

void InputColumnDeriver::Visit(const PhysicalLeftNLJoin *) {}

void InputColumnDeriver::Visit(const PhysicalRightNLJoin *) {}

void InputColumnDeriver::Visit(const PhysicalOuterNLJoin *) {}

void InputColumnDeriver::Visit(const PhysicalInnerHashJoin *) {}

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
  for (auto expr : required_cols_) {
    // TODO(boweic): star expression should be eliminated in the binder
    // If we need to select * just add it to the output, since it already
    // contains all columns
    if (expr->GetExpressionType() == ExpressionType::STAR) {
      output_input_cols_ =
          pair<vector<AbstractExpression *>,
               vector<vector<AbstractExpression *>>>{{expr}, {}};
      return;
    }
  }
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
  for (auto expr : required_cols_) {
    expression::ExpressionUtil::GetTupleValueExprs(input_cols_set, expr);
  }

  // TODO(boweic): do not use shared_ptr
  vector<shared_ptr<AbstractExpression>> groupby_cols;
  if (op->type() == OpType::HashGroupBy) {
    groupby_cols = reinterpret_cast<const PhysicalHashGroupBy *>(op)->columns;
  } else if (op->type() == OpType::SortGroupBy) {
    groupby_cols = reinterpret_cast<const PhysicalSortGroupBy *>(op)->columns;
  }
  vector<AbstractExpression *> output_cols;
  for (auto &groupby_col : groupby_cols) {
    output_cols.push_back(groupby_col.get());
    input_cols_set.insert(groupby_col.get());
  }
  vector<AbstractExpression *> input_cols;
  for (auto &col : input_cols_set) {
    // output all required columns so that we could perform projection later
    output_cols.push_back(col);
    input_cols.push_back(col);
  }

  output_input_cols_ =
      pair<vector<AbstractExpression *>, vector<vector<AbstractExpression *>>>{
          output_cols, {input_cols}};
}

void InputColumnDeriver::JoinHelper(const BaseOperatorNode *op) {
  vector<AnnotatedExpression> join_conds;
  if (op->type() == OpType::InnerHashJoin) {
    join_conds =
        reinterpret_cast<const PhysicalInnerHashJoin *>(op)->join_predicates;
  } else if (op->type() == OpType::InnerNLJoin) {
    join_conds =
        reinterpret_cast<const PhysicalInnerNLJoin *>(op)->join_predicates;
  }

  ExprSet input_cols_set;

  for (auto &join_cond : join_conds) {
    expression::ExpressionUtil::GetTupleValueExprs(input_cols_set,
                                                   join_cond.expr.get());
  }
  ExprSet output_cols_set;
  for (auto expr : required_cols_) {
    expression::ExpressionUtil::GetTupleValueExprs(output_cols_set, expr);
  }
  input_cols_set.insert(output_cols_set.begin(), output_cols_set.end());
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
  vector<AbstractExpression *> output_cols;
  for (auto &col : output_cols_set) {
    output_cols.push_back(col);
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
