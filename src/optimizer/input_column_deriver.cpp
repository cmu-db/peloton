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

namespace peloton {
namespace optimizer {

InputColumnDeriver::InputColumnDeriver() {}

pair<vector<expression::AbstractExpression *>,
     vector<vector<expression::AbstractExpression *>>>
InputColumnDeriver::DeriveInputColumns(
    GroupExpression *gexpr,
    vector<expression::AbstractExpression *> required_cols, Memo *memo) {
  gexpr_ = gexpr;
  required_cols_ = move(required_cols);
  memo_ = memo;
  gexpr->Op().Accept(this);

  return move(output_input_cols_);
}

void InputColumnDeriver::Visit(const DummyScan *) {
  // Scan does not have input column
  ScanHelper();
}

void InputColumnDeriver::Visit(const PhysicalSeqScan *) { ScanHelper(); }

void InputColumnDeriver::Visit(const PhysicalIndexScan *) { ScanHelper(); }
// TODO QueryDerivedScan should be deprecated
void InputColumnDeriver::Visit(const QueryDerivedScan *) { ScanHelper(); }
// project should be added after this step
void InputColumnDeriver::Visit(const PhysicalProject *) { PL_ASSERT(false); }

void InputColumnDeriver::Visit(const PhysicalLimit *) { Passdown(); }

void InputColumnDeriver::Visit(const PhysicalOrderBy *) {}

void InputColumnDeriver::Visit(const PhysicalHashGroupBy *) {}

void InputColumnDeriver::Visit(const PhysicalSortGroupBy *) {}

void InputColumnDeriver::Visit(const PhysicalAggregate *) {}

void InputColumnDeriver::Visit(const PhysicalDistinct *) { Passdown(); }

void InputColumnDeriver::Visit(const PhysicalFilter *) {}

void InputColumnDeriver::Visit(const PhysicalInnerNLJoin *) {}

void InputColumnDeriver::Visit(const PhysicalLeftNLJoin *) {}

void InputColumnDeriver::Visit(const PhysicalRightNLJoin *) {}

void InputColumnDeriver::Visit(const PhysicalOuterNLJoin *) {}

void InputColumnDeriver::Visit(const PhysicalInnerHashJoin *) {}

void InputColumnDeriver::Visit(const PhysicalLeftHashJoin *) {}

void InputColumnDeriver::Visit(const PhysicalRightHashJoin *) {}

void InputColumnDeriver::Visit(const PhysicalOuterHashJoin *) {}

void InputColumnDeriver::Visit(const PhysicalInsert *) {}

void InputColumnDeriver::Visit(const PhysicalInsertSelect *) {}

void InputColumnDeriver::Visit(const PhysicalDelete *) {}

void InputColumnDeriver::Visit(const PhysicalUpdate *) {}

void InputColumnDeriver::ScanHelper() {
  // Scan does not have input column, output columns should contain all tuple
  // value expressions needed
  output_input_cols_ = pair<vector<expression::AbstractExpression *>,
                            vector<vector<expression::AbstractExpression *>>>{
      required_cols_, {}};
}

void InputColumnDeriver::Passdown() {
  output_input_cols_ = pair<vector<expression::AbstractExpression *>,
                            vector<vector<expression::AbstractExpression *>>>{
      required_cols_, {required_cols_}};
}
}  // namespace optimizer
}  // namespace peloton
