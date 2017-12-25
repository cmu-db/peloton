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

#include "optimizer/child_property_deriver.h"
#include "optimizer/properties.h"
#include "optimizer/group_expression.h"
#include "optimizer/property_set.h"
#include "optimizer/memo.h"
#include "storage/data_table.h"

using std::move;
using std::vector;
using std::make_pair;
using std::vector;
using std::shared_ptr;
using std::pair;
using std::make_shared;

namespace peloton {
namespace optimizer {

vector<pair<shared_ptr<PropertySet>, vector<shared_ptr<PropertySet>>>>
ChildPropertyDeriver::GetProperties(GroupExpression *gexpr,
                                    shared_ptr<PropertySet> requirements,
                                    Memo *memo) {
  requirements_ = requirements;
  output_.clear();
  memo_ = memo;
  gexpr_ = gexpr;
  gexpr->Op().Accept(this);
  return move(output_);
}

void ChildPropertyDeriver::Visit(const PhysicalSeqScan *) {
  // Seq Scan does not provide any property
  output_.push_back(
      make_pair(make_shared<PropertySet>(), vector<shared_ptr<PropertySet>>{}));
};

void ChildPropertyDeriver::Visit(const PhysicalIndexScan *op) {
  auto provided_prop = make_shared<PropertySet>();
  storage::DataTable *target_table = op->table_;
  for (auto prop : requirements_->Properties()) {
    if (prop->Type() == PropertyType::SORT) {
      // Walk through all indices in the table, check if any of the index could
      // provide the sort property
      // TODO(boweic) : for now we only consider index built on one column since
      // from the current interface we cannot know the order of column in the
      // index
      auto sort_prop = prop->As<PropertySort>();
      if (sort_prop->GetSortColumnSize() > 1 ||
          sort_prop->GetSortColumn(0)->GetExpressionType() !=
              ExpressionType::VALUE_TUPLE) {
        break;
      }
      auto tv_expr = reinterpret_cast<expression::TupleValueExpression *>(
          sort_prop->GetSortColumn(0));
      auto obj_id = tv_expr->GetBoundOid();
      oid_t col_id = std::get<2>(obj_id);
      for (auto &column_set : target_table->GetIndexColumns()) {
        if (column_set.size() > 1) {
          continue;
        }
        if (column_set.count(col_id)) {
          provided_prop = requirements_;
          break;
        }
      }
    }
  }
  output_.push_back(
      make_pair(provided_prop, vector<shared_ptr<PropertySet>>{}));
}

void ChildPropertyDeriver::Visit(const QueryDerivedScan *) {
  output_.push_back(
      make_pair(requirements_, vector<shared_ptr<PropertySet>>{requirements_}));
}

/**
 * Note:
 * Fulfill the entire projection property in the aggregation. Should
 * enumerate different combination of the aggregation functions and other
 * projection.
 */
void ChildPropertyDeriver::Visit(const PhysicalHashGroupBy *) {
  output_.push_back(
      make_pair(make_shared<PropertySet>(),
                vector<shared_ptr<PropertySet>>{make_shared<PropertySet>()}));
}

void ChildPropertyDeriver::Visit(const PhysicalSortGroupBy *op) {
  // Child must provide sort for Groupby columns
  vector<bool> sort_acsending(op->columns.size(), true);
  vector<expression::AbstractExpression *> sort_cols;
  for (auto &col : op->columns) sort_cols.push_back(col.get());
  shared_ptr<Property> sort_prop(
      new PropertySort(sort_cols, move(sort_acsending)));
  shared_ptr<PropertySet> prop_set =
      make_shared<PropertySet>(vector<shared_ptr<Property>>{sort_prop});
  output_.push_back(
      make_pair(prop_set, vector<shared_ptr<PropertySet>>{prop_set}));
}

void ChildPropertyDeriver::Visit(const PhysicalAggregate *) {
  output_.push_back(
      make_pair(make_shared<PropertySet>(),
                vector<shared_ptr<PropertySet>>{make_shared<PropertySet>()}));
}

void ChildPropertyDeriver::Visit(const PhysicalLimit *) {
  // Let child fulfil all the required properties
  vector<shared_ptr<PropertySet>> child_input_properties{requirements_};

  output_.push_back(make_pair(requirements_, move(child_input_properties)));
}

void ChildPropertyDeriver::Visit(const PhysicalDistinct *) {
  // Let child fulfil all the required properties
  vector<shared_ptr<PropertySet>> child_input_properties{requirements_};

  output_.push_back(make_pair(requirements_, move(child_input_properties)));
}
void ChildPropertyDeriver::Visit(const PhysicalOrderBy *) {}
void ChildPropertyDeriver::Visit(const PhysicalInnerNLJoin *) {
  DeriveForJoin();
}
void ChildPropertyDeriver::Visit(const PhysicalLeftNLJoin *) {}
void ChildPropertyDeriver::Visit(const PhysicalRightNLJoin *) {}
void ChildPropertyDeriver::Visit(const PhysicalOuterNLJoin *) {}
void ChildPropertyDeriver::Visit(const PhysicalInnerHashJoin *) {
  DeriveForJoin();
}

void ChildPropertyDeriver::Visit(const PhysicalLeftHashJoin *) {}
void ChildPropertyDeriver::Visit(const PhysicalRightHashJoin *) {}
void ChildPropertyDeriver::Visit(const PhysicalOuterHashJoin *) {}
void ChildPropertyDeriver::Visit(const PhysicalInsert *) {
  vector<shared_ptr<PropertySet>> child_input_properties;

  output_.push_back(make_pair(requirements_, move(child_input_properties)));
}
void ChildPropertyDeriver::Visit(const PhysicalInsertSelect *) {
  // Let child fulfil all the required properties
  vector<shared_ptr<PropertySet>> child_input_properties{requirements_};

  output_.push_back(make_pair(requirements_, move(child_input_properties)));
}
void ChildPropertyDeriver::Visit(const PhysicalUpdate *) {
  // Let child fulfil all the required properties
  vector<shared_ptr<PropertySet>> child_input_properties{requirements_};

  output_.push_back(make_pair(requirements_, move(child_input_properties)));
}
void ChildPropertyDeriver::Visit(const PhysicalDelete *) {
  // Let child fulfil all the required properties
  vector<shared_ptr<PropertySet>> child_input_properties{requirements_};

  output_.push_back(make_pair(requirements_, move(child_input_properties)));
};

void ChildPropertyDeriver::Visit(const DummyScan *) {
  // Provide nothing
  output_.push_back(
      make_pair(make_shared<PropertySet>(), vector<shared_ptr<PropertySet>>()));
}

void ChildPropertyDeriver::DeriveForJoin() {
  output_.push_back(make_pair(
      make_shared<PropertySet>(),
      vector<shared_ptr<PropertySet>>(2, make_shared<PropertySet>())));

  // If there is sort property and all the sort columns are from the probe table
  // (currently right table), we can push down the sort property
  for (auto prop : requirements_->Properties()) {
    if (prop->Type() == PropertyType::SORT) {
      auto sort_prop = prop->As<PropertySort>();
      size_t sort_col_size = sort_prop->GetSortColumnSize();
      Group *probe_group = memo_->GetGroupByID(gexpr_->GetChildGroupId(1));
      bool can_pass_down = true;
      for (size_t idx = 0; idx < sort_col_size; ++idx) {
        ExprSet tuples;
        expression::ExpressionUtil::GetTupleValueExprs(
            tuples, sort_prop->GetSortColumn(idx));
        for (auto &expr : tuples) {
          auto tv_expr =
              reinterpret_cast<expression::TupleValueExpression *>(expr);
          // If a column is not in the prob table, we cannot fulfill the sort
          // property in the requirement
          if (!probe_group->GetTableAliases().count(tv_expr->GetTableName())) {
            can_pass_down = false;
            break;
          }
        }
        if (can_pass_down == false) {
          break;
        }
      }
      if (can_pass_down) {
        output_.push_back(make_pair(
            requirements_, vector<shared_ptr<PropertySet>>{
                               make_shared<PropertySet>(), requirements_}));
      }
    }
  }
}
}  // namespace optimizer
}  // namespace peloton
