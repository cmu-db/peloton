//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// project_info.cpp
//
// Identification: src/planner/project_info.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/project_info.h"

#include "executor/executor_context.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"

namespace peloton {
namespace planner {

/**
 * @brief Mainly release the expression in target list.
 */
ProjectInfo::~ProjectInfo() {
  for (auto target : target_list_) {
    delete target.second.expr;
  }
}

/**
 * @brief Evaluate projections from one or two source tuples and
 * put result in destination.
 * The destination should be pre-allocated by the caller.
 *
 * @warning Destination should not be the same as any source.
 *
 * @warning If target list and direct map list have overlapping
 * destination columns, the behavior is undefined.
 *
 * @warning If projected value is not inlined, only a shallow copy is written.
 *
 * @param dest    Destination tuple.
 * @param tuple1  Source tuple 1.
 * @param tuple2  Source tuple 2.
 * @param econtext  ExecutorContext for expression evaluation.
 */
bool ProjectInfo::Evaluate(storage::Tuple *dest, const AbstractTuple *tuple1,
                           const AbstractTuple *tuple2,
                           executor::ExecutorContext *econtext) const {
  // Get varlen pool
  type::AbstractPool *pool = nullptr;
  if (econtext != nullptr) pool = econtext->GetPool();

  // (A) Execute target list
  for (auto target : target_list_) {
    auto col_id = target.first;
    auto expr = target.second.expr;
    auto value = expr->Evaluate(tuple1, tuple2, econtext);

    dest->SetValue(col_id, value, pool);
  }

  // (B) Execute direct map
  for (auto dm : direct_map_list_) {
    auto dest_col_id = dm.first;
    // whether left tuple or right tuple ?
    auto tuple_index = dm.second.first;
    auto src_col_id = dm.second.second;

    if (tuple_index == 0) {
      type::Value value = (tuple1->GetValue(src_col_id));
      dest->SetValue(dest_col_id, value, pool);
    } else {
      type::Value value = (tuple2->GetValue(src_col_id));
      dest->SetValue(dest_col_id, value, pool);
    }
  }

  return true;
}

bool ProjectInfo::Evaluate(AbstractTuple *dest, const AbstractTuple *tuple1,
                           const AbstractTuple *tuple2,
                           executor::ExecutorContext *econtext) const {
  // (A) Execute target list
  for (auto target : target_list_) {
    auto col_id = target.first;
    auto expr = target.second.expr;
    auto value = expr->Evaluate(tuple1, tuple2, econtext);
    dest->SetValue(col_id, value);
  }

  // (B) Execute direct map
  for (auto dm : direct_map_list_) {
    auto dest_col_id = dm.first;
    // whether left tuple or right tuple ?
    auto tuple_index = dm.second.first;
    auto src_col_id = dm.second.second;

    if (tuple_index == 0) {
      type::Value val1 = (tuple1->GetValue(src_col_id));
      dest->SetValue(dest_col_id, val1);
    } else {
      type::Value val2 = (tuple2->GetValue(src_col_id));
      dest->SetValue(dest_col_id, val2);
    }
  }

  return true;
}

void ProjectInfo::PerformRebinding(
    BindingContext &output_context,
    const std::vector<const BindingContext *> &input_contexts) const {
  // (A) First go over the direct mapping, mapping attributes from the
  // appropriate input context to the output context
  for (auto &dm : direct_map_list_) {
    oid_t dest_col_id = dm.first;
    oid_t src_col_id = dm.second.second;

    const BindingContext *src_context = input_contexts[dm.second.first];
    const auto *dest_ai = src_context->Find(src_col_id);
    LOG_TRACE("Direct: Dest col %u is bound to AI %p", dest_col_id, dest_ai);
    output_context.BindNew(dest_col_id, dest_ai);
  }

  // (B) Add the attributes produced by the target list expressions
  for (auto &target : target_list_) {
    oid_t dest_col_id = target.first;
    auto &derived_attribute = const_cast<DerivedAttribute &>(target.second);

    PL_ASSERT(derived_attribute.expr != nullptr);

    LOG_TRACE("Binding target-list expressions ...");
    auto *expr =
        const_cast<expression::AbstractExpression *>(derived_attribute.expr);
    expr->PerformBinding(input_contexts);

    // Setup the result type of the derived attribute
    derived_attribute.attribute_info.type = expr->ResultType();

    // Setup the attribute id of the derived attribute
    derived_attribute.attribute_info.attribute_id = dest_col_id;

    const auto *dest_ai = &derived_attribute.attribute_info;
    LOG_TRACE("Target: Dest col %u is bound to AI %p", dest_col_id, dest_ai);
    output_context.BindNew(dest_col_id, dest_ai);
  }
}

void ProjectInfo::PartitionInputs(
    std::vector<std::vector<oid_t>> &input) const {
  for (const auto &dm : direct_map_list_) {
    oid_t src_input = dm.second.first;
    oid_t src_input_col = dm.second.second;
    if (src_input >= input.size()) {
      input.resize(src_input + 1);
    }
    input[src_input].push_back(src_input_col);
  }
}

std::string ProjectInfo::Debug() const {
  std::ostringstream buffer;
  buffer << "Target List: < DEST_column_id , expression >\n";
  for (auto &target : target_list_) {
    buffer << "Dest Col id: " << target.first << std::endl;
    buffer << "Expr: \n" << target.second.expr->GetInfo();
    buffer << std::endl;
  }
  buffer << "DirectMap List: < NEW_col_id , <tuple_idx , OLD_col_id>  > \n";
  for (auto &dmap : direct_map_list_) {
    buffer << "<" << dmap.first << ", <" << dmap.second.first << ", "
           << dmap.second.second << "> >\n";
  }
  std::string info = buffer.str();
  StringUtil::RTrim(info);
  return info;
}

hash_t ProjectInfo::Hash(const planner::DerivedAttribute &attribute) const {
  hash_t hash = HashUtil::Hash(&attribute.attribute_info.type.type_id);

  hash = HashUtil::CombineHashes(hash,
      HashUtil::Hash(&attribute.attribute_info.attribute_id));

  return HashUtil::CombineHashes(hash, attribute.expr->Hash());
}

hash_t ProjectInfo::Hash() const {
  hash_t hash = 0;

  for (size_t i = 0; i < target_list_.size(); i++) {
    Target t = target_list_[i];

    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&t.first));
    hash = HashUtil::CombineHashes(hash, Hash(t.second));
  }

  for (size_t i = 0; i < direct_map_list_.size(); i++) {
    DirectMap dm = direct_map_list_[i];

    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&dm.first));
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&dm.second.first));
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&dm.second.second));
  }

  return hash;
}

bool ProjectInfo::AreEqual(const planner::DerivedAttribute &A,
                           const planner::DerivedAttribute &B) const {
  if (A.attribute_info.type != B.attribute_info.type)
    return false;

  if (A.attribute_info.attribute_id != B.attribute_info.attribute_id)
    return false;

  return *A.expr == *B.expr;
}

bool ProjectInfo::operator==(const ProjectInfo &rhs) const {
  // TargetList
  size_t tl_size = target_list_.size();
  if (tl_size != rhs.GetTargetList().size())
    return false;

  for (size_t i = 0; i < tl_size; i++) {
    Target t = target_list_[i];
    Target other_t = rhs.GetTargetList()[i];
    if (t.first != other_t.first)
      return false;
    if (!AreEqual(t.second, other_t.second))
      return false;
  }

  // DirectMapList
  size_t dml_size = direct_map_list_.size();
  if (dml_size != rhs.GetDirectMapList().size())
    return false;

  for (size_t i = 0; i < dml_size; i++) {
    DirectMap dm = direct_map_list_[i];
    DirectMap other_dm = rhs.GetDirectMapList()[i];
    if (dm.first != other_dm.first)
      return false;
    else if (dm.second.first != other_dm.second.first)
      return false;
    else if (dm.second.second != other_dm.second.second)
      return false;
  }

  return true;
}

void ProjectInfo::VisitParameters(
    codegen::QueryParametersMap &map, std::vector<peloton::type::Value> &values,
    const std::vector<peloton::type::Value> &values_from_user) {
  if (IsNonTrivial()) {

    for (auto &target : GetTargetList()) {
      const auto &derived_attribute = target.second;
      PL_ASSERT(derived_attribute.expr != nullptr);

      auto *expr =
          const_cast<expression::AbstractExpression *>(derived_attribute.expr);
      expr->VisitParameters(map, values, values_from_user);
    }
  }
}

}  // namespace planner
}  // namespace peloton
