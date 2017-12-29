//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// projection_translator.cpp
//
// Identification: src/codegen/operator/projection_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/projection_translator.h"

#include "codegen/compilation_context.h"
#include "common/logger.h"
#include "planner/projection_plan.h"

namespace peloton {
namespace codegen {

ProjectionTranslator::ProjectionTranslator(const planner::ProjectionPlan &plan,
                                           CompilationContext &context,
                                           Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), plan_(plan) {
  // Prepare translator for our child
  PL_ASSERT(plan.GetChildrenSize() < 2);
  context.Prepare(*plan_.GetChild(0), pipeline);

  // Prepare translators for the projection
  const auto *projection_info = plan_.GetProjectInfo();
  PrepareProjection(context, *projection_info);
}

void ProjectionTranslator::Produce() const {
  GetCompilationContext().Produce(*plan_.GetChild(0));
}

void ProjectionTranslator::Consume(ConsumerContext &context,
                                   RowBatch::Row &row) const {
  // Add attribute accessors for all non-trivial (i.e. derived) attributes
  std::vector<RowBatch::ExpressionAccess> accessors;
  AddNonTrivialAttributes(row.GetBatch(), *plan_.GetProjectInfo(), accessors);

  // That's it
  context.Consume(row);
}

std::string ProjectionTranslator::GetName() const {
  bool non_trivial = plan_.GetProjectInfo()->IsNonTrivial();
  std::string ret = "Projection";
  ret.append(non_trivial ? "(non-trivial)" : "(trivial)");
  return ret;
}

void ProjectionTranslator::PrepareProjection(
    CompilationContext &context, const planner::ProjectInfo &projection_info) {
  // If the projection is non-trivial, we need to prepare translators for every
  // target expression
  if (projection_info.IsNonTrivial()) {
    for (const auto &target : projection_info.GetTargetList()) {
      const auto &derived_attribute = target.second;
      PL_ASSERT(derived_attribute.expr != nullptr);
      context.Prepare(*derived_attribute.expr);
    }
  }
}

void ProjectionTranslator::AddNonTrivialAttributes(
    RowBatch &row_batch, const planner::ProjectInfo &projection_info,
    std::vector<RowBatch::ExpressionAccess> &accessors) {
  // If the projection is non-trivial, we need to add attribute accessors for
  // all targets
  if (projection_info.IsNonTrivial()) {
    // Construct an accessor for each target
    const auto &target_list = projection_info.GetTargetList();
    for (uint32_t i = 0; i < target_list.size(); i++) {
      const auto &derived_attribute = target_list[i].second;
      accessors.emplace_back(*derived_attribute.expr);
    }

    // Add each accessor to the batch
    for (uint32_t i = 0; i < target_list.size(); i++) {
      const auto &attribute_info = target_list[i].second.attribute_info;
      LOG_DEBUG("Adding attribute '%s' (%p) to batch",
                attribute_info.name.c_str(), &attribute_info);
      row_batch.AddAttribute(&attribute_info, &accessors[i]);
    }
  }
}

}  // namespace codegen
}  // namespace peloton