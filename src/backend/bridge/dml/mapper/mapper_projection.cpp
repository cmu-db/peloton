/*-------------------------------------------------------------------------
 *
 * mapper_projection.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/dml/mapper/mapper_projection.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/dml/mapper/mapper.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Projection
//===--------------------------------------------------------------------===//

extern std::vector<std::pair<oid_t, expression::AbstractExpression*>>
TransformTargetList(List* target_list, oid_t column_count);

/**
 * @brief Transforms a PG ProjInfo into Peloton.
 * It includes both trivial and non-trivial projections.
 *
 * @param proj_info Postgres ProjectionInfo
 * @param column_count Max column column in the output schema.
 *  This is used to help discard junk attributes, as
 *  we don't need them in Peloton.
 *
 * @return A vector of <column_id, expression> pairs.
 */
std::vector<std::pair<oid_t, expression::AbstractExpression*>>
TransformProjInfo(const ProjectionInfo* proj_info, oid_t column_count) {
  std::vector<std::pair<oid_t, expression::AbstractExpression*>> proj_list;

  // 1. Extract the non-trivial projections (expression-based in Postgres)
  proj_list = TransformTargetList(proj_info->pi_targetlist, column_count);

  // 2. Extract the trivial projections
  // (simple var reference, such as 'SELECT b, b, a FROM' or 'SET a=b').
  // Postgres treats them in a short cut, but we don't (at least for now).
  if (proj_info->pi_numSimpleVars > 0)
  {
    int    numSimpleVars = proj_info->pi_numSimpleVars;
    auto    slot = proj_info->pi_slot;
    bool    *isnull = slot->tts_isnull;
    int     *varSlotOffsets = proj_info->pi_varSlotOffsets;
    int     *varNumbers = proj_info->pi_varNumbers;

    if (proj_info->pi_directMap) // Sequential direct map
    {
      /* especially simple case where vars go to output in order */
      for (int out_col_id = 0; out_col_id < numSimpleVars && out_col_id < column_count; out_col_id++)
      {
        // Input should be scan tuple
        assert(varSlotOffsets[out_col_id] ==  offsetof(ExprContext, ecxt_scantuple));

        int     varNumber = varNumbers[out_col_id] - 1;

        oid_t   in_col_id = static_cast<oid_t>(varNumber);

        if(!(isnull[out_col_id])){   // Non null, direct map
          oid_t tuple_idx = 0;
          proj_list.emplace_back(out_col_id,
                                 expression::TupleValueFactory(tuple_idx, in_col_id));
        }
        else { // NUll, constant
          Value null = ValueFactory::GetNullValue();
          proj_list.emplace_back(out_col_id,
                                 expression::ConstantValueFactory(null));
        }

        LOG_INFO("Input column : %u , Output column : %u \n", in_col_id, out_col_id);
      }
    }
    else  // Non-sequential direct map
    {
      /* we have to pay attention to varOutputCols[] */
      int      *varOutputCols = proj_info->pi_varOutputCols;

      for (int i = 0; i < numSimpleVars; i++)
      {
        // Input should be scan tuple
        assert(varSlotOffsets[i] ==  offsetof(ExprContext, ecxt_scantuple));

        int     varNumber = varNumbers[i] - 1;
        int     varOutputCol = varOutputCols[i] - 1;
        oid_t   in_col_id = static_cast<oid_t>(varNumber);
        oid_t   out_col_id = static_cast<oid_t>(varOutputCol);

        if(!(isnull[out_col_id])){   // Non null, direct map
          oid_t tuple_idx = 0;
          proj_list.emplace_back(out_col_id,
                                 expression::TupleValueFactory(tuple_idx, in_col_id));
        }
        else { // NUll, constant
          Value null = ValueFactory::GetNullValue();
          proj_list.emplace_back(out_col_id,
                                 expression::ConstantValueFactory(null));
        }

        LOG_INFO("Input column : %u , Output column : %u \n", in_col_id, out_col_id);
      }
    }
  }

  return proj_list;
}

} // namespace bridge
} // namespace peloton

