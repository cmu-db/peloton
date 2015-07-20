/*-------------------------------------------------------------------------
 *
 * mapper_utils.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/dml/mapper/mapper_utils.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/dml/mapper/mapper.h"

namespace peloton {
namespace bridge {

static inline const ValueArray BuildParams(const ParamListInfo param_list);

//===--------------------------------------------------------------------===//
// Utils
//===--------------------------------------------------------------------===//

inline const ValueArray BuildParams(const ParamListInfo param_list) {
  ValueArray params;
  if (param_list != nullptr) {
    params.Reset(param_list->numParams);
    for (int i = 0; i < params.GetSize(); ++i) {
      ParamExternData *param = &(param_list->params[i]);
      params[i] = TupleTransformer::GetValue(param->value, param->ptype);
    }
  }
  LOG_INFO("Built param list of size %d", params.GetSize());
  return params;
}

} // namespace bridge
} // namespace peloton
