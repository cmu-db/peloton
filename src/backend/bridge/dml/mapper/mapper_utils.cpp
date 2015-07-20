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
    ParamExternData *postgres_param = param_list->params;
    for (int i = 0; i < params.GetSize(); ++i, ++postgres_param) {
      params[i] = TupleTransformer::GetValue(postgres_param->value, postgres_param->ptype);
    }
  }
  LOG_INFO("Built param list of size %d", params.GetSize());
  return params;
}

} // namespace bridge
} // namespace peloton
