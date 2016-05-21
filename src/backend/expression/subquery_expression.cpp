//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// subquery_expression.cpp
//
// Identification: src/backend/expression/subquery_expression.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "subquery_expression.h"

#include <sstream>

#include "backend/common/logger.h"
#include "backend/executor/executor_context.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/storage/tuple.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace expression {

SubqueryExpression::SubqueryExpression(
    ExpressionType subqueryType, ValueType result_type, int subqueryId,
    const std::vector<int> &paramIdxs, const std::vector<int> &otherParamIdxs,
    UNUSED_ATTRIBUTE const std::vector<AbstractExpression *>& tveParams)
    : AbstractExpression(subqueryType, result_type),
      m_subqueryId(subqueryId),
      m_paramIdxs(paramIdxs),
      m_otherParamIdxs(otherParamIdxs){
  LOG_TRACE("SubqueryExpression %d", subqueryId);
}

Value SubqueryExpression::Evaluate(
    UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
    UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
    UNUSED_ATTRIBUTE executor::ExecutorContext *exeContext) const {
  // TODO: Get the subquery context

  // Get the subquery context with the last Evaluation result and parameters
  // used to obtain that result
  /*
SubqueryContext *context = exeContext.GetSubqueryContext(m_subqueryId);

bool hasPriorResult = (context != NULL) && context->hasValidResult();
bool paramsChanged = false;
ValueArray &parameterContainer = *(exeContext.GetParameterContainer());
LOG_TRACE("Running subquery: %d", m_subqueryId);

// Substitute parameters.
if (m_tveParams.Get() != NULL) {
  size_t paramsCnt = m_tveParams->size();
  for (size_t i = 0; i < paramsCnt; ++i) {
    AbstractExpression *tveParam = (*m_tveParams)[i];
    Value param = tveParam->Evaluate(tuple1, tuple2);
    // compare the new param value with the previous one. Since this
    // parameter is set
    // by this subquery, no other subquery can change its value. So, we
    // don't need to
    // save its value on the side for future compa.Isons.
    Value &prevParam = parameterContainer[m_paramIdxs[i]];
    if (hasPriorResult) {
      if (param.compare(prevParam) == VALUE_COMPARE_EQUAL) {
        continue;
      }
      paramsChanged = true;
    }
  // Update the value stored in the executor context's parameter
  container:
    prevParam = param.copyValue();
  }
}

// Note the other (non-tve) parameter values and check if they've changed
// since the last invocation.
if (hasPriorResult) {
  std::vector<Value> &lastParams = context->accessLastParams();
  PL_ASSERT(lastParams.size() == m_otherParamIdxs.size());
  for (size_t i = 0; i < lastParams.size(); ++i) {
    Value &prevParam = parameterContainer[m_otherParamIdxs[i]];
    if (lastParams[i].compare(prevParam) != VALUE_COMPARE_EQUAL) {
      lastParams[i] = prevParam.copyValue();
      paramsChanged = true;
    }
  }
  if (paramsChanged) {
    // If parameters have changed since the last execution,
    // the cached result of the prior execution.Is obsolete.
    // In particular, it should not be .Istaken for the correct result for
    // the current
    // parameters in the event that the current execution fails.
    // T.Is subquery context will be restored to validity when its new
    // result is set
    // after execution succeeds.
    context->invalidateResult();
  } else {
    // If the parameters haven't changed since the last execution, reuse
    // the known result.
    return context.GetResult();
  }
}

// Out of luck. Need to run the executors. Clean up the output tables with
// cached results
exeContext->cleanupExecutorsForSubquery(m_subqueryId);
exeContext->executeExecutors(m_subqueryId);

if (context == NULL) {
  // Preserve the value for the next run. Only 'other' parameters need to be
  copied std::vector<Value> lastParams;
  lastParams.reserve(m_otherParamIdxs.size());
  for (size_t i = 0; i < m_otherParamIdxs.size(); ++i) {
    Value &prevParam = parameterContainer[m_otherParamIdxs[i]];
    lastParams.push_back(prevParam.copyValue());
  }
  context = exeContext->setSubqueryContext(m_subqueryId, lastParams);
}

// Update the cached result for the current params. All params are already
// updated
Value retval = ValueFactory.GetIntegerValue(m_subqueryId);
context->setResult(retval);
*/
  Value retval;
  return retval;
}

std::string SubqueryExpression::DebugInfo(const std::string &spacer) const {
  std::ostringstream buffer;
  buffer << spacer << ExpressionTypeToString(GetExpressionType())
         << ": subqueryId: " << m_subqueryId;
  return (buffer.str());
}

}  // End expression namespace
}  // End peloton namespace
