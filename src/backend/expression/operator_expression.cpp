//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// operator_expression.cpp
//
// Identification: src/backend/expression/operator_expression.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "operator_expression.h"

#include <sstream>

#include "backend/common/logger.h"
#include "backend/executor/executor_context.h"
#include "backend/common/value.h"
#include "backend/common/value_peeker.h"

#include "backend/storage/tuple.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace expression {

Value OperatorExistsExpression::Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                                         executor::ExecutorContext *context) const
{
    // Execute the subquery and.Get its subquery id
    assert(m_left != NULL);
    Value lnv = m_left->Evaluate(tuple1, tuple2, context);
    //int subqueryId = ValuePeeker::PeekInteger(lnv);

    // TODO: Get the subquery context
    // The EXISTS (SELECT inner_expr ...) Evaluateuates as follows:
    // The subquery produces a row => TRUE
    // The subquery produces an empty result set => FALSE
    /*
    Table* outputTable = exeContext-.GetSubqueryOutputTable(subqueryId);
    assert(outputTable != NULL);
    if (outputTable->activeTupleCount() > 0) {
        return Value::GetTrue();
    } else {
        return Value::GetFalse();
    }
    */
    return lnv;
}

}  // End expression namespace
}  // End peloton namespace

