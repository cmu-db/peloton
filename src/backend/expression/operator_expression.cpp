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
#include "backend/common/executor_context.h"
#include "backend/common/value.h"
#include "backend/common/value_peeker.h"

#include "backend/storage/tuple.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace expression {

Value OperatorExistsExpression::eval(const AbstractTuple *tuple1, const TableTuple *tuple2) const
{
    // Execute the subquery and get its subquery id
    assert(m_left != NULL);
    Value lnv = m_left->eval(tuple1, tuple2);
    int subqueryId = ValuePeeker::peekInteger(lnv);

    // Get the subquery context

    ExecutorContext* exeContext = ExecutorContext::getExecutorContext();

    // The EXISTS (SELECT inner_expr ...) evaluates as follows:
    // The subquery produces a row => TRUE
    // The subquery produces an empty result set => FALSE
    Table* outputTable = exeContext->getSubqueryOutputTable(subqueryId);
    assert(outputTable != NULL);
    if (outputTable->activeTupleCount() > 0) {
        return Value::getTrue();
    } else {
        return Value::getFalse();
    }
}

}  // End expression namespace
}  // End peloton namespace

