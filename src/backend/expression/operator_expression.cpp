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

#include "common/debuglog.h"
#include "common/executorcontext.hpp"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"

#include "common/tabletuple.h"
#include "executors/executorutil.h"
#include "storage/table.h"
#include "storage/tableiterator.h"


namespace voltdb {

NValue OperatorExistsExpression::eval(const TableTuple *tuple1, const TableTuple *tuple2) const
{
    // Execute the subquery and get its subquery id
    assert(m_left != NULL);
    NValue lnv = m_left->eval(tuple1, tuple2);
    int subqueryId = ValuePeeker::peekInteger(lnv);

    // Get the subquery context

    ExecutorContext* exeContext = ExecutorContext::getExecutorContext();

    // The EXISTS (SELECT inner_expr ...) evaluates as follows:
    // The subquery produces a row => TRUE
    // The subquery produces an empty result set => FALSE
    Table* outputTable = exeContext->getSubqueryOutputTable(subqueryId);
    assert(outputTable != NULL);
    if (outputTable->activeTupleCount() > 0) {
        return NValue::getTrue();
    } else {
        return NValue::getFalse();
    }
}

}
