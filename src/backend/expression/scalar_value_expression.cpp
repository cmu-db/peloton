//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// scalar_value_expression.cpp
//
// Identification: src/backend/expression/scalar_value_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/expression/scalar_value_expression.h"
#include "backend/common/value_peeker.h"
#include "backend/common/value.h"
#include "backend/storage/tuple.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace expression {

Value ScalarValueExpression::eval(const AbstractTuple *tuple1, const TableTuple *tuple2) const
{
    // Execute the subquery and get its subquery id
    assert(m_left != NULL);
    Value lnv = m_left->eval(tuple1, tuple2);
    int subqueryId = ValuePeeker::peekInteger(lnv);

    // Get the subquery context
    ExecutorContext* exeContext = ExecutorContext::getExecutorContext();
    Table* table = exeContext->getSubqueryOutputTable(subqueryId);
    assert(table != NULL);
    if (table->activeTupleCount() > 1) {
        // throw runtime exception
        char message[256];
        snprintf(message, 256, "More than one row returned by a scalar/row subquery");
        throw Exception(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }
    TableIterator& iterator = table->iterator();
    AbstractTuple tuple(table->schema());
    if (iterator.next(tuple)) {
        return tuple.getValue(0);
    } else {
        return Value::getNullValue(m_left->getValueType());
    }

}

std::string ScalarValueExpression::debugInfo(const std::string &spacer) const {
    return "ScalarValueExpression";
}

}  // End expression namespace
}  // End peloton namespace

