

#ifndef HSTORETUPLEVALUEEXPRESSION_H
#define HSTORETUPLEVALUEEXPRESSION_H

#include "expressions/abstractexpression.h"
#include "common/tabletuple.h"

#include <string>
#include <sstream>

namespace voltdb {

class SerializeInput;
class SerializeOutput;

class TupleValueExpressionMarker {
public:
        virtual ~TupleValueExpressionMarker(){}
        virtual int getColumnId() const = 0;
};

class TupleValueExpression : public AbstractExpression, public TupleValueExpressionMarker {
  public:
    TupleValueExpression(int value_idx, std::string tableName, std::string colName)
        : AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE)
    {
        VOLT_TRACE("OptimizedTupleValueExpression %d %d", m_type, value_idx);
        this->tuple_idx = 0;
        this->value_idx = value_idx;
        this->table_name = tableName;
        this->column_name = colName;
    };

    inline voltdb::NValue eval(const TableTuple *tuple1, const TableTuple *tuple2)  const {
        if (tuple_idx == 0)
            return tuple1->getNValue(this->value_idx);
        else
            return tuple2->getNValue(this->value_idx);
    }

    std::string debugInfo(const std::string &spacer) const {
        std::ostringstream buffer;
        buffer << spacer << "Optimized Column Reference[" << this->value_idx << "]\n";
        return (buffer.str());
    }

    int getColumnId() const {return this->value_idx;}

    std::string getTableName() {
        return table_name;
    }

    // Don't know this index until the executor examines the expression.
    void setTupleIndex(int idx) {
        tuple_idx = idx;
    }

  protected:

    int tuple_idx;           // which tuple. defaults to tuple1
    int value_idx;           // which (offset) column of the tuple
    std::string table_name;
    std::string column_name;
};

}
#endif
