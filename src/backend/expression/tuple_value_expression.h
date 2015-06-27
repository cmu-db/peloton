#pragma once

#include "backend/expression/abstract_expression.h"

#include <string>
#include <sstream>

namespace peloton {
namespace expression {

class SerializeInput;
class SerializeOutput;

//===--------------------------------------------------------------------===//
// Tuple Value Expression
//===--------------------------------------------------------------------===//

class TupleValueExpressionMarker {
 public:
  virtual ~TupleValueExpressionMarker(){}
  virtual int GetColumnId() const = 0;
};

class TupleValueExpression : public AbstractExpression, public TupleValueExpressionMarker {

 public:

  TupleValueExpression(int tuple_idx, int value_idx, std::string table_name, std::string col_name)
 : AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE) {
    this->tuple_idx = tuple_idx;
    this->value_idx = value_idx;
    this->table_name = table_name;
    this->column_name = col_name;
  };

  inline Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2)  const {
    if (tuple_idx == 0)
      return tuple1->GetValue(this->value_idx);
    else
      return tuple2->GetValue(this->value_idx);
  }

  std::string DebugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << spacer << "TupleValueReference[" << this->value_idx << "]\n";
    return (buffer.str());
  }

  int GetColumnId() const {
    return this->value_idx;
  }

  std::string GetTableName() {
    return table_name;
  }

  // Don't know this index until the executor examines the expression.
  void SetTupleIndex(int idx) {
    tuple_idx = idx;
  }

 protected:

  // which tuple. defaults to tuple1
  int tuple_idx;

  // which (offset) column of the tuple
  int value_idx;

  std::string table_name;
  std::string column_name;
};

} // End expression namespace
} // End peloton namespace

