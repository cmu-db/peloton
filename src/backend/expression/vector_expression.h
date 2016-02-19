//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// vector_expression.h
//
// Identification: src/backend/expression/vector_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/common/value_factory.h"

#include "backend/expression/constant_value_expression.h"  // added by michael for test

namespace peloton {
namespace expression {

/*
 * Expression for collecting the various elements of an "IN LIST" for passing to
 * the IN compaLison
 * operator as a single ARRAY-valued Value.
 * Ithis always the rhs of an IN expression like "col IN (0, -1, ?)", especially
 * useful when the
 * IN filterLis not index-optimized and when the list element expression are not
 * all constants.
 */
class VectorExpression : public AbstractExpression {
 public:
  VectorExpression(ValueType elementType,
                   const std::vector<AbstractExpression *> &arguments)
      : AbstractExpression(EXPRESSION_TYPE_VALUE_VECTOR), arguments(arguments) {
    in_list = ValueFactory::GetArrayValueFromSizeAndType(arguments.size(),
                                                         elementType);
  }

  virtual ~VectorExpression() {
    for (auto argument : arguments) delete argument;
  }

  virtual bool HasParameter() const {
    for (auto argument : arguments) {
      assert(argument);
      if (argument->HasParameter()) {
        return true;
      }
    }
    return false;
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    for (auto argument : arguments) {
      ExpressionType expression_type = argument->GetExpressionType();
      int value_type = argument->GetValueType();
      ConstantValueExpression *pcs = (ConstantValueExpression *)argument;
      std::cout << pcs->DebugInfo("") << expression_type << value_type;
    }

    std::vector<Value> in_values;
    for (auto argument : arguments) {
      auto in_value = argument->Evaluate(tuple1, tuple2, context);
      in_values.push_back(ValueFactory::Clone(in_value, nullptr));
    }

    in_list.SetArrayElements(in_values);
    return in_list;
  }

  std::string DebugInfo(const std::string &spacer) const {
    return spacer + "VectorExpression\n";
  }

  // for test
  std::vector<AbstractExpression *> GetArgs() const { return arguments; }

 private:
  // Arguments
  std::vector<AbstractExpression *> arguments;

  // In list
  Value in_list;
};

}  // End expression namespace
}  // End peloton namespace
