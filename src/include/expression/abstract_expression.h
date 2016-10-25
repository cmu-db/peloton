//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_expression.h
//
// Identification: src/include/expression/abstract_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <string>

#include "common/serializeio.h"
#include "common/printable.h"
#include "common/types.h"
#include "common/value_factory.h"
#include "common/macros.h"
#include "common/logger.h"

namespace peloton {

class Printable;
class AbstractTuple;

namespace executor {
class ExecutorContext;
}

namespace expression {

//===----------------------------------------------------------------------===//
// AbstractExpression
//
// Predicate objects for filtering tuples during query execution.
// These objects are stored in query plans and passed to Storage Access Manager.
//
// An expression usually has a longer life cycle than an execution, because,
// for example, it can be cached and reused for several executions of the same
// query template. Moreover, those executions can run simultaneously.
// So, an expression should not store per-execution information in its states.
// An expression tree (along with the plan node tree containing it) should
// remain constant and read-only during an execution.
//===----------------------------------------------------------------------===//

using namespace peloton::common;

class AbstractExpression : public Printable {
 public:
  // destroy this node and all children
  virtual ~AbstractExpression() {
    if (left_ != nullptr)
      delete left_;
    if (right_ != nullptr)
      delete right_;
  }

  virtual Value Evaluate(const AbstractTuple *tuple1,
                              const AbstractTuple *tuple2,
                              executor::ExecutorContext *context) const = 0;

  /**
   * Return true if this expression or any descendent has a value that should be
   * substituted with a parameter.
   */
  virtual bool HasParameter() const {
    if (left_ != nullptr && left_->HasParameter())
      return true;
    if (right_ != nullptr && right_->HasParameter())
      return true;
    return false;
  }

  /** accessors */
  ExpressionType GetExpressionType() const { return exp_type_; }

  Type::TypeId GetValueType() const { return value_type_; }

  const AbstractExpression *GetLeft() const { return left_; }

  const AbstractExpression *GetRight() const { return right_; }

  AbstractExpression *GetModifiableLeft() { return left_; }

  AbstractExpression *GetModifiableRight() { return right_; } 

  void setLeftExpression(AbstractExpression *left) {
    left_ = left;
  }

  void setRightExpression(AbstractExpression *right) {
    right_ = right;
  }

  const std::string GetInfo() const {
    std::ostringstream os;

    os << "\tExpression :: "
       << " expression type = " << GetExpressionType() << ","
       << " value type = " << Type::GetInstance(GetValueType())->ToString() << ","
       << std::endl;

    return os.str();
  }

  virtual AbstractExpression *Copy() const = 0;

  inline AbstractExpression *CopyUtil(
      const AbstractExpression *expression) const {
    return (expression == nullptr) ? nullptr : expression->Copy();
  }

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  // Each sub-class will have to implement this function
  //===--------------------------------------------------------------------===//

  //virtual bool SerializeTo(SerializeOutput &output) const {}

  //virtual bool DeserializeFrom(SerializeInput &input) const {}

  virtual int SerializeSize() { return 0; }

  char* GetName() const {
    return name;
  }

  // Parser stuff
  int ival = 0;
  AbstractExpression *expr = nullptr;

  char *name = nullptr;
  char *column = nullptr;
  char *alias = nullptr;
  char *database = nullptr;

  bool distinct = false;

 protected:
  AbstractExpression(ExpressionType type) : exp_type_(type) {}
  AbstractExpression(ExpressionType exp_type, Type::TypeId type_id)
      : exp_type_(exp_type), value_type_(type_id) {}
  AbstractExpression(ExpressionType exp_type, Type::TypeId type_id,
                     AbstractExpression *left,
                     AbstractExpression *right)
      : exp_type_(exp_type), value_type_(type_id), left_(left), right_(right) {}

  ExpressionType exp_type_ = EXPRESSION_TYPE_INVALID;
  Type::TypeId value_type_ = Type::INVALID;

  AbstractExpression *left_ = nullptr;
  AbstractExpression *right_ = nullptr;

  bool has_parameter_ = false;
};

}  // End expression namespace
}  // End peloton namespace
