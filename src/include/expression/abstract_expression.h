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

#include "common/logger.h"
#include "common/macros.h"
#include "common/printable.h"
#include "type/serializeio.h"
#include "type/types.h"
#include "type/value_factory.h"
#include "common/sql_node_visitor.h"
#include "util/hash_util.h"

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

class AbstractExpression : public Printable {
 public:
  virtual type::Value Evaluate(const AbstractTuple *tuple1,
                               const AbstractTuple *tuple2,
                               executor::ExecutorContext *context) const = 0;

  /**
   * Return true if this expression or any descendent has a value that should be
   * substituted with a parameter.
   */
  virtual bool HasParameter() const {
    for (auto &child : children_) {
      if (child->HasParameter()) {
        return true;
      }
    }
    return false;
  }

  const AbstractExpression *GetChild(int index) const {
    return GetModifiableChild(index);
  }

  size_t GetChildrenSize() const { return children_.size(); }

  AbstractExpression *GetModifiableChild(int index) const {
    if (index < 0 || index >= (int)children_.size()) {
      return nullptr;
    }
    return children_[index].get();
  }

  void SetChild(int index, AbstractExpression *expr) {
    if (index >= (int)children_.size()) {
      children_.resize(index + 1);
    }
    children_[index].reset(expr);
  }

  /** accessors */

  inline ExpressionType GetExpressionType() const { return exp_type_; }

  inline type::Type::TypeId GetValueType() const { return return_value_type_; }

  virtual void DeduceExpressionType() {}
  
  // Walks the expressoin trees and generate the correct expression name
  virtual void DeduceExpressionName() {
    // If alias exists, it will be used in TrafficCop
    if (!alias.empty())
      return;
    
    for (auto& child : children_)
      child->DeduceExpressionName();
    auto op_str = ExpressionTypeToString(exp_type_, true);
    auto children_size = children_.size();
    PL_ASSERT(children_size <= 2);
    if (children_size == 2) {
      expr_name_ = GetChild(0)->expr_name_ + " " + op_str + " " +
        GetChild(1)->expr_name_;
    } else if (children_size == 1) {
      expr_name_ = op_str + " " + GetChild(0)->expr_name_;
    }
  }

  const std::string GetInfo() const {
    std::ostringstream os;

    os << "\tExpression :: "
       << " expression type = " << GetExpressionType() << ","
       << " value type = "
       << type::Type::GetInstance(GetValueType())->ToString() << ","
       << std::endl;

    return os.str();
  }

  virtual bool Equals(AbstractExpression *expr) {
    if (exp_type_ != expr->exp_type_ ||
        children_.size() != expr->children_.size())
      return false;
    for (unsigned i = 0; i < children_.size(); i++) {
      if (!children_[i]->Equals(expr->children_[i].get()))
        return false;
    }
    return true;
  }

  virtual hash_t Hash() const {
    hash_t hash = HashUtil::Hash(&exp_type_);
    for (size_t i = 0; i < GetChildrenSize(); i++) {
      auto child = GetChild(i);
      hash = HashUtil::CombineHashes(hash, child->Hash());
    }
    return hash;
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

  // virtual bool SerializeTo(SerializeOutput &output) const {}

  // virtual bool DeserializeFrom(SerializeInput &input) const {

  virtual int SerializeSize() { return 0; }

  const char *GetExpressionName() const { return expr_name_.c_str(); }

  // Parser stuff
  int ival_ = 0;

  std::string expr_name_;
  std::string alias;

  bool distinct_ = false;

  virtual void Accept(SqlNodeVisitor *) = 0;

  virtual void AcceptChildren(SqlNodeVisitor *v) {
    for (auto &child : children_) {
      child->Accept(v);
    }
  }

 protected:
  AbstractExpression(ExpressionType type) : exp_type_(type) {}
  AbstractExpression(ExpressionType exp_type,
                     type::Type::TypeId return_value_type)
      : exp_type_(exp_type), return_value_type_(return_value_type) {}
  AbstractExpression(ExpressionType exp_type,
                     type::Type::TypeId return_value_type,
                     AbstractExpression *left, AbstractExpression *right)
      : exp_type_(exp_type), return_value_type_(return_value_type) {
    // Order of these is important!
    if (left != nullptr)
      children_.push_back(std::unique_ptr<AbstractExpression>(left));
    // Sometimes there's no right child. E.g.: OperatorUnaryMinusExpression.
    if (right != nullptr)
      children_.push_back(std::unique_ptr<AbstractExpression>(right));
  }
  AbstractExpression(const AbstractExpression &other)
      : ival_(other.ival_),
        expr_name_(other.expr_name_),
        distinct_(other.distinct_),
        exp_type_(other.exp_type_),
        return_value_type_(other.return_value_type_),
        has_parameter_(other.has_parameter_) {
    for (auto &child : other.children_) {
      children_.push_back(std::unique_ptr<AbstractExpression>(child->Copy()));
    }
  }

  ExpressionType exp_type_ = ExpressionType::INVALID;
  type::Type::TypeId return_value_type_ = type::Type::INVALID;

  std::vector<std::unique_ptr<AbstractExpression>> children_;

  bool has_parameter_ = false;
};

// Equality Comparator class for Abstract Expression
class ExprEqualCmp {
 public:
  inline bool operator()(AbstractExpression *expr1,
                         AbstractExpression *expr2) const {
    return expr1->Equals(expr2);
  }
};

// Hasher class for Abstract Expression
class ExprHasher {
 public:
  inline size_t operator()(AbstractExpression *expr) const {
    return expr->Hash();
  }
};

}  // End expression namespace
}  // End peloton namespace
