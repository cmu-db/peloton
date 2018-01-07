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

#include "common/printable.h"
#include "planner/attribute_info.h"
#include "expression/parameter.h"
#include "codegen/query_parameters_map.h"
#include "common/internal_types.h"
#include "storage/zone_map_manager.h"

namespace peloton {

// Forward Declaration
class Printable;
class AbstractTuple;
class SqlNodeVisitor;
enum class ExpressionType;

namespace codegen {
namespace type {
class Type;
}  // namespace type
}  // namespace codegen

namespace executor {
class ExecutorContext;
}  // namespace executor

namespace planner {
class BindingContext;
}  // namespace planner

namespace type {
class Value;
}  // namespace type

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

  /**
   * @brief Apply the operator to the inputs and produce ouput
   *
   * This will be removed in the future and replaced by the
   * the LLVM engine. You should not modify any Evaluate methods
   * nor should you add new ones.
   *
   * @param tuple1
   * @param tuple2
   * @param context
   * @return
   * @deprecated
   */
  virtual type::Value Evaluate(const AbstractTuple *tuple1,
                               const AbstractTuple *tuple2,
                               executor::ExecutorContext *context) const = 0;

  /**
   * Return true if this expression or any descendent has a value that should be
   * substituted with a parameter.
   */
  virtual bool HasParameter() const;

  virtual bool IsNullable() const;

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

  //===----------------------------------------------------------------------===//
  // Utilities and members for Zone Map consumption.
  //===----------------------------------------------------------------------===//
  bool IsZoneMappable();

  size_t GetNumberofParsedPredicates() const {
    return parsed_predicates.size();
  }

  const std::vector<storage::PredicateInfo> *GetParsedPredicates() const {
    return &parsed_predicates;
  }

  void ClearParsedPredicates() { parsed_predicates.clear(); }

  std::vector<storage::PredicateInfo> parsed_predicates;
  //===----------------------------------------------------------------------===//
  
  /** accessors */

  ExpressionType GetExpressionType() const { return exp_type_; }

  type::TypeId GetValueType() const { return return_value_type_; }

  codegen::type::Type ResultType() const;

  // Attribute binding
  virtual void PerformBinding(
      const std::vector<const planner::BindingContext *> &binding_contexts);

  // Is this expression computable using SIMD instructions?
  virtual bool IsSIMDable() const {
    for (uint32_t i = 0; i < GetChildrenSize(); i++) {
      if (!children_[i]->IsSIMDable()) {
        return false;
      }
    }
    return true;
  }

  // Get all the attributes this expression uses
  virtual void GetUsedAttributes(
      std::unordered_set<const planner::AttributeInfo *> &attributes) const;

  virtual void DeduceExpressionType() {}

  // Walks the expressoin trees and generate the correct expression name
  virtual void DeduceExpressionName();

  const std::string GetInfo() const;

  // Equlity checks without actual values
  virtual bool operator==(const AbstractExpression &rhs) const;
  virtual bool operator!=(const AbstractExpression &rhs) const {
    return !(*this == rhs);
  }
  virtual hash_t Hash() const;

  // Exact match including value equality
  virtual bool ExactlyEquals(const AbstractExpression &other) const;
  virtual hash_t HashForExactMatch() const;

  virtual void VisitParameters(codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) {
    for (auto &child : children_) {
      child->VisitParameters(map, values, values_from_user);
    }
  };

  virtual AbstractExpression *Copy() const = 0;

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  // Each sub-class will have to implement this function
  //===--------------------------------------------------------------------===//

  // virtual bool SerializeTo(SerializeOutput &output) const {}

  // virtual bool DeserializeFrom(SerializeInput &input) const {

  virtual int SerializeSize() const { return 0; }

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
  AbstractExpression(ExpressionType exp_type, type::TypeId return_value_type)
      : exp_type_(exp_type), return_value_type_(return_value_type) {}
  AbstractExpression(ExpressionType exp_type, type::TypeId return_value_type,
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
  type::TypeId return_value_type_ = type::TypeId::INVALID;

  std::vector<std::unique_ptr<AbstractExpression>> children_;

  bool has_parameter_ = false;
};

// Equality Comparator class for Abstract Expression
class ExprEqualCmp {
 public:
  inline bool operator()(std::shared_ptr<AbstractExpression> expr1,
                         std::shared_ptr<AbstractExpression> expr2) const {
    return expr1.get()->ExactlyEquals(*expr2.get());
  }
};

// Hasher class for Abstract Expression
class ExprHasher {
 public:
  inline size_t operator()(std::shared_ptr<AbstractExpression> expr) const {
    return expr->HashForExactMatch();
  }
};

}  // namespace expression
}  // namespace peloton
