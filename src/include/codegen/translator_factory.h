//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// translator_factory.h
//
// Identification: src/include/codegen/translator_factory.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>

namespace peloton {

namespace expression {
class AbstractExpression;
}  // namespace expression

namespace planner {
class AbstractPlan;
}  // namespace planner

namespace codegen {

// Forward declare these guys so CompilationContext can include this file
class CompilationContext;
class ExpressionTranslator;
class OperatorTranslator;
class Pipeline;

//===----------------------------------------------------------------------===//
// An factory interface that creates the appropriate translator for a given
// query operator or expression.
//===----------------------------------------------------------------------===//
class TranslatorFactory {
 public:
  // Return a new unique translator for the given operator
  virtual std::unique_ptr<OperatorTranslator> CreateTranslator(
      const planner::AbstractPlan &plan, CompilationContext &context,
      Pipeline &pipeline) const;

  // Return a new unique translator for the given expression
  virtual std::unique_ptr<ExpressionTranslator> CreateTranslator(
      const expression::AbstractExpression &exp,
      CompilationContext &context) const;
};

}  // namespace codegen
}  // namespace peloton