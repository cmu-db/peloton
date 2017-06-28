//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule.h
//
// Identification: src/include/optimizer/rule.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/pattern.h"

#include <memory>
#include "operator_expression.h"
#include "memo.h"

namespace peloton {
namespace optimizer {

class Rule {
 public:
  virtual ~Rule(){};

  std::shared_ptr<Pattern> GetMatchPattern() const { return match_pattern; }

  bool IsPhysical() const { return physical; }

  bool IsLogical() const { return logical; }

  virtual bool Check(std::shared_ptr<OperatorExpression> expr, Memo *memo) const = 0;

  virtual void Transform(
      std::shared_ptr<OperatorExpression> input,
      std::vector<std::shared_ptr<OperatorExpression>> &transformed) const = 0;

 protected:
  std::shared_ptr<Pattern> match_pattern;
  bool physical = false;
  bool logical = false;
};

} /* namespace optimizer */
} /* namespace peloton */
