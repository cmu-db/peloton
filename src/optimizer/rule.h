//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule.h
//
// Identification: src/backend/optimizer/rule.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/optimizer/op_expression.h"
#include "backend/optimizer/pattern.h"

#include <memory>

namespace peloton {
namespace optimizer {

class Rule {
 public:
  virtual ~Rule() {};

  std::shared_ptr<Pattern> GetMatchPattern() const { return match_pattern; }

  bool IsPhysical() const { return physical; }

  bool IsLogical() const { return logical; }

  virtual bool Check(std::shared_ptr<OpExpression> expr) const = 0;

  virtual void Transform(
    std::shared_ptr<OpExpression> input,
    std::vector<std::shared_ptr<OpExpression>> &transformed) const = 0;

 protected:
  std::shared_ptr<Pattern> match_pattern;
  bool physical = false;
  bool logical = false;
};

} /* namespace optimizer */
} /* namespace peloton */
