//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_model_factory.h
//
// Identification: src/include/optimizer/cost_model_factory.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "optimizer/cost_calculator.h"

#include "common/exception.h"

namespace peloton {
namespace optimizer {

class CostCalculatorFactory {
 public:
  /*
   * Creates the respective cost calculator given a cost calculator name
   */
  static std::unique_ptr<AbstractCostCalculator> CreateCostCalculator(
      const std::string &cost_model_name);
};

}  // namespace peloton
}  // namespace optimizer
