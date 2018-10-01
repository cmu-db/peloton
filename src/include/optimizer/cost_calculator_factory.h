//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_calculator_factory.h
//
// Identification: src/include/optimizer/cost_calculator_factory.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
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