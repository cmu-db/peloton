//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_model_factory.cpp
//
// Identification: src/optimizer/cost_model_factory.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/cost_calculator_factory.h"
#include "optimizer/cost_calculator.h"

#include "common/exception.h"

namespace peloton {
namespace optimizer {

std::unique_ptr<AbstractCostCalculator>
CostCalculatorFactory::CreateCostCalculator(
    const std::string &cost_model_name) {
  if (cost_model_name == "CostCalculator") {
    return std::unique_ptr<AbstractCostCalculator>(new CostCalculator);
  } else {
    throw OptimizerException("Could not create cost calculator: `" +
                             cost_model_name + "`");
  }
}

}  // namesapce peloton
}  // namesapce optimizer
