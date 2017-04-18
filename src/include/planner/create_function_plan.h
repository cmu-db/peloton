//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_function_plan.h
//
// Identification: src/include/planner/create_function_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"

namespace peloton {

namespace parser {
class CreateFunctionStatement;
}

namespace planner {
class CreateFunctionPlan : public AbstractPlan {
  public:
  CreateFunctionPlan() = delete;
  CreateFunctionPlan(const CreateFunctionPlan &) = delete;
  CreateFunctionPlan &operator=(const CreateFunctionPlan &) = delete;
  CreateFunctionPlan(CreateFunctionPlan &&) = delete;
  CreateFunctionPlan &operator=(CreateFunctionPlan &&) = delete;

  // Temporary fix to handle Copy()
  explicit CreateFunctionPlan(std::string func);

  //explicit CreatePlan(std::string name, std::string database_name,
  //                    std::unique_ptr<catalog::Schema> schema,
  //                    CreateType c_type);

  explicit CreateFunctionPlan(parser::CreateFunctionStatement *parse_tree);

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::CREATE_FUNC; }

  const std::string GetInfo() const { return "Get Create Function Plan"; }

  // //////////////////////Check if this gets called later from somewhere else/////////////////////////
  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new CreateFunctionPlan("UDF function"));
  }

  inline std::string GetFunctionName() const { return function_name; }

  inline PLType GetUDFLanguage() const { return language; }

  inline std::vector<std::string> GetFunctionBody() const { return function_body; }

  inline std::vector<std::string> GetFunctionParameterNames() const { return function_param_names; }

  inline std::vector<type::Type::TypeId> GetFunctionParameterTypes() const { return function_param_types; }

  inline type::Type::TypeId GetReturnType() const { return return_type; }

  inline bool IsReplace() const { return is_replace; }

  private:

  //Indicates the UDF language type
  PLType language;

  // Function parameters names passed to the UDF
  std::vector<std::string> function_param_names;

  // Function parameter types passed to the UDF
  std::vector<type::Type::TypeId> function_param_types;

  // Query string/ function body of the UDF
  std::vector<std::string> function_body;

  // Indicates if the function definition needs to be replaced
  bool is_replace;

  // Function name of the UDF
  std::string function_name;

  // Return type of the UDF
  type::Type::TypeId return_type;

  int param_count = 0;

};
}
}
