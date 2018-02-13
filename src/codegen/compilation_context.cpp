//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compilation_context.cpp
//
// Identification: src/codegen/compilation_context.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/compilation_context.h"

#include "common/logger.h"
#include "common/timer.h"

namespace peloton {
namespace codegen {

// Constructor
CompilationContext::CompilationContext(CodeContext &code,
                                       QueryState &query_state,
                                       const QueryParametersMap &parameters_map,
                                       ExecutionConsumer &execution_consumer)
    : code_context_(code),
      query_state_(query_state),
      parameter_cache_(parameters_map),
      exec_consumer_(execution_consumer),
      codegen_(code_context_),
      pipelines_(),
      main_pipeline_(*this) {}

// Prepare the translator for the given operator
void CompilationContext::Prepare(const planner::AbstractPlan &op,
                                 Pipeline &pipeline) {
  auto translator = translator_factory_.CreateTranslator(op, *this, pipeline);
  op_translators_.insert(std::make_pair(&op, std::move(translator)));
}

// Prepare the translator for the given expression
void CompilationContext::Prepare(const expression::AbstractExpression &exp) {
  auto translator = translator_factory_.CreateTranslator(exp, *this);
  exp_translators_.insert(std::make_pair(&exp, std::move(translator)));
}

// Produce tuples for the given operator
void CompilationContext::Produce(const planner::AbstractPlan &op) {
  auto *translator = GetTranslator(op);
  PELOTON_ASSERT(translator != nullptr);
  translator->Produce();
}

// Generate all plan functions for the given query
void CompilationContext::GeneratePlan(Query &query,
                                      QueryCompiler::CompileStats *stats) {
  // Start timing
  Timer<std::ratio<1, 1000>> timer;
  timer.Start();

  // First we prepare the consumer and translators for each plan node
  exec_consumer_.Prepare(*this);
  Prepare(query.GetPlan(), main_pipeline_);

  // Finalize the runtime state
  query_state_.FinalizeType(codegen_);

  if (stats != nullptr) {
    timer.Stop();
    stats->setup_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  LOG_TRACE("Main pipeline: %s", main_pipeline_.GetInfo().c_str());

  // Generate the helper functions the query needs
  GenerateHelperFunctions();

  // Generate the init() function
  llvm::Function *init = GenerateInitFunction();

  // Generate the plan() function
  llvm::Function *plan = GeneratePlanFunction(query.GetPlan());

  // Generate the  tearDown() function
  llvm::Function *tear_down = GenerateTearDownFunction();

  if (stats != nullptr) {
    timer.Stop();
    stats->ir_gen_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  // Next, we prepare the query statement with the functions we've generated
  Query::QueryFunctions funcs = {
      .init_func = init, .plan_func = plan, .tear_down_func = tear_down};
  bool prepared = query.Prepare(funcs);
  if (!prepared) {
    throw Exception{"There was an error preparing the compiled query"};
  }

  // We're done
  if (stats != nullptr) {
    timer.Stop();
    stats->jit_ms = timer.GetDuration();
  }
}

// Generate any helper functions that the query needs
void CompilationContext::GenerateHelperFunctions() {
  // Allow each operator to initialize its state
  for (auto &iter : op_translators_) {
    auto &translator = iter.second;
    translator->DefineAuxiliaryFunctions();
  }

  // Define each auxiliary producer function
  for (auto &iter : auxiliary_producers_) {
    const auto &plan = *iter.first;
    const auto &function_declaration = iter.second;
    FunctionBuilder func{code_context_, function_declaration};
    {
      // Let the plan produce
      Produce(plan);
      // That's it
      func.ReturnAndFinish();
    }
  }
}

// Generate code for the init() function of the query
llvm::Function *CompilationContext::GenerateInitFunction() {
  // Create function definition
  std::string name = StringUtil::Format("_%lu_init", code_context_.GetID());
  std::vector<FunctionDeclaration::ArgumentInfo> args = {
      {"queryState", query_state_.GetType()->getPointerTo()}};
  FunctionBuilder init_func{code_context_, name, codegen_.VoidType(), args};
  {
    // Let the consumer initialize
    exec_consumer_.InitializeQueryState(*this);

    // Allow each operator to initialize their state
    for (auto &iter : op_translators_) {
      auto &translator = iter.second;
      translator->InitializeQueryState();
    }

    // Finish the function
    init_func.ReturnAndFinish();
  }

  // Get the generated function
  return init_func.GetFunction();
}

// Generate the code for the plan() function of the query
llvm::Function *CompilationContext::GeneratePlanFunction(
    const planner::AbstractPlan &root) {
  std::string name = StringUtil::Format("_%lu_plan", code_context_.GetID());
  std::vector<FunctionDeclaration::ArgumentInfo> args = {
      {"queryState", query_state_.GetType()->getPointerTo()}};
  FunctionBuilder plan_func{code_context_, name, codegen_.VoidType(), args};
  {
    // Generate the primary plan logic
    Produce(root);
    // Finish the function
    plan_func.ReturnAndFinish();
  }

  // Get the function
  return plan_func.GetFunction();
}

// Generate the code for the tearDown() function of the query
llvm::Function *CompilationContext::GenerateTearDownFunction() {
  std::string name = StringUtil::Format("_%lu_tearDown", code_context_.GetID());
  std::vector<FunctionDeclaration::ArgumentInfo> args = {
      {"queryState", query_state_.GetType()->getPointerTo()}};
  FunctionBuilder tear_down_func{code_context_, name, codegen_.VoidType(),
                                 args};
  {
    // Let the consumer cleanup
    exec_consumer_.TearDownQueryState(*this);

    // Allow each operator to clean up their state
    for (auto &iter : op_translators_) {
      auto &translator = iter.second;
      translator->TearDownQueryState();
    }

    // Finish the function
    tear_down_func.ReturnAndFinish();
  }
  // Get the function
  return tear_down_func.GetFunction();
}

// Get the registered translator for the given expression
ExpressionTranslator *CompilationContext::GetTranslator(
    const expression::AbstractExpression &exp) const {
  auto iter = exp_translators_.find(&exp);
  return iter == exp_translators_.end() ? nullptr : iter->second.get();
}

// Get the registered translator for the given operator
OperatorTranslator *CompilationContext::GetTranslator(
    const planner::AbstractPlan &op) const {
  auto iter = op_translators_.find(&op);
  return iter == op_translators_.end() ? nullptr : iter->second.get();
}

AuxiliaryProducerFunction CompilationContext::DeclareAuxiliaryProducer(
    const planner::AbstractPlan &plan, const std::string &provided_name) {
  auto iter = auxiliary_producers_.find(&plan);
  if (iter != auxiliary_producers_.end()) {
    const auto &declaration = iter->second;
    return AuxiliaryProducerFunction(declaration);
  }

  // Make the declaration for the caller to use

  std::string fn_name;
  if (!provided_name.empty()) {
    fn_name = provided_name;
  } else {
    fn_name = StringUtil::Format("_%lu_auxPlanFunction", code_context_.GetID());
  }

  std::vector<FunctionDeclaration::ArgumentInfo> fn_args = {
      {"queryState", query_state_.GetType()->getPointerTo()}};

  auto declaration = FunctionDeclaration::MakeDeclaration(
      code_context_, fn_name, FunctionDeclaration::Visibility::Internal,
      codegen_.VoidType(), fn_args);

  // Save the function declaration for later definition
  auxiliary_producers_.emplace(&plan, declaration);

  return AuxiliaryProducerFunction(declaration);
}

}  // namespace codegen
}  // namespace peloton
