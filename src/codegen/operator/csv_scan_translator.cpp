//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scan_translator.cpp
//
// Identification: src/codegen/operator/csv_scan_translator.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/csv_scan_translator.h"

#include "codegen/compilation_context.h"
#include "codegen/function_builder.h"
#include "codegen/lang/if.h"
#include "codegen/operator/projection_translator.h"
#include "codegen/pipeline.h"
#include "codegen/proxy/csv_scanner_proxy.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/type/sql_type.h"
#include "planner/csv_scan_plan.h"

namespace peloton {
namespace codegen {

CSVScanTranslator::CSVScanTranslator(const planner::CSVScanPlan &scan,
                                     CompilationContext &context,
                                     Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), scan_(scan) {
  auto &runtime_state = context.GetRuntimeState();
  scanner_id_ = runtime_state.RegisterState(
      "csvScanner", CSVScannerProxy::GetType(GetCodeGen()));
}

void CSVScanTranslator::InitializeState() {
  auto &codegen = GetCodeGen();

  // Arguments
  auto *scanner_ptr = LoadStatePtr(scanner_id_);
  auto *exec_ctx_ptr = GetCompilationContext().GetExecutorContextPtr();
  auto *file_path = codegen.ConstString(scan_.GetFileName(), "filePath");
  auto *output_col_types = ConstructColumnDescriptor();
  auto *runtime_state_ptr = codegen->CreatePointerCast(
      codegen.GetState(), codegen.VoidType()->getPointerTo());

  std::vector<oid_t> out_cols;
  scan_.GetOutputColumns(out_cols);
  auto *num_output_cols =
      codegen.Const32(static_cast<uint32_t>(out_cols.size()));

  auto *consumer_func = codegen->CreatePointerCast(
      consumer_func_, proxy::TypeBuilder<void (*)(void *)>::GetType(codegen));

  // Call
  codegen.Call(CSVScannerProxy::Init,
               {scanner_ptr, exec_ctx_ptr, file_path, output_col_types,
                num_output_cols, consumer_func, runtime_state_ptr,
                codegen.Const8(scan_.GetDelimiterChar()),
                codegen.Const8(scan_.GetQuoteChar()),
                codegen.Const8(scan_.GetEscapeChar())});
}

void CSVScanTranslator::DefineAuxiliaryFunctions() {
  // Define consumer function here
  CodeGen &codegen = GetCodeGen();
  CompilationContext &cc = GetCompilationContext();

  std::vector<FunctionDeclaration::ArgumentInfo> arg_types = {
      {"runtimeState",
       cc.GetRuntimeState().FinalizeType(codegen)->getPointerTo()}};
  codegen::FunctionDeclaration decl{codegen.GetCodeContext(), "consumer",
                                    FunctionDeclaration::Visibility::Internal,
                                    codegen.VoidType(), arg_types};
  codegen::FunctionBuilder scan_consumer{codegen.GetCodeContext(), decl};
  {
    ConsumerContext ctx{cc, GetPipeline()};

    Vector v{nullptr, 1, nullptr};
    RowBatch one{GetCompilationContext(), codegen.Const32(0),
                 codegen.Const32(1), v, false};
    RowBatch::Row row{one, nullptr, nullptr};

    // Get the attributes
    std::vector<const planner::AttributeInfo *> output_attributes;
    scan_.GetAttributes(output_attributes);

    // Load the pointer to the columns view
    auto *cols = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
        CSVScannerProxy::GetType(codegen), LoadStatePtr(scanner_id_), 0, 4));

    // For each column, call the type's input function to read the input value
    for (uint32_t i = 0; i < output_attributes.size(); i++) {
      const auto *output_ai = output_attributes[i];

      const auto &sql_type = output_ai->type.GetSqlType();

      auto *is_null = codegen->CreateConstInBoundsGEP2_32(
          CSVScannerColumnProxy::GetType(codegen), cols, i, 3);

      codegen::Value val, null_val;
      lang::If not_null{codegen,
                        codegen->CreateNot(codegen->CreateLoad(is_null))};
      {
        // Grab a pointer to the ptr and length
        auto *type = codegen->CreatePointerCast(
            codegen.ConstType(output_ai->type),
            TypeProxy::GetType(codegen)->getPointerTo());
        auto *ptr = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
            CSVScannerColumnProxy::GetType(codegen), cols, i, 1));
        auto *len = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
            CSVScannerColumnProxy::GetType(codegen), cols, i, 2));

        // Invoke the input function
        auto *input_func = sql_type.GetInputFunction(codegen, output_ai->type);
        auto *raw_val = codegen.CallFunc(input_func, {type, ptr, len});

        // Non-null value
        val = codegen::Value{output_ai->type, raw_val, nullptr,
                             codegen.ConstBool(false)};
      }
      not_null.ElseBlock();
      {
        // Null value
        null_val = sql_type.GetNullValue(codegen);
      }
      not_null.EndIf();

      codegen::Value final_val = not_null.BuildPHI(val, null_val);
      row.RegisterAttributeValue(output_ai, final_val);
    }

    ctx.Consume(row);
    scan_consumer.ReturnAndFinish();
  }
  consumer_func_ = scan_consumer.GetFunction();
}

void CSVScanTranslator::Produce() const {
  auto *scanner_ptr = LoadStatePtr(scanner_id_);
  GetCodeGen().Call(CSVScannerProxy::Produce, {scanner_ptr});
}

void CSVScanTranslator::TearDownState() {
  auto *scanner_ptr = LoadStatePtr(scanner_id_);
  GetCodeGen().Call(CSVScannerProxy::Destroy, {scanner_ptr});
}

std::string CSVScanTranslator::GetName() const {
  return std::__cxx11::string();
}

llvm::Value *CSVScanTranslator::ConstructColumnDescriptor() const {
  // First, we pull out all the attributes produced by the scan, in order
  std::vector<const planner::AttributeInfo *> cols;
  scan_.GetAttributes(cols);

  // But, what we really need are just the column types, so pull those out now
  std::vector<codegen::type::Type> col_types_vec;
  for (const auto *col : cols) {
    col_types_vec.push_back(col->type);
  }

  CodeGen &codegen = GetCodeGen();

  auto num_bytes = cols.size() * sizeof(decltype(col_types_vec)::value_type);
  auto *bytes = codegen.ConstGenericBytes(
      col_types_vec.data(), static_cast<uint32_t>(num_bytes), "colTypes");
  return codegen->CreatePointerCast(
      bytes, TypeProxy::GetType(codegen)->getPointerTo());
}

}  // namespace codegen
}  // namespace peloton