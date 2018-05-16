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
  // Register the CSV scanner instance
  auto &runtime_state = context.GetRuntimeState();
  scanner_id_ = runtime_state.RegisterState(
      "csvScanner", CSVScannerProxy::GetType(GetCodeGen()));

  // Load information about the attributes output by the scan plan
  scan_.GetAttributes(output_attributes_);
}

void CSVScanTranslator::InitializeState() {
  auto &codegen = GetCodeGen();

  // Arguments
  llvm::Value *scanner_ptr = LoadStatePtr(scanner_id_);
  llvm::Value *exec_ctx_ptr = GetCompilationContext().GetExecutorContextPtr();
  llvm::Value *file_path = codegen.ConstString(scan_.GetFileName(), "filePath");

  auto num_cols = static_cast<uint32_t>(output_attributes_.size());

  // We need to generate an array of type::Type. To do so, we construct a vector
  // of the types of the output columns, and we create an LLVM constant that is
  // a copy of the underlying bytes.

  std::vector<codegen::type::Type> col_types_vec;
  col_types_vec.reserve(num_cols);
  for (const auto *ai : output_attributes_) {
    col_types_vec.push_back(ai->type);
  }
  llvm::Value *raw_col_type_bytes = codegen.ConstGenericBytes(
      col_types_vec.data(), static_cast<uint32_t>(col_types_vec.capacity()),
      "colTypes");
  llvm::Value *output_col_types = codegen->CreatePointerCast(
      raw_col_type_bytes, TypeProxy::GetType(codegen)->getPointerTo());

  // Now create a pointer to the consumer function
  using ConsumerFuncType = void (*)(void *);
  llvm::Value *consumer_func = codegen->CreatePointerCast(
      consumer_func_, proxy::TypeBuilder<ConsumerFuncType>::GetType(codegen));

  // Cast the runtime type to an opaque void*. This is because we're calling
  // into pre-compiled C++ that doesn't know that the dynamically generated
  // RuntimeState* looks like.
  llvm::Value *runtime_state_ptr = codegen->CreatePointerCast(
      codegen.GetState(), codegen.VoidType()->getPointerTo());

  // Call CSVScanner::Init()
  codegen.Call(CSVScannerProxy::Init,
               {scanner_ptr, exec_ctx_ptr, file_path, output_col_types,
                codegen.Const32(num_cols), consumer_func, runtime_state_ptr,
                codegen.Const8(scan_.GetDelimiterChar()),
                codegen.Const8(scan_.GetQuoteChar()),
                codegen.Const8(scan_.GetEscapeChar())});
}

namespace {

class CSVColumnAccess : public RowBatch::AttributeAccess {
 public:
  CSVColumnAccess(const planner::AttributeInfo *ai, llvm::Value *csv_columns,
                  std::string null_str, llvm::Value *runtime_null_str)
      : ai_(ai),
        csv_columns_(csv_columns),
        null_str_(std::move(null_str)),
        runtime_null_(runtime_null_str) {}

  llvm::Value *Columns() const { return csv_columns_; }

  uint32_t ColumnIndex() const { return ai_->attribute_id; }

  bool IsNullable() const { return ai_->type.nullable; }

  const type::SqlType &SqlType() const { return ai_->type.GetSqlType(); }

  llvm::Value *IsNull(CodeGen &codegen, llvm::Value *data_ptr,
                      llvm::Value *data_len) const {
    uint32_t null_str_len = static_cast<uint32_t>(null_str_.length());

    // Is the length of the column value the same as the NULL string?
    llvm::Value *eq_len =
        codegen->CreateICmpEQ(data_len, codegen.Const32(null_str_len));

    // If the null string is empty, generate simple comparison
    if (null_str_len == 0) {
      return eq_len;
    }

    llvm::Value *cmp_res;
    lang::If check_null{codegen, eq_len};
    {
      // Do a memcmp against the NULL string
      cmp_res = codegen.Memcmp(data_ptr, runtime_null_,
                               codegen.Const64(null_str_.length()));
      cmp_res = codegen->CreateICmpEQ(cmp_res, codegen.Const32(0));
    }
    check_null.EndIf();
    return check_null.BuildPHI(cmp_res, codegen.ConstBool(false));
  }

  Value LoadValueIgnoreNull(CodeGen &codegen, llvm::Value *type,
                            llvm::Value *data_ptr,
                            llvm::Value *data_len) const {
    auto *input_func = SqlType().GetInputFunction(codegen, ai_->type);
    auto *raw_val = codegen.CallFunc(input_func, {type, data_ptr, data_len});
    if (SqlType().IsVariableLength()) {
      // StrWithLen
      llvm::Value *str_ptr = codegen->CreateExtractValue(raw_val, 0);
      llvm::Value *str_len = codegen->CreateExtractValue(raw_val, 1);
      return codegen::Value{ai_->type, str_ptr, str_len,
                            codegen.ConstBool(false)};
    } else {
      return codegen::Value{ai_->type, raw_val, nullptr,
                            codegen.ConstBool(false)};
    }
  }

  Value Access(CodeGen &codegen, UNUSED_ATTRIBUTE RowBatch::Row &row) override {
    // Load the type, data pointer and length values for the column
    auto *type = codegen->CreateConstInBoundsGEP2_32(
        CSVScannerColumnProxy::GetType(codegen), Columns(), ColumnIndex(), 0);
    auto *data_ptr = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
        CSVScannerColumnProxy::GetType(codegen), Columns(), ColumnIndex(), 1));
    auto *data_len = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
        CSVScannerColumnProxy::GetType(codegen), Columns(), ColumnIndex(), 2));

    // If the valid isn't NULLable, avoid the null check here
    if (!IsNullable()) {
      return LoadValueIgnoreNull(codegen, type, data_ptr, data_len);
    }

    // If the value isn't actually null, try to parse it
    codegen::Value valid_val, null_val;
    lang::If is_null{codegen,
                     codegen->CreateNot(IsNull(codegen, data_ptr, data_len))};
    {
      // Load valid
      valid_val = LoadValueIgnoreNull(codegen, type, data_ptr, data_len);
    }
    is_null.ElseBlock();
    {
      // Default null
      null_val = SqlType().GetNullValue(codegen);
    }
    is_null.EndIf();

    // Return
    return is_null.BuildPHI(valid_val, null_val);
  }

 private:
  const planner::AttributeInfo *ai_;
  llvm::Value *csv_columns_;
  const std::string null_str_;
  llvm::Value *runtime_null_;
};

}  // namespace

void CSVScanTranslator::DefineAuxiliaryFunctions() {
  CodeGen &codegen = GetCodeGen();
  CompilationContext &cc = GetCompilationContext();

  // Define consumer function here
  std::vector<FunctionDeclaration::ArgumentInfo> arg_types = {
      {"runtimeState",
       cc.GetRuntimeState().FinalizeType(codegen)->getPointerTo()}};
  FunctionDeclaration decl{codegen.GetCodeContext(), "consumer",
                           FunctionDeclaration::Visibility::Internal,
                           codegen.VoidType(), arg_types};
  FunctionBuilder scan_consumer{codegen.GetCodeContext(), decl};
  {
    ConsumerContext ctx{cc, GetPipeline()};

    Vector v{nullptr, 1, nullptr};
    RowBatch one{GetCompilationContext(), codegen.Const32(0),
                 codegen.Const32(1), v, false};

    // Load the pointer to the columns view
    llvm::Value *cols = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
        CSVScannerProxy::GetType(codegen), LoadStatePtr(scanner_id_), 0, 1));

    llvm::Value *null_str = codegen.ConstString(scan_.GetNullString(), "null");

    // Add accessors for all columns into the row batch
    std::vector<CSVColumnAccess> column_accessors;
    for (uint32_t i = 0; i < output_attributes_.size(); i++) {
      column_accessors.emplace_back(output_attributes_[i], cols,
                                    scan_.GetNullString(), null_str);
    }
    for (uint32_t i = 0; i < output_attributes_.size(); i++) {
      one.AddAttribute(output_attributes_[i], &column_accessors[i]);
    }

    // Push the row through the pipeline
    RowBatch::Row row{one, nullptr, nullptr};
    ctx.Consume(row);

    // Done
    scan_consumer.ReturnAndFinish();
  }

  // The consumer function has been generated. Get a pointer to it now.
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
  return StringUtil::Format(
      "CSVScan(file: '%s', delimiter: '%c', quote: '%c', escape: '%c')",
      scan_.GetFileName().c_str(), scan_.GetDelimiterChar(),
      scan_.GetQuoteChar(), scan_.GetEscapeChar());
}

}  // namespace codegen
}  // namespace peloton