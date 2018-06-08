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
#include "codegen/vector.h"
#include "planner/csv_scan_plan.h"

namespace peloton {
namespace codegen {

CSVScanTranslator::CSVScanTranslator(const planner::CSVScanPlan &scan,
                                     CompilationContext &context,
                                     Pipeline &pipeline)
    : OperatorTranslator(scan, context, pipeline) {
  // Register the CSV scanner instance
  auto &query_state = context.GetQueryState();
  scanner_id_ = query_state.RegisterState(
      "csvScanner", CSVScannerProxy::GetType(GetCodeGen()));

  // Load information about the attributes output by the scan plan
  scan.GetAttributes(output_attributes_);
}

void CSVScanTranslator::InitializeQueryState() {
  auto &codegen = GetCodeGen();

  auto &scan = GetPlanAs<planner::CSVScanPlan>();

  // Arguments
  llvm::Value *scanner_ptr = LoadStatePtr(scanner_id_);
  llvm::Value *exec_ctx_ptr = GetExecutorContextPtr();
  llvm::Value *file_path = codegen.ConstString(scan.GetFileName(), "filePath");

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
  llvm::Value *query_state_ptr =
      codegen->CreatePointerCast(codegen.GetState(), codegen.VoidPtrType());

  // Call CSVScanner::Init()
  codegen.Call(CSVScannerProxy::Init,
               {scanner_ptr, exec_ctx_ptr, file_path, output_col_types,
                codegen.Const32(num_cols), consumer_func, query_state_ptr,
                codegen.Const8(scan.GetDelimiterChar()),
                codegen.Const8(scan.GetQuoteChar()),
                codegen.Const8(scan.GetEscapeChar())});
}

namespace {

/**
 * This is a deferred column access class configured to load the contents of a
 * given column.
 */
class CSVColumnAccess : public RowBatch::AttributeAccess {
 public:
  CSVColumnAccess(const planner::AttributeInfo *ai, llvm::Value *csv_columns,
                  std::string null_str, llvm::Value *runtime_null_str)
      : ai_(ai),
        csv_columns_(csv_columns),
        null_str_(std::move(null_str)),
        runtime_null_(runtime_null_str) {}

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  llvm::Value *Columns() const { return csv_columns_; }

  uint32_t ColumnIndex() const { return ai_->attribute_id; }

  bool IsNullable() const { return ai_->type.nullable; }

  const type::SqlType &SqlType() const { return ai_->type.GetSqlType(); }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Logic
  ///
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Check if a column's value is considered NULL. Given a pointer to the
   * column's string value, and the length of the string, this function will
   * check if the column's value is determined to be NULL. This is done by
   * comparing the column's contents with the NULL string configured in the
   * CSV scan plan (i.e., provided by the user).
   *
   * @param codegen The codegen instance
   * @param data_ptr A pointer to the column's string value
   * @param data_len The length of the column's string value
   * @return True if the column is equivalent to the NULL string. False
   * otherwise.
   */
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

  /**
   * Load the value of the given column with the given type, ignoring a null
   * check.
   *
   * @param codegen The codegen instance
   * @param type The SQL type of the column
   * @param data_ptr A pointer to the column's string representation
   * @param data_len The length of the column's string representation
   * @return The parsed value
   */
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

  /**
   * Access this column in the given row. In reality, this function pulls out
   * the column information from the CSVScanner state and loads/parses the
   * column's value.
   *
   * @param codegen The codegen instance
   * @param row The row. This isn't used.
   * @return The value of the column
   */
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
  // Information about the attribute
  const planner::AttributeInfo *ai_;

  // A pointer to the array of columns
  llvm::Value *csv_columns_;

  // The NULL string configured for the CSV scan
  const std::string null_str_;

  // The runtime NULL string (a constant in LLVM)
  llvm::Value *runtime_null_;
};

}  // namespace

// We define the callback/consumer function for CSV parsing here
void CSVScanTranslator::DefineAuxiliaryFunctions() {
  CodeGen &codegen = GetCodeGen();
  CompilationContext &cc = GetCompilationContext();

  auto &scan = GetPlanAs<planner::CSVScanPlan>();

  // Define consumer function here
  std::vector<FunctionDeclaration::ArgumentInfo> arg_types = {
      {"queryState", cc.GetQueryState().GetType()->getPointerTo()}};
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

    llvm::Value *null_str = codegen.ConstString(scan.GetNullString(), "null");

    // Add accessors for all columns into the row batch
    std::vector<CSVColumnAccess> column_accessors;
    for (uint32_t i = 0; i < output_attributes_.size(); i++) {
      column_accessors.emplace_back(output_attributes_[i], cols,
                                    scan.GetNullString(), null_str);
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

void CSVScanTranslator::TearDownQueryState() {
  auto *scanner_ptr = LoadStatePtr(scanner_id_);
  GetCodeGen().Call(CSVScannerProxy::Destroy, {scanner_ptr});
}

}  // namespace codegen
}  // namespace peloton
