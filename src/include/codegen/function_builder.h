//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_builder.h
//
// Identification: src/include/codegen/function_builder.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/code_context.h"

namespace peloton {
namespace codegen {

/**
 * A function declaration defines the signature of the function. A declaration
 * includes the function's name, return type, and all arguments. Additionally,
 * users can decide the visibility of the function, and define attributes on the
 * arguments of the function.
 */
class FunctionDeclaration {
 public:
  /**
   * The visibility of the function
   */
  enum class Visibility : uint8_t {
    External = 0,
    ExternalAvailable = 1,
    Internal = 2,
  };

  /**
   * Captures information about each function argument
   */
  struct ArgumentInfo {
    std::string name;
    llvm::Type *type;
    ArgumentInfo(std::string _name, llvm::Type *_type)
        : name(std::move(_name)), type(_type) {}
  };

  /**
   * Constructor.
   *
   * @param cc The context where this function's IR exists
   * @param name The name of the function
   * @param visibility The visibility of the function
   * @param ret_type The return type of the function
   * @param args The list of arguments (name and type) the function takes
   */
  FunctionDeclaration(CodeContext &cc, const std::string &name,
                      Visibility visibility, llvm::Type *ret_type,
                      const std::vector<ArgumentInfo> &args);

  /**
   * Get the raw LLVM function declaration we generated
   *
   * @return The generated function declaration
   */
  llvm::Function *GetDeclaredFunction() const { return func_decl_; }

  /**
   * Factory function to create a function declaration
   *
   * @param cc The context where this function's IR exists
   * @param name The name of the function
   * @param visibility The visibility of the function
   * @param ret_type The return type of the function
   * @param args The list of arguments (name and type) the function takes
   * @return A function declaration with the provided interface
   */
  static FunctionDeclaration MakeDeclaration(
      CodeContext &cc, const std::string &name, Visibility visibility,
      llvm::Type *ret_type, const std::vector<ArgumentInfo> &args);

 private:
  // The name of the function
  std::string name_;

  // The visibility of this function
  Visibility visibility_;

  // The return type of the function
  llvm::Type *ret_type_;

  // The function arguments
  std::vector<ArgumentInfo> args_info_;

  // The declaration
  llvm::Function *func_decl_;
};

/**
 * A handy class to build the definition of a function. This builder either
 * creates a new function or begins the definition of previously declared
 * function using the provided declaration. After instantiation, code-generation
 * shifts to the entry point of the function allowing users to defines its
 * logic. Arguments are accessible through the GetArgumentBy*(...) methods.
 * The user completes the construction of the function by calling Finish(...),
 * after which the function is fully defined.

 * FunctionBuilders can safely nest. This enables construction of one function
 * while in the middle of construction of a different function. However, to do
 * this safely, the functions must be "finished" in reverse order of nesting.
 */
class FunctionBuilder {
  friend class CodeGen;

 public:
  /// Constructors
  FunctionBuilder(CodeContext &cc, const FunctionDeclaration &declaration);
  FunctionBuilder(CodeContext &cc, std::string name, llvm::Type *ret_type,
                  const std::vector<FunctionDeclaration::ArgumentInfo> &args);

  /// Destructor
  ~FunctionBuilder();

  /// This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(FunctionBuilder);

  /// Access to function arguments by argument name or index position
  llvm::Value *GetArgumentByName(std::string name);
  llvm::Value *GetArgumentByPosition(uint32_t index);

  ///
  llvm::BasicBlock *GetExitBlock();

  /// Finish the current function
  void ReturnAndFinish(llvm::Value *res = nullptr);

  /// Return the function we created
  llvm::Function *GetFunction() const { return func_; }

  llvm::Value *GetOrCacheVariable(
      const std::string &name, const std::function<llvm::Value *()> &load_func);

 private:
  // Get the first basic block in this function
  llvm::BasicBlock *GetEntryBlock() const { return entry_bb_; }

  // Get the basic block where the function throws an overflow exception
  llvm::BasicBlock *GetOverflowBB();

  // Get the basic block where the function throws a divide by zero exception
  llvm::BasicBlock *GetDivideByZeroBB();

 private:
  // Private constructor taking in a fully constructed function declaration
  FunctionBuilder(CodeContext &cc, llvm::Function *func_decl);

 private:
  // Has this function finished construction
  bool finished_;

  // The context into which the generated function will go into
  CodeContext &code_context_;

  // The function that was being constructed at the time our new function is
  // created. We keep this here to restore the state of code generation after
  // our function has finished being constructed.
  FunctionBuilder *previous_function_;
  llvm::BasicBlock *previous_insert_point_;

  // The function we create
  llvm::Function *func_;
  // The entry/first BB in the function
  llvm::BasicBlock *entry_bb_;
  // The arithmetic overflow error block
  llvm::BasicBlock *overflow_bb_;
  // The divide-by-zero error block
  llvm::BasicBlock *divide_by_zero_bb_;
  // The return block
  llvm::BasicBlock *return_bb_;

  // Cached variables
  std::unordered_map<std::string, llvm::Value *> cached_vars_;
};

}  // namespace codegen
}  // namespace peloton