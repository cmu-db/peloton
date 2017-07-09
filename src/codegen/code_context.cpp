//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// code_context.cpp
//
// Identification: src/codegen/code_context.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/code_context.h"

#include "llvm/ExecutionEngine/MCJIT.h"
#include "llvm/ExecutionEngine/SectionMemoryManager.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/raw_os_ostream.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/Scalar.h"
#if LLVM_VERSION_GE(3, 9)
#include "llvm/Transforms/Scalar/GVN.h"
#endif

#include "common/logger.h"

namespace peloton {
namespace codegen {

static std::atomic<uint64_t> kIdCounter{0};

//===----------------------------------------------------------------------===//
// Constructor
//===----------------------------------------------------------------------===//
CodeContext::CodeContext()
    : id_(kIdCounter++),
      context_(new llvm::LLVMContext()),
      module_(new llvm::Module("_" + std::to_string(id_) + "_plan", *context_)),
      builder_(*context_),
      func_(nullptr),
      opt_pass_manager_(module_),
      jit_engine_(nullptr) {
  // Initialize JIT stuff
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();

  // Create the JIT engine.  We transfer ownership of the module to the engine,
  // but we retain a reference to it here so that we can lookup method
  // references etc.
  std::unique_ptr<llvm::Module> m{module_};
  module_ = m.get();
  jit_engine_.reset(llvm::EngineBuilder(std::move(m))
                        .setEngineKind(llvm::EngineKind::JIT)
                        .setMCJITMemoryManager(
                             llvm::make_unique<llvm::SectionMemoryManager>())
                        .setMCPU(llvm::sys::getHostCPUName())
                        .setErrorStr(&err_str_)
                        .create());
  PL_ASSERT(jit_engine_ != nullptr);

#if LLVM_VERSION_EQ(3, 6)
  // LLVM 3.6
  // Set the layout of the module based on what the engine is
  const llvm::DataLayout &data_layout = *jit_engine_->getDataLayout();
  module_->setDataLayout(data_layout);
#endif

  // The set of optimization passes we include
  opt_pass_manager_.add(llvm::createInstructionCombiningPass());
  opt_pass_manager_.add(llvm::createReassociatePass());
  opt_pass_manager_.add(llvm::createGVNPass());
  opt_pass_manager_.add(llvm::createCFGSimplificationPass());
  opt_pass_manager_.add(llvm::createAggressiveDCEPass());
  opt_pass_manager_.add(llvm::createCFGSimplificationPass());

  // Setup the common types we need once
  bool_type_ = llvm::Type::getInt1Ty(*context_);
  int8_type_ = llvm::Type::getInt8Ty(*context_);
  int16_type_ = llvm::Type::getInt16Ty(*context_);
  int32_type_ = llvm::Type::getInt32Ty(*context_);
  int64_type_ = llvm::Type::getInt64Ty(*context_);
  double_type_ = llvm::Type::getDoubleTy(*context_);
  void_type_ = llvm::Type::getVoidTy(*context_);
  char_ptr_type_ = llvm::Type::getInt8PtrTy(*context_);
}

CodeContext::~CodeContext() {
  // We need this empty constructor because we declared a std::unique_ptr<>
  // on llvm::ExecutionEngine and llvm::LLVMContext that are forward-declared
  // in the header file. To make this compile, this destructor needs to exist.
}

// Return the pointer to the LLVM function in this module given its name
llvm::Function *CodeContext::GetFunction(const std::string &fn_name) const {
  return module_->getFunction(fn_name);
}

// Get a pointer to the JITed function of the given type
void *CodeContext::GetFunctionPointer(llvm::Function *fn) const {
  return jit_engine_->getPointerToFunction(fn);
}

const llvm::DataLayout &CodeContext::GetDataLayout() const {
  return module_->getDataLayout();
}

//===----------------------------------------------------------------------===//
// JIT the code contained within after optimizing it
//===----------------------------------------------------------------------===//
bool CodeContext::Compile() {
  // Verify the module is okay
  llvm::raw_ostream &errors = llvm::errs();
  if (llvm::verifyModule(*module_, &errors)) {
    // There is an error in the module that failed compilation.
    // Dump the crappy IR to the log ...
    LOG_ERROR("ERROR IN MODULE:\n%s\n", GetIR().c_str());
    return false;
  }

  // Run each of our optimization passes over the functions in this module
  opt_pass_manager_.doInitialization();
  for (auto fn = module_->begin(), end = module_->end(); fn != end; fn++) {
    opt_pass_manager_.run(*fn);
  }
  opt_pass_manager_.doFinalization();

  // Finalize the object, this is where the JIT happens
  jit_engine_->finalizeObject();

  // Log the module
  LOG_TRACE("%s\n", GetIR().c_str());

  // All is well
  return true;
}

void CodeContext::DumpContents() const {
  std::error_code error_code;

  // First, write out the LLVM IR file
  {
    std::string ll_fname = "dump_plan_" + std::to_string(id_) + ".ll";
    llvm::raw_fd_ostream ll_ostream{ll_fname, error_code, llvm::sys::fs::F_RW};
    module_->print(ll_ostream, nullptr, false);
  }

  // Now, write out the raw ASM
  {
    std::string asm_fname = "dump_plan_" + std::to_string(id_) + ".s";
    llvm::raw_fd_ostream asm_ostream{asm_fname, error_code,
                                     llvm::sys::fs::F_RW};
    llvm::legacy::PassManager pass_manager;
    auto *target_machine = jit_engine_->getTargetMachine();
    target_machine->Options.MCOptions.AsmVerbose = true;
    target_machine->addPassesToEmitFile(pass_manager, asm_ostream,
                                        llvm::TargetMachine::CGFT_AssemblyFile);
    pass_manager.run(*module_);
    target_machine->Options.MCOptions.AsmVerbose = false;
  }
}

// Get the textual form of the IR in this context
std::string CodeContext::GetIR() const {
  std::string module_str;
  llvm::raw_string_ostream ostream{module_str};
  module_->print(ostream, nullptr, false);
  return module_str;
}

}  // namespace codegen
}  // namespace peloton