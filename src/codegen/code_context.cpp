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
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_os_ostream.h"
#include "llvm/Transforms/Scalar.h"
#if LLVM_VERSION_GE(3, 9)
#include "llvm/Transforms/Scalar/GVN.h"
#endif

#include "common/exception.h"
#include "common/logger.h"

namespace peloton {
namespace codegen {

/// Atomic plan ID counter
static std::atomic<uint64_t> kIdCounter{0};

namespace {
class PelotonMM : public llvm::SectionMemoryManager {
 public:
  explicit PelotonMM(
      const std::unordered_map<std::string, CodeContext::FuncPtr> &symbols)
      : symbols_(symbols) {}

#if LLVM_VERSION_GE(4, 0)
#define RET_TYPE llvm::JITSymbol
#define BUILD_RET_TYPE(addr) \
  (RET_TYPE{(llvm::JITTargetAddress)addr, llvm::JITSymbolFlags::Exported})
#else
#define RET_TYPE llvm::RuntimeDyld::SymbolInfo
#define BUILD_RET_TYPE(addr) \
  (RET_TYPE{(uint64_t)addr, llvm::JITSymbolFlags::Exported})
#endif
  RET_TYPE findSymbol(const std::string &name) override {
    LOG_TRACE("Looking up symbol '%s' ...", name.c_str());
    if (auto *builtin = LookupSymbol(name)) {
      LOG_TRACE("--> Resolved to builtin @ %p", builtin);
      return BUILD_RET_TYPE(builtin);
    }

    LOG_TRACE("--> Not builtin, use fallback resolution ...");
    return llvm::SectionMemoryManager::findSymbol(name);
  }
#undef RET_TYPE
#undef BUILD_RET_TYPE

 private:
  void *LookupSymbol(const std::string &name) const {
    // Check for a builtin with the exact name
    auto symbol_iter = symbols_.find(name);
    if (symbol_iter != symbols_.end()) {
      return symbol_iter->second;
    }

    // Check for a builtin with the leading '_' removed
    if (!name.empty() && name[0] == '_') {
      symbol_iter = symbols_.find(name.substr(1));
      if (symbol_iter != symbols_.end()) {
        return symbol_iter->second;
      }
    }

    // Nothing
    return nullptr;
  }

 private:
  // The code context
  const std::unordered_map<std::string, CodeContext::FuncPtr> &symbols_;
};
}  // anonymous namespace

/// Constructor
CodeContext::CodeContext()
    : id_(kIdCounter++),
      context_(nullptr),
      module_(nullptr),
      builder_(nullptr),
      func_(nullptr),
      udf_func_ptr_(nullptr),
      pass_manager_(nullptr),
      engine_(nullptr) {
  // Initialize JIT stuff
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();

  // Create the context
  context_.reset(new llvm::LLVMContext());

  // Create the module
  module_ = new llvm::Module("_" + std::to_string(id_) + "_plan", *context_);

  // Create the IR builder
  builder_.reset(new llvm::IRBuilder<>(*context_));

  // Create the JIT engine.  We transfer ownership of the module to the engine,
  // but we retain a reference to it here so that we can lookup method
  // references etc.
  std::unique_ptr<llvm::Module> m{module_};
  module_ = m.get();
  engine_.reset(llvm::EngineBuilder(std::move(m))
                    .setEngineKind(llvm::EngineKind::JIT)
                    .setMCJITMemoryManager(
                        llvm::make_unique<PelotonMM>(function_symbols_))
                    .setMCPU(llvm::sys::getHostCPUName())
                    .setErrorStr(&err_str_)
                    .create());
  PL_ASSERT(engine_ != nullptr);

  // The set of optimization passes we include
  pass_manager_.reset(new llvm::legacy::FunctionPassManager(module_));
  pass_manager_->add(llvm::createInstructionCombiningPass());
  pass_manager_->add(llvm::createReassociatePass());
  pass_manager_->add(llvm::createGVNPass());
  pass_manager_->add(llvm::createCFGSimplificationPass());
  pass_manager_->add(llvm::createAggressiveDCEPass());
  pass_manager_->add(llvm::createCFGSimplificationPass());

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

/// Destructor
CodeContext::~CodeContext() {
  // We need this empty constructor because we declared a std::unique_ptr<>
  // on llvm::ExecutionEngine and llvm::LLVMContext that are forward-declared
  // in the header file. To make this compile, this destructor needs to exist.
}

void CodeContext::RegisterFunction(llvm::Function *func) {
  PL_ASSERT(func->getParent() == &GetModule() &&
            "The provided function is part of a different context and module");
  // Insert the function without an implementation
  functions_.emplace_back(func, nullptr);
}

void CodeContext::RegisterExternalFunction(llvm::Function *func_decl,
                                           CodeContext::FuncPtr func_impl) {
  PL_ASSERT(func_decl->isDeclaration() &&
            "The first argument must be a function declaration");
  PL_ASSERT(func_impl != nullptr && "The function pointer cannot be NULL");
  functions_.emplace_back(func_decl, func_impl);

  // Register the builtin symbol by name
  function_symbols_[func_decl->getName()] = func_impl;
}

void CodeContext::RegisterBuiltin(llvm::Function *func_decl,
                                  CodeContext::FuncPtr func_impl) {
  const auto name = func_decl->getName();
  if (LookupBuiltin(name) != nullptr) {
    LOG_DEBUG("Builtin '%s' already registered, skipping ...", name.data());
    return;
  }

  // Sanity check
  PL_ASSERT(func_decl->isDeclaration() &&
            "You cannot provide a function definition for a builtin function");

  // Register the builtin function
  builtins_[name] = func_decl;

  // Register the builtin symbol by name
  function_symbols_[name] = func_impl;
}

/// Optimize and JIT compile all the functions that were created in this context
bool CodeContext::Compile() {
  // Verify the module is okay
  llvm::raw_ostream &errors = llvm::errs();
  if (llvm::verifyModule(*module_, &errors)) {
    // There is an error in the module that failed compilation.
    // Dump the crappy IR to the log ...
    LOG_ERROR("ERROR IN MODULE:\n%s\n", GetIR().c_str());
    return false;
  }

  // Run the optimization passes over each function in this module
  pass_manager_->doInitialization();
  for (auto &func_iter : functions_) {
    pass_manager_->run(*func_iter.first);
  }
  pass_manager_->doFinalization();

  // Functions and module have been optimized, now JIT compile the module
  engine_->finalizeObject();

  // Pull out the compiled function implementations
  for (auto &func_iter : functions_) {
    func_iter.second = engine_->getPointerToFunction(func_iter.first);
  }

  // Log the module
  LOG_TRACE("%s\n", GetIR().c_str());

  // All is well
  return true;
}

/// Get the module's layout
const llvm::DataLayout &CodeContext::GetDataLayout() const {
  return module_->getDataLayout();
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
    auto *target_machine = engine_->getTargetMachine();
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