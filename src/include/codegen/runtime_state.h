//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// runtime_state.h
//
// Identification: src/include/codegen/runtime_state.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

#include <string>
#include <vector>

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// This class captures all the state that a query plans' operators need. During
// the compilation process, we pass this class around so that translators can
// register operator-specific state. An example of state would be a hash-join
// which requires a hash-table. State can be either global or local (i.e., on
// the stack).
//
// In the end, all global state is combined into a dynamic struct type. This
// struct is the only function argument to each of the three component functions
// for the query. Operators must initialize any global state in the init()
// function and must clean up and global state in the tearDown() function.
//
// Local state is guaranteed to be allocated once at the start of the plan()
// function. All access to _any_ query state must go through this class.
//
// For note, the reason we construct a single struct type as the only function
// argument to generated query functions is:
//
// (1) The LLVM API is verbose. Clumping all the state into a single struct
//     makes constructing the function type much easier.
// (2) We don't want to worry about potentially reaching some system-specific
//     limit on the number of arguments a function can accept.
//
//===----------------------------------------------------------------------===//
class RuntimeState {
 public:
  // An identifier
  typedef uint32_t StateID;

  // Constructor
  RuntimeState();

  // Register a parameter with the given name and type in this state. Callers
  // can specify whether the state is local (i.e., on the stack) or global.
  RuntimeState::StateID RegisterState(std::string name, llvm::Type *type);

  // Get the pointer to the given state information with the given ID
  llvm::Value *LoadStatePtr(CodeGen &codegen,
                            RuntimeState::StateID state_id) const;

  // Get the actual value of the state information with the given ID
  llvm::Value *LoadStateValue(CodeGen &codegen,
                              RuntimeState::StateID state_id) const;

  // Construct the equivalent LLVM type that represents this runtime state
  llvm::Type *FinalizeType(CodeGen &codegen);

 private:
  // Little struct to track information of elements in the runtime state
  struct StateInfo {
    // The name and type of the variable
    std::string name;
    llvm::Type *type;

    // This is the index into the runtime state type that this state is stored
    uint32_t index;

    // If the state is local, this is the current value of the state
    llvm::Value *val;
  };

 private:
  // All the states we've allocated
  std::vector<RuntimeState::StateInfo> state_slots_;

  // The LLVM type of this runtime state. This type is cached for re-use.
  llvm::Type *constructed_type_;
};

}  // namespace codegen
}  // namespace peloton