//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// runtime_state.h
//
// Identification: src/include/codegen/runtime_state.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

#include <string>
#include <vector>

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// RuntimeState captures all the state that a query plans' operator translators
// need at _query_ runtime. As we create the translator tree for the query, we
// pass around a single instance of this RuntimeState. Translators Allocate()
// to describe the variables they'll need at runtime (e.g., a hash-join will
// need a hash-table). When done, RuntimeState is a struct type whose contents
// are defined by the query. This structure becomes the only argument to each
// of the init(), plan() and tearDown() functions in the generated query.
//
// Components of RuntimeState are initialized in the query's init(...) function,
// and cleaned up in the query's tearDown(...) function.
//
// In the final code, all functions will have a single argument whose type
// is a struct that contains all the states that each operator will need. We do
// this for two reasons:
//
// (1) The LLVM API is verbose, clumping all the state into a single class makes
//     constructing the function type much easier.
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

  // Allocate a parameter with the given name and type in this state
  RuntimeState::StateID RegisterState(std::string name, llvm::Type *type,
                                      bool is_on_stack = false);

  // Get the pointer to the given state information with the given ID
  llvm::Value *GetStatePtr(CodeGen &codegen,
                           RuntimeState::StateID state_id) const;

  // Get the actual value of the state information with the given ID
  llvm::Value *GetStateValue(CodeGen &codegen,
                             RuntimeState::StateID state_id) const;

  // Construct the equivalent LLVM type that represents this runtime state
  llvm::Type *ConstructType(CodeGen &codegen);

  void CreateLocalState(CodeGen &codegen);

 private:
  // Little struct to track information of elements in the runtime state
  struct StateInfo {
    // The name and type of the variable
    std::string name;
    llvm::Type *type;

    // Is this state allocated on the stack (local) or as a function parameter
    bool local;

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