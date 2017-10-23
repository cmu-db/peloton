//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timer_set.h
//
// Identification: src/codegen/timer_set.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/proxy/timer_set_proxy.h"

// Accessor class to TimerSet
namespace peloton {
namespace codegen {

class TimerSet {
 public:
  TimerSet(llvm::Value *timer_set) : timer_set_ptr_(timer_set) {}

  void Init(CodeGen &codegen) {
    codegen.Call(TimerSetProxy::Init, {timer_set_ptr_});
  }

  void Destroy(CodeGen &codegen) {
    codegen.Call(TimerSetProxy::Destroy, {timer_set_ptr_});
  }

  void StartTimer(int timer_id, CodeGen &codegen) {
    codegen.Call(TimerSetProxy::Start,
                 {timer_set_ptr_, codegen.Const32(timer_id)});
  }

  void StopTimer(int timer_id, CodeGen &codegen) {
    codegen.Call(TimerSetProxy::Stop,
                 {timer_set_ptr_, codegen.Const32(timer_id)});
  }

  llvm::Value *GetDuration(int timer_id, CodeGen &codegen) {
    return codegen.Call(TimerSetProxy::GetDuration,
                        {timer_set_ptr_, codegen.Const32(timer_id)});
  }

  void PrintDuration(int timer_id, const std::string &name, CodeGen &codegen) {
    llvm::Value *duration =
        codegen.Call(TimerSetProxy::GetDuration,
                     {timer_set_ptr_, codegen.Const32(timer_id)});
    codegen.CallPrintf(name + ": %0.0f ms\n", {duration});
  }

 private:
  llvm::Value *timer_set_ptr_;
};

}  // namespace codegen
}  // namespace peloton