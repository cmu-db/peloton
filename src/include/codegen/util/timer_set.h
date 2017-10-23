//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timer_set.h
//
// Identification: src/codegen/util/timer_set.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string.h>
#include <unordered_map>
#include "common/logger.h"
#include "common/timer.h"

namespace peloton {
namespace codegen {
namespace util {

// A set of common/timer. This class is used in the codegen code to measure
// time.
class TimerSet {
 public:
  using kRatio = std::ratio<1, 1000>;

  void Init() { timers = new std::unordered_map<uint32_t, Timer<kRatio>>(); }

  void Destroy() { delete timers; }

  void Start(uint32_t timer_id) {
    if (timers->find(timer_id) == timers->end()) {
      // Create a new timer
      (*timers)[timer_id] = Timer<kRatio>();
    }
    timers->at(timer_id).Start();
  }

  void Stop(uint32_t timer_id) {
    auto &timer = timers->at(timer_id);
    timer.Stop();
  }

  double GetDuration(uint32_t timer_id) {
    return timers->at(timer_id).GetDuration();
  }

  std::string GetInfo() {
    std::ostringstream os;
    for (auto &pair : *timers) {
      os << pair.first << ": " << pair.second.GetDuration() << " ms\n";
    }
    return os.str();
  }

 private:
  std::unordered_map<uint32_t, Timer<kRatio>> *timers;
};

}  // namespace util
}  // namespace codegen
}  // namespace peloton
