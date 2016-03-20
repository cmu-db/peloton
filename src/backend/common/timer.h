//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// timer.h
//
// Identification: src/backend/common/timer.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// timer.cpp
//
// Identification: src/backend/common/timer.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <chrono>

//===--------------------------------------------------------------------===//
// Timer
//===--------------------------------------------------------------------===//

template<typename ResolutionRatio>
class Timer{

 public:
  Timer() : elapsed(0) {}

  void Start(){
    begin = std::chrono::high_resolution_clock::now();
  }

  void Stop() {
    end = std::chrono::high_resolution_clock::now();

    double duration =
        std::chrono::duration_cast<
        std::chrono::duration<double, ResolutionRatio> >(end - begin).count();

    elapsed += duration;
  }

  void Reset() { elapsed = 0; }

  // Get Elapsed duration
  double GetDuration() const {
    return elapsed;
  }

 private:

  // Start
  std::chrono::time_point<std::chrono::high_resolution_clock> begin;

  // End
  std::chrono::time_point<std::chrono::high_resolution_clock> end;

  // Elapsed time (with desired resolution)
  double elapsed;
};
