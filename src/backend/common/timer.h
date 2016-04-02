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

#include <iostream>
#include <chrono>

//===--------------------------------------------------------------------===//
// Timer
//===--------------------------------------------------------------------===//

typedef std::chrono::high_resolution_clock clock_;

typedef std::chrono::time_point<clock_> time_point_;

template<typename ResolutionRatio = std::ratio<1> >
class Timer{

 public:
  Timer() : elapsed(0) {}

  void Start(){
    begin = clock_::now();
  }

  void Stop() {
    end = clock_::now();

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
  time_point_ begin;

  // End
  time_point_ end;

  // Elapsed time (with desired resolution)
  double elapsed;
};
