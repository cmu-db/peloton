//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timer.h
//
// Identification: src/include/common/timer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <chrono>
#include <sstream>

#include "common/printable.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Timer
//===--------------------------------------------------------------------===//

typedef std::chrono::high_resolution_clock clock_;

typedef std::chrono::time_point<clock_> time_point_;

template <typename ResolutionRatio = std::ratio<1> >
class Timer : public peloton::Printable {
 public:
  Timer() : elapsed_(0), total_(0), invocations_(0) {}

  inline void Start() { begin_ = clock_::now(); }

  inline void Stop() {
    end_ = clock_::now();

    double duration =
        std::chrono::duration_cast<
            std::chrono::duration<double, ResolutionRatio> >(end_ - begin_)
            .count();

    elapsed_ += duration;
    total_ += duration;
    invocations_++;
  }

  /**
   * Reset the current elapsed time counter.
   */
  void Reset() { elapsed_ = 0; }

  /**
   * Get elapsed duration
   * @return
   */
  double GetDuration() const { return elapsed_; }

  /**
   * Return the total amount of time measured by
   * this timer after multiple invocations.
   * @return
   */
  double GetTotalDuration() const { return (total_); }

  /**
   * Get number of invocations
   * @return
   */
  int GetInvocations() const { return invocations_; }

  // Get a string representation for debugging
  inline const std::string GetInfo() const {
    std::ostringstream os;
    os << "Timer["
       << "elapsed=" << elapsed_ << ", "
       << "invocations=" << invocations_ << "]";
    return (os.str());
  }

 private:
  // Start
  time_point_ begin_;

  // End
  time_point_ end_;

  // Elapsed time (with desired resolution)
  double elapsed_;

  /**
   * Total amount of time (even after resets)
   */
  double total_;

  // Number of invocations
  int invocations_;
};

}  // namespace peloton
