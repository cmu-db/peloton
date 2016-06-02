//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// generator.h
//
// Identification: src/backend/common/generator.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <random>

//===--------------------------------------------------------------------===//
// Generator
//===--------------------------------------------------------------------===//

class UniformGenerator {

 public:
  UniformGenerator() {
    unif = std::uniform_real_distribution<double>(0, 1);
  }

  UniformGenerator(double lower_bound, double upper_bound){
    unif = std::uniform_real_distribution<double>(lower_bound, upper_bound);
  }

  double GetSample(){
    return unif(rng);
  }

 private:

  // Random number generator
  std::mt19937_64 rng;

  // Distribution
  std::uniform_real_distribution<double> unif;

};
