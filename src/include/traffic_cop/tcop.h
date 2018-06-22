//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// t=cop.h
//
// Identification: src/include/traffic_cop/tcop.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

namespace peloton {
namespace tcop {
struct JobHandle {
  bool is_queueing_;
};


} // namespace tcop
} // namespace peloton