//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// global.h
//
// Identification: /src/include/util/global.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#if defined __clang__
#define PELOTON_FALLTHROUGH [[clang::fallthrough]]
#elif defined __GNUC__ && __GNUC__ >= 7
#define PELOTON_FALLTHROUGH [[fallthrough]]
#else
#define PELOTON_FALLTHROUGH
#endif
