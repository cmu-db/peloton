//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// iterator.h
//
// Identification: src/include/common/iterator.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

namespace peloton {

//===--------------------------------------------------------------------===//
// Iterator Interface
//===--------------------------------------------------------------------===//

template <class T>
class Iterator {
 public:
  virtual bool Next(T &out) = 0;

  virtual bool HasNext() = 0;

  virtual ~Iterator() {}
};

}  // namespace peloton
