//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_serializer.h
//
// Identification: src/backend/networking/abstract_serializer.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace networking {

class AbstractSerializer {
 public:

  /** @brief SerializeMessage function to be overridden by derived class. */
  virtual int SerializeMessage() = 0;

  /** @brief DeserializeMessage function to be overridden by derived class. */
  virtual int DeserializeMessage() = 0;

  virtual ~AbstractSerializer(){};
};

}  // namespace networking
}  // namespace peloton
