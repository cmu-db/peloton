//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_message.h
//
// Identification: src/backend/mfabric/abstract_message.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace mfabric {

class AbstractSerializer {
 public:

  /** @brief SerializeMessage function to be overridden by derived class. */
  virtual int SerializeMessage() = 0;

  /** @brief DeserializeMessage function to be overridden by derived class. */
  virtual int DeserializeMessage() = 0;

}  // namespace mfabric
}  // namespace peloton
