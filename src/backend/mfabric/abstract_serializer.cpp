//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_message.cpp
//
// Identification: src/backend/mfabric/abstract_message.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/mfabric/abstract_serializer.h"

namespace peloton {
namespace mfabric {

/**
 * @brief Creates and returns socket.
 *
 * @return True or false based on serialization
 */
int AbstractSerializer::SerializeMessage() {
  // Base class implementation if required

}

/**
 * @brief Deserializes message
 *
 * @return True or false based on serialization
 */
int AbstractSerializer::DeserializeMessage() {
  // Base class implementation if required

}

}  // namespace mfabric
}  // namespace peloton
