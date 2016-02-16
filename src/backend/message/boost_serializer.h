//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// boost_serializer.h
//
// Identification: src/backend/message/boost_serializer.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/message/abstract_serializer.h"

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

namespace peloton {
namespace message {

  // TODO: If we use this serialization method, we have to implement the
  // the serialize() method in the classes whose objects we wish to serialze
  // This is probably a problem, need to zero in on a library which
  // doesn't have this overhead



}  // namespace message
}  // namespace peloton
