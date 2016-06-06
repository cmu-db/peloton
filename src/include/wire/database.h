//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database.h
//
// Identification: src/wire/database.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>
#include <tuple>

#include "common/logger.h"
#include "wire/wire.h"

namespace peloton {
namespace wire {

typedef std::pair<std::vector<unsigned char>, std::vector<unsigned char>> ResType;

// fieldinfotype: field name, oid (data type), size
typedef std::tuple<std::string, int, int> FieldInfoType;

#define WIRE_INTEGER 1
#define WIRE_TEXT 2
#define WIRE_FLOAT 3
#define WIRE_NULL 4

class DataBase {
public:
  DataBase() { }

  virtual ~DataBase() { }
};

}  // End wire namespace
}  // End peloton namespace

