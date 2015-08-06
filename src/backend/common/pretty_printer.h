//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// pretty_printer.h
//
// Identification: src/backend/common/pretty_printer.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/storage/tuple.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Pretty printer
//===--------------------------------------------------------------------===//

class PrettyPrinter {
  PrettyPrinter(const PrettyPrinter &) = delete;
  PrettyPrinter &operator=(const PrettyPrinter &) = delete;

 public:
  // pretty print tuple pointers
  static void PrintTuple(storage::Tuple *source) {
    if (source != nullptr)
      std::cout << (*source);
    else
      std::cout << "[ nullptr tuple ]\n";
  }

  // pretty print tile pointers
  static void PrintTile(storage::Tile *source) {
    if (source != nullptr)
      std::cout << (*source);
    else
      std::cout << "[ nullptr tile ]\n";
  }

  // pretty print tile group pointers
  static void PrintTileGroup(storage::TileGroup *source) {
    if (source != nullptr)
      std::cout << (*source);
    else
      std::cout << "[ nullptr tile group ]\n";
  }
};

}  // End peloton namespace
