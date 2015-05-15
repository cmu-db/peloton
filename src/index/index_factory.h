/*-------------------------------------------------------------------------
 *
 * index_factory.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/index/index_factory.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <string>
#include <vector>

#include "index/index.h"

namespace nstore {
namespace index {

//===--------------------------------------------------------------------===//
// Index Factory
//===--------------------------------------------------------------------===//

class TableIndexFactory {
public:
    static TableIndex *getInstance(const TableIndexScheme &scheme);
};

} // End index namespace
} // End nstore namespace
