/*-------------------------------------------------------------------------
 *
 * outfuncs.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/backend/outfuncs.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "postgres.h"

#include <string>

//===--------------------------------------------------------------------===//
// Outfuncs
//===--------------------------------------------------------------------===//

namespace nstore {
namespace backend {

/*
 * NodeToString -
 *     returns the ascii representation of the Node as a palloc'd string
 */
std::string NodeToString(const void *obj);


} // namespace backend
} // namespace nstore
