/*-------------------------------------------------------------------------
*
* iterator.h
* file description
*
* Copyright(c) 2015, CMU
*
* /n-store/src/common/iterator.h
*
*-------------------------------------------------------------------------
*/


#pragma once

#include "storage/tuple.h"

namespace nstore {

/**
 * Interface for iterators returning tuples.
 */

class Iterator {
public:

    virtual bool Next(storage::Tuple &out) = 0;

    virtual ~Iterator() {}

};

} // End nstore namespace


