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

namespace peloton {

//===--------------------------------------------------------------------===//
// Iterator Interface
//===--------------------------------------------------------------------===//

template<class T>
class Iterator {
public:
	virtual bool Next(T &out) = 0;

	virtual bool HasNext() = 0;

	virtual ~Iterator() {}
};

} // End peloton namespace


