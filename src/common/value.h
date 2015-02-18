/*-------------------------------------------------------------------------
 *
 * value.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/value.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

namespace nstore {

class Value{

public:
	Value CastAs(ValueType type) const{
		Value ret;

		return ret;
	}

	void CopyTo(void *location, const bool is_inlined, const int32_t max_length) const{

	}

};


} // End nstore namespace


