/*-------------------------------------------------------------------------
*
* volatile_backend.h
* file description
*
* Copyright(c) 2015, CMU
*
* /n-store/src/storage/volatile_backend.h
*
*-------------------------------------------------------------------------
*/

#pragma once

#include "storage/backend.h"

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// VM Backend
//===--------------------------------------------------------------------===//


class VMBackend : public Backend {

public:
	virtual ~VMBackend(){};

	void* Allocate(size_t size) {
		return ::operator new(size);
	}

	void Free(void* ptr) {
		::operator delete(ptr);
	}

	void Sync(void* ptr __attribute__((unused)))  {
		// does nothing
	}

	std::string GetBackendType() const{
		return GetBackendTypeName(BACKEND_TYPE_VM);
	}

};

} // End storage namespace
} // End nstore namespace

