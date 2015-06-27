/*-------------------------------------------------------------------------
*
* volatile_backend.h
* file description
*
* Copyright(c) 2015, CMU
*
*-------------------------------------------------------------------------
*/

#pragma once

#include "backend/storage/abstract_backend.h"

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// VM Backend
//===--------------------------------------------------------------------===//


class VMBackend : public AbstractBackend {

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
		return BackendTypeToString(BACKEND_TYPE_VM);
	}

};

} // End storage namespace
} // End peloton namespace

