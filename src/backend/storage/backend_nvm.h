/*-------------------------------------------------------------------------
*
* persistent_backend.h
* file description
*
* Copyright(c) 2015, CMU
*
* /n-store/src/storage/persistent_backend.h
*
*-------------------------------------------------------------------------
*/

#pragma once

#include "backend/storage/abstract_backend.h"

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// NVM Backend (Non-volatile memory)
//===--------------------------------------------------------------------===//


class NVMBackend : public AbstractBackend {

public:
	virtual ~NVMBackend(){};

	void* Allocate(size_t size) {
		return ::operator new(size);
	}

	void Free(void* ptr) {
		::operator delete(ptr);
	}

	void Sync(void* ptr)  {
		// does nothing
	}

	std::string GetBackendType() const{
		return BackendTypeToString(BACKEND_TYPE_NVM);
	}

};

} // End storage namespace
} // End peloton namespace



