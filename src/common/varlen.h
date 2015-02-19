/*-------------------------------------------------------------------------
 *
 * varlen.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/varlen.h
 *
 *-------------------------------------------------------------------------
 */


#pragma once

#include <cstddef>

namespace nstore {

class Pool;

/// An object to use in lieu of raw char* pointers for strings
/// which are not inlined into tuple storage. This provides a
/// constant value to live in tuple storage while allowing the memory
/// containing the actual string to be moved around as the result of
/// compaction.
class StringRef{
public:
	//friend class CompactingStringPool;

	/// Create and return a new StringRef object which points to an
	/// allocated memory block of the requested size. The caller
	/// may provide an optional Pool from which the memory (and
	/// the memory for the StringRef object itself) will be
	/// allocated, intended for temporary strings. If no Pool
	/// object is provided, the StringRef and the string memory will be
	/// allocated out of the ThreadLocalPool.
	static StringRef* create(std::size_t size,
			Pool* dataPool = NULL);

	/// Destroy the given StringRef object and free any memory, if
	/// any, allocated from pools to store the object.
	/// sref must have been allocated and returned by a call to
	/// StringRef::create() and must not have been created in a
	/// temporary Pool
	static void destroy(StringRef* sref);
	char* get();
	const char* get() const;

private:
	StringRef(std::size_t size);
	StringRef(std::size_t size, Pool* dataPool);
	~StringRef();
	/// Callback used via the back-pointer in order to update the
	/// pointer to the memory backing this string reference
	void updateStringLocation(void* location);
	void setBackPtr();
	std::size_t m_size;
	bool m_tempPool;
	char* m_stringPtr;
};

} // End nstore namespace

