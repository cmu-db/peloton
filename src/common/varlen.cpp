/*-------------------------------------------------------------------------
 *
 * string_ref.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/varlen.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "common/varlen_pool.h"
#include "common/varlen.h"

namespace nstore {

StringRef* StringRef::create(size_t size, Pool* dataPool){
	StringRef* retval;
	if (dataPool != NULL)
	{
		retval =
				new(dataPool->allocate(sizeof(StringRef))) StringRef(size, dataPool);
	}
	else
	{
		retval = new StringRef(size);
	}
	return retval;
}

void StringRef::destroy(StringRef* sref){
	delete sref;
}

StringRef::StringRef(size_t size){
	m_size = size + sizeof(StringRef*);
	m_tempPool = false;
	m_stringPtr = new char[m_size];
	setBackPtr();
}

StringRef::StringRef(std::size_t size, Pool* dataPool){
	m_tempPool = true;
	m_stringPtr =
			reinterpret_cast<char*>(dataPool->allocate(size + sizeof(StringRef*)));
	setBackPtr();
}

StringRef::~StringRef(){
	if (!m_tempPool){
		delete[] m_stringPtr;
	}
}

char* StringRef::get(){
	return m_stringPtr + sizeof(StringRef*);
}

const char* StringRef::get() const{
	return m_stringPtr + sizeof(StringRef*);
}

void StringRef::updateStringLocation(void* location){
	m_stringPtr = reinterpret_cast<char*>(location);
}

void StringRef::setBackPtr()
{
	StringRef** backptr = reinterpret_cast<StringRef**>(m_stringPtr);
	*backptr = this;
}

} // End nstore namespace

