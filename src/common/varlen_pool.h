/*-------------------------------------------------------------------------
 *
 * varlen_pool.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/varlen_pool.h
 *
 *-------------------------------------------------------------------------
 */


#pragma once

#include <vector>
#include <iostream>
#include <stdint.h>
#include <sys/mman.h>
#include <errno.h>
#include <climits>
#include <string.h>

namespace nstore {

static const size_t TEMP_POOL_CHUNK_SIZE = 262144;

/**
 * Description of a chunk of memory allocated on the heap
 */
class Chunk {
public:
	Chunk()
: m_offset(0), m_size(0), m_chunkData(NULL)
{
}

	inline Chunk(uint64_t size, void *chunkData)
	: m_offset(0), m_size(size), m_chunkData(static_cast<char*>(chunkData))
	{
	}

	int64_t getSize() const
	{
		return static_cast<int64_t>(m_size);
	}

	uint64_t m_offset;
	uint64_t m_size;
	char *m_chunkData;
};

/*
 * Find next higher power of two
 */
template <class T>
inline T nexthigher(T k) {
	if (k == 0)
		return 1;
	k--;
	for (uint i=1; i<sizeof(T)*CHAR_BIT; i<<=1)
		k = k | k >> i;
	return k+1;
}

/**
 * A memory pool that provides fast allocation and deallocation. The
 * only way to release memory is to free all memory in the pool by
 * calling purge.
 */
class Pool {
public:

	Pool() :
		m_allocationSize(TEMP_POOL_CHUNK_SIZE), m_maxChunkCount(1), m_currentChunkIndex(0)
{
		init();
}

	Pool(uint64_t allocationSize, uint64_t maxChunkCount) :
		m_allocationSize(allocationSize),
		m_maxChunkCount(static_cast<std::size_t>(maxChunkCount)),
		m_currentChunkIndex(0)
	{
		init();
	}

	void init() {
		char *storage = new char[m_allocationSize];
		m_chunks.push_back(Chunk(m_allocationSize, storage));
	}

	~Pool() {
		for (std::size_t ii = 0; ii < m_chunks.size(); ii++) {
			delete [] m_chunks[ii].m_chunkData;
		}
		for (std::size_t ii = 0; ii < m_oversizeChunks.size(); ii++) {
			delete [] m_oversizeChunks[ii].m_chunkData;
		}
	}

	/*
	 * Allocate a continous block of memory of the specified size.
	 */
	 inline void* allocate(std::size_t size) {
		 /*
		  * See if there is space in the current chunk
		  */
		 Chunk *currentChunk = &m_chunks[m_currentChunkIndex];
		 if (size > currentChunk->m_size - currentChunk->m_offset) {
			 /*
			  * Not enough space. Check if it is greater then our allocation size.
			  */
			 if (size > m_allocationSize) {
				 /*
				  * Allocate an oversize chunk that will not be reused.
				  */
				 char *storage = new char[size];
				 m_oversizeChunks.push_back(Chunk(nexthigher(size), storage));
				 Chunk &newChunk = m_oversizeChunks.back();
				 newChunk.m_offset = size;
				 return newChunk.m_chunkData;
			 }

			 /*
			  * Check if there is an already allocated chunk we can use.
			  */
			 m_currentChunkIndex++;
			 if (m_currentChunkIndex < m_chunks.size()) {
				 currentChunk = &m_chunks[m_currentChunkIndex];
				 currentChunk->m_offset = size;
				 return currentChunk->m_chunkData;
			 } else {
				 /*
				  * Need to allocate a new chunk
				  */
				  //                std::cout << "Pool had to allocate a new chunk. Not a good thing "
				 //                  "from a performance perspective. If you see this we need to look "
				 //                  "into structuring our pool sizes and allocations so the this doesn't "
				 //                  "happen frequently" << std::endl;
				 char *storage = new char[m_allocationSize];
				 m_chunks.push_back(Chunk(m_allocationSize, storage));
				 Chunk &newChunk = m_chunks.back();
				 newChunk.m_offset = size;
				 return newChunk.m_chunkData;
			 }
		 }

		 /*
		  * Get the offset into the current chunk. Then increment the
		  * offset counter by the amount being allocated.
		  */
		 void *retval = currentChunk->m_chunkData + currentChunk->m_offset;
		 currentChunk->m_offset += size;

		 //Ensure 8 byte alignment of future allocations
		 currentChunk->m_offset += (8 - (currentChunk->m_offset % 8));
		 if (currentChunk->m_offset > currentChunk->m_size) {
			 currentChunk->m_offset = currentChunk->m_size;
		 }

		 return retval;
	 }

	 /*
	  * Allocate a continous block of memory of the specified size conveniently initialized to 0s
	  */
	 inline void* allocateZeroes(std::size_t size) { return ::memset(allocate(size), 0, size); }

	 inline void purge() {
		 /*
		  * Erase any oversize chunks that were allocated
		  */
		 const std::size_t numOversizeChunks = m_oversizeChunks.size();
		 for (std::size_t ii = 0; ii < numOversizeChunks; ii++) {
			 delete [] m_oversizeChunks[ii].m_chunkData;
		 }
		 m_oversizeChunks.clear();

		 /*
		  * Set the current chunk to the first in the list
		  */
		 m_currentChunkIndex = 0;
		 std::size_t numChunks = m_chunks.size();

		 /*
		  * If more then maxChunkCount chunks are allocated erase all extra chunks
		  */
		 if (numChunks > m_maxChunkCount) {
			 for (std::size_t ii = m_maxChunkCount; ii < numChunks; ii++) {
				 delete []m_chunks[ii].m_chunkData;
			 }
			 m_chunks.resize(m_maxChunkCount);
		 }

		 numChunks = m_chunks.size();
		 for (std::size_t ii = 0; ii < numChunks; ii++) {
			 m_chunks[ii].m_offset = 0;
		 }
	 }

	 int64_t getAllocatedMemory()
	 {
		 int64_t total = 0;
		 total += m_chunks.size() * m_allocationSize;
		 for (uint32_t i = 0; i < m_oversizeChunks.size(); i++)
		 {
			 total += m_oversizeChunks[i].getSize();
		 }
		 return total;
	 }

private:
	 const uint64_t m_allocationSize;
	 std::size_t m_maxChunkCount;
	 std::size_t m_currentChunkIndex;
	 std::vector<Chunk> m_chunks;
	 /*
	  * Oversize chunks that will be freed and not reused.
	  */
	 std::vector<Chunk> m_oversizeChunks;
	 // No implicit copies
	 Pool(const Pool&);
	 Pool& operator=(const Pool&);
};

} // End nstore namespace



