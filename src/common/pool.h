/*-------------------------------------------------------------------------
 *
 * pool.h
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
#include <errno.h>
#include <climits>
#include <string.h>

namespace nstore {

static const size_t TEMP_POOL_CHUNK_SIZE = 256 * 1024; // 256 KB

//===--------------------------------------------------------------------===//
// Chunk of memory allocated on the heap
//===--------------------------------------------------------------------===//

class Chunk {
public:
	Chunk(): m_offset(0), m_size(0), m_chunkData(NULL){
	}

	inline Chunk(uint64_t size, void *chunkData)
	: m_offset(0), m_size(size), m_chunkData(static_cast<char*>(chunkData))	{
	}

	int64_t getSize() const	{
		return static_cast<int64_t>(m_size);
	}

	uint64_t m_offset;
	uint64_t m_size;
	char *m_chunkData;
};

/// Find next higher power of two
template <class T>
inline T nexthigher(T k) {
	if (k == 0)
		return 1;
	k--;
	for (uint i=1; i<sizeof(T)*CHAR_BIT; i<<=1)
		k = k | k >> i;
	return k+1;
}

//===--------------------------------------------------------------------===//
// Memory Pool
//===--------------------------------------------------------------------===//

/**
 * A memory pool that provides fast allocation and deallocation. The
 * only way to release memory is to free all memory in the pool by
 * calling purge.
 */
class Pool {
	Pool(const Pool&) = delete;
	Pool& operator=(const Pool&) = delete;

public:

	Pool() :
		allocation_size(TEMP_POOL_CHUNK_SIZE), max_chunk_count(1), current_chunk_index(0){
		Init();
	}

	Pool(uint64_t allocationSize, uint64_t maxChunkCount) :
		allocation_size(allocationSize),
		max_chunk_count(static_cast<std::size_t>(maxChunkCount)),
		current_chunk_index(0){
		Init();
	}

	void Init() {
		char *storage = new char[allocation_size];
		chunks.push_back(Chunk(allocation_size, storage));
	}

	~Pool() {
		for (std::size_t ii = 0; ii < chunks.size(); ii++) {
			delete [] chunks[ii].m_chunkData;
		}
		for (std::size_t ii = 0; ii < oversize_chunks.size(); ii++) {
			delete [] oversize_chunks[ii].m_chunkData;
		}
	}

	/// Allocate a continous block of memory of the specified size.
	inline void* Allocate(std::size_t size) {

		/// See if there is space in the current chunk
		Chunk *currentChunk = &chunks[current_chunk_index];
		if (size > currentChunk->m_size - currentChunk->m_offset) {

			/// Not enough space. Check if it is greater then our allocation size.
			if (size > allocation_size) {

				/// Allocate an oversize chunk that will not be reused.
				char *storage = new char[size];
				oversize_chunks.push_back(Chunk(nexthigher(size), storage));
				Chunk &newChunk = oversize_chunks.back();
				newChunk.m_offset = size;
				return newChunk.m_chunkData;
			}

			/// Check if there is an already allocated chunk we can use.
			current_chunk_index++;
			if (current_chunk_index < chunks.size()) {
				currentChunk = &chunks[current_chunk_index];
				currentChunk->m_offset = size;
				return currentChunk->m_chunkData;
			} else {
				/// Need to allocate a new chunk
				char *storage = new char[allocation_size];
				chunks.push_back(Chunk(allocation_size, storage));
				Chunk &newChunk = chunks.back();
				newChunk.m_offset = size;
				return newChunk.m_chunkData;
			}
		}

		/// Get the offset into the current chunk. Then increment the
		/// offset counter by the amount being allocated.
		void *retval = currentChunk->m_chunkData + currentChunk->m_offset;
		currentChunk->m_offset += size;

		///	Ensure 8 byte alignment of future allocations
		currentChunk->m_offset += (8 - (currentChunk->m_offset % 8));
		if (currentChunk->m_offset > currentChunk->m_size) {
			currentChunk->m_offset = currentChunk->m_size;
		}

		return retval;
	}

	/// Allocate a continous block of memory of the specified size conveniently initialized to 0s
	inline void* AllocateZeroes(std::size_t size) {
		return ::memset(Allocate(size), 0, size);
	}

	inline void Purge() {

		/// Erase any oversize chunks that were allocated
		const std::size_t numOversizeChunks = oversize_chunks.size();
		for (std::size_t ii = 0; ii < numOversizeChunks; ii++) {
			delete [] oversize_chunks[ii].m_chunkData;
		}
		oversize_chunks.clear();

		/// Set the current chunk to the first in the list
		current_chunk_index = 0;
		std::size_t numChunks = chunks.size();

		/// If more then maxChunkCount chunks are allocated erase all extra chunks
		if (numChunks > max_chunk_count) {
			for (std::size_t ii = max_chunk_count; ii < numChunks; ii++) {
				delete []chunks[ii].m_chunkData;
			}
			chunks.resize(max_chunk_count);
		}

		numChunks = chunks.size();
		for (std::size_t ii = 0; ii < numChunks; ii++) {
			chunks[ii].m_offset = 0;
		}
	}

	int64_t GetAllocatedMemory()
	{
		int64_t total = 0;
		total += chunks.size() * allocation_size;
		for (uint32_t i = 0; i < oversize_chunks.size(); i++)
		{
			total += oversize_chunks[i].getSize();
		}
		return total;
	}

private:

	const uint64_t allocation_size;
	std::size_t max_chunk_count;
	std::size_t current_chunk_index;
	std::vector<Chunk> chunks;

	/// Oversize chunks that will be freed and not reused.
	std::vector<Chunk> oversize_chunks;
};

} // End nstore namespace



