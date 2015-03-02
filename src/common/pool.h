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

#include "storage/backend.h"

namespace nstore {

static const size_t TEMP_POOL_CHUNK_SIZE = 1024 * 1024; // 1 MB

//===--------------------------------------------------------------------===//
// Chunk of memory allocated on the heap
//===--------------------------------------------------------------------===//

class Chunk {
public:
	Chunk(): offset(0), size(0), chunk_data(NULL){
	}

	inline Chunk(uint64_t size, void *chunkData)
	: offset(0), size(size), chunk_data(static_cast<char*>(chunkData))	{
	}

	int64_t getSize() const	{
		return static_cast<int64_t>(size);
	}

	uint64_t offset;
	uint64_t size;
	char *chunk_data;
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
	Pool() = delete;
	Pool(const Pool&) = delete;
	Pool& operator=(const Pool&) = delete;

public:

	Pool(storage::Backend *_backend) :
		backend(_backend),
		allocation_size(TEMP_POOL_CHUNK_SIZE),
		max_chunk_count(1),
		current_chunk_index(0){
		Init();
	}

	Pool(storage::Backend *_backend, uint64_t allocation_size, uint64_t max_chunk_count) :
		backend(_backend),
		allocation_size(allocation_size),
		max_chunk_count(static_cast<std::size_t>(max_chunk_count)),
		current_chunk_index(0){
		Init();
	}

	void Init() {
		char *storage = (char *) backend->Allocate(allocation_size);
		chunks.push_back(Chunk(allocation_size, storage));
	}

	~Pool() {
		for (std::size_t ii = 0; ii < chunks.size(); ii++) {
			backend->Free(chunks[ii].chunk_data);
		}
		for (std::size_t ii = 0; ii < oversize_chunks.size(); ii++) {
			backend->Free(oversize_chunks[ii].chunk_data);
		}
	}

	/// Allocate a continous block of memory of the specified size.
	inline void* Allocate(std::size_t size) {

		/// See if there is space in the current chunk
		Chunk *current_chunk = &chunks[current_chunk_index];
		if (size > current_chunk->size - current_chunk->offset) {

			/// Not enough space. Check if it is greater than our allocation size.
			if (size > allocation_size) {

				/// Allocate an oversize chunk that will not be reused.
				char *storage = (char *) backend->Allocate(size);
				oversize_chunks.push_back(Chunk(nexthigher(size), storage));
				Chunk &newChunk = oversize_chunks.back();
				newChunk.offset = size;
				return newChunk.chunk_data;
			}

			/// Check if there is an already allocated chunk we can use.
			current_chunk_index++;

			if (current_chunk_index < chunks.size()) {
				current_chunk = &chunks[current_chunk_index];
				current_chunk->offset = size;
				return current_chunk->chunk_data;
			}
			else {
				/// Need to allocate a new chunk
				char *storage = (char *) backend->Allocate(allocation_size);
				chunks.push_back(Chunk(allocation_size, storage));
				Chunk &new_chunk = chunks.back();
				new_chunk.offset = size;
				return new_chunk.chunk_data;
			}
		}

		/// Get the offset into the current chunk. Then increment the
		/// offset counter by the amount being allocated.
		void *retval = current_chunk->chunk_data + current_chunk->offset;
		current_chunk->offset += size;

		///	Ensure 8 byte alignment of future allocations
		current_chunk->offset += (8 - (current_chunk->offset % 8));
		if (current_chunk->offset > current_chunk->size) {
			current_chunk->offset = current_chunk->size;
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
			backend->Free(oversize_chunks[ii].chunk_data);
		}
		oversize_chunks.clear();

		/// Set the current chunk to the first in the list
		current_chunk_index = 0;
		std::size_t num_chunks = chunks.size();

		/// If more then maxChunkCount chunks are allocated erase all extra chunks
		if (num_chunks > max_chunk_count) {
			for (std::size_t ii = max_chunk_count; ii < num_chunks; ii++) {
				backend->Free(chunks[ii].chunk_data);
			}
			chunks.resize(max_chunk_count);
		}

		num_chunks = chunks.size();
		for (std::size_t ii = 0; ii < num_chunks; ii++) {
			chunks[ii].offset = 0;
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

	/// Location of pool on storage
	storage::Backend *backend;

	const uint64_t allocation_size;
	std::size_t max_chunk_count;
	std::size_t current_chunk_index;
	std::vector<Chunk> chunks;

	/// Oversize chunks that will be freed and not reused.
	std::vector<Chunk> oversize_chunks;
};

} // End nstore namespace



