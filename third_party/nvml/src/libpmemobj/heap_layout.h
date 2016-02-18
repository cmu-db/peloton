/*
 * Copyright (c) 2015, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * heap_layout.h -- internal definitions for heap layout
 */

#define	HEAP_MAJOR 1
#define	HEAP_MINOR 0

#define	MAX_CHUNK (UINT16_MAX - 7) /* has to be multiple of 8 */
#define	CHUNKSIZE ((size_t)1024 * 256)	/* 256 kilobytes */
#define	MAX_MEMORY_BLOCK_SIZE (MAX_CHUNK * CHUNKSIZE)
#define	HEAP_SIGNATURE_LEN 16
#define	HEAP_SIGNATURE "MEMORY_HEAP_HDR\0"
#define	ZONE_HEADER_MAGIC 0xC3F0A2D2
#define	ZONE_MIN_SIZE (sizeof (struct zone) - (MAX_CHUNK - 1) * CHUNKSIZE)
#define	ZONE_MAX_SIZE (sizeof (struct zone))
#define	HEAP_MIN_SIZE (sizeof (struct heap_layout) + ZONE_MIN_SIZE)
#define	REDO_LOG_SIZE 4
#define	BITS_PER_VALUE 64U
#define	MAX_CACHELINE_ALIGNMENT 40 /* run alignment, 5 cachelines */
#define	RUN_METASIZE (MAX_CACHELINE_ALIGNMENT * 8)
#define	MAX_BITMAP_VALUES (MAX_CACHELINE_ALIGNMENT - 2)
#define	RUN_BITMAP_SIZE (BITS_PER_VALUE * MAX_BITMAP_VALUES)
#define	RUNSIZE (CHUNKSIZE - RUN_METASIZE)
#define	MIN_RUN_SIZE 128

enum chunk_flags {
	CHUNK_FLAG_ZEROED	=	0x0001,
	CHUNK_RUN_ACTIVE	=	0x0002
};

enum chunk_type {
	CHUNK_TYPE_UNKNOWN,
	CHUNK_TYPE_FOOTER, /* not actual chunk type */
	CHUNK_TYPE_FREE,
	CHUNK_TYPE_USED,
	CHUNK_TYPE_RUN,

	MAX_CHUNK_TYPE
};

struct chunk {
	uint8_t data[CHUNKSIZE];
};

struct chunk_run {
	uint64_t block_size;
	uint64_t bucket_vptr; /* runtime information */
	uint64_t bitmap[MAX_BITMAP_VALUES];
	uint8_t data[RUNSIZE];
};

struct chunk_header {
	uint16_t type;
	uint16_t flags;
	uint32_t size_idx;
};

struct zone_header {
	uint32_t magic;
	uint32_t size_idx;
	uint8_t reserved[56];
};

struct zone {
	struct zone_header header;
	struct chunk_header chunk_headers[MAX_CHUNK];
	struct chunk chunks[MAX_CHUNK];
};

struct heap_header {
	char signature[HEAP_SIGNATURE_LEN];
	uint64_t major;
	uint64_t minor;
	uint64_t size;
	uint64_t chunksize;
	uint64_t chunks_per_zone;
	uint8_t reserved[960];
	uint64_t checksum;
};

struct heap_layout {
	struct heap_header header;
	struct zone zones[];
};

struct allocation_header {
	uint32_t zone_id;
	uint32_t chunk_id;
	uint64_t size;
};

struct allocator_lane_section {
	struct redo_log redo[REDO_LOG_SIZE];
};
