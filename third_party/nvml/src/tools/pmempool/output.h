/*
 * Copyright (c) 2014-2015, Intel Corporation
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
 * output.h -- declarations of output printing related functions
 */

#include <uuid/uuid.h>
#include <time.h>
#include <stdint.h>
#include <stdio.h>

void out_set_vlevel(int vlevel);
void out_set_stream(FILE *stream);
void out_set_prefix(const char *prefix);
void out_set_col_width(unsigned int col_width);
void outv_err(const char *fmt, ...);
void out_err(const char *file, int line, const char *func,
		const char *fmt, ...);
void outv_err_vargs(const char *fmt, va_list ap);
void out_indent(int i);
void outv(int vlevel, const char *fmt, ...);
void outv_nl(int vlevel);
int outv_check(int vlevel);
void outv_title(int vlevel, const char *fmt, ...);
void outv_field(int vlevel, const char *field, const char *fmt, ...);
void outv_hexdump(int vlevel, const void *addr, size_t len, size_t offset,
		int sep);
const char *out_get_uuid_str(uuid_t uuid);
const char *out_get_time_str(time_t time);
const char *out_get_size_str(uint64_t size, int human);
const char *out_get_percentage(double percentage);
const char *out_get_checksum(void *addr, size_t len, uint64_t *csump);
const char *out_get_btt_map_entry(uint32_t map);
const char *out_get_pool_type_str(pmem_pool_type_t type);
const char *out_get_pool_signature(pmem_pool_type_t type);
const char *out_get_lane_section_str(enum lane_section_type type);
const char *out_get_tx_state_str(uint64_t state);
const char *out_get_chunk_type_str(enum chunk_type type);
const char *out_get_chunk_flags(uint16_t flags);
const char *out_get_zone_magic_str(uint32_t magic);
const char *out_get_pmemoid_str(PMEMoid oid, uint64_t uuid_lo);
const char *out_get_internal_type_str(enum internal_type type);
const char *out_get_ei_class_str(uint8_t ei_class);
const char *out_get_ei_data_str(uint8_t ei_class);
const char *out_get_e_machine_str(uint16_t e_machine);
const char *out_get_alignment_desc_str(uint64_t ad, uint64_t cur_ad);
