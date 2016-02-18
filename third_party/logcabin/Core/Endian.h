/* Copyright (c) 2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <cinttypes>
#include <endian.h>

#ifndef LOGCABIN_CORE_ENDIAN_H
#define LOGCABIN_CORE_ENDIAN_H

// RHEL6 is missing this cast, which affects htobe16 and
// be16toh.
#ifdef __bswap_constant_16
#undef __bswap_constant_16
#define __bswap_constant_16(x) \
     (uint16_t((((x) >> 8) & 0xff) | (((x) & 0xff) << 8)))
#endif

#endif /* LOGCABIN_CORE_ENDIAN_H */
