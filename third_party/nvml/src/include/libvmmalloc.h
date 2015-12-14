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
 * libvmmalloc.h -- definitions of libvmmalloc entry points
 *
 * This library exposes memory-mapped files as volatile memory (a la malloc)
 *
 * See libvmmalloc(3) for details.
 */

#ifndef	LIBVMMALLOC_H
#define	LIBVMMALLOC_H 1

#ifdef __cplusplus
extern "C" {
#endif

#define	VMMALLOC_MAJOR_VERSION 1
#define	VMMALLOC_MINOR_VERSION 0

#include <sys/types.h>

#define	VMMALLOC_MIN_POOL ((size_t)(1024 * 1024 * 14)) /* min pool size: 14MB */

#define	VMMALLOC_OVERRIDE_ALIGNED_ALLOC
#define	VMMALLOC_OVERRIDE_VALLOC
#define	VMMALLOC_OVERRIDE_MEMALIGN

/*
 * check compiler support for various function attributes
 */
#if defined(__GNUC__) && !defined(__clang__)

#define	GCC_VER (__GNUC__ * 100 + __GNUC_MINOR__)

#if GCC_VER >= 296
#define	__ATTR_MALLOC__ __attribute__((malloc))
#else
#define	__ATTR_MALLOC__
#endif

#if GCC_VER >= 303
#define	__ATTR_NONNULL__(x) __attribute__((nonnull(x)))
#else
#define	__ATTR_NONNULL__(x)
#endif

#if GCC_VER >= 403
#define	__ATTR_ALLOC_SIZE__(...) __attribute__((alloc_size(__VA_ARGS__)))
#else
#define	__ATTR_ALLOC_SIZE__(...)
#endif

#if GCC_VER >= 409
#define	__ATTR_ALLOC_ALIGN__(x) __attribute__((alloc_align(x)))
#else
#define	__ATTR_ALLOC_ALIGN__(x)
#endif

#else /* clang and other compilers */

#ifndef __has_attribute
#define	__has_attribute(x) 0
#endif

#if __has_attribute(malloc)
#define	__ATTR_MALLOC__ __attribute__((malloc))
#else
#define	__ATTR_MALLOC__
#endif

#if __has_attribute(nonnull)
#define	__ATTR_NONNULL__(x) __attribute__((nonnull(x)))
#else
#define	__ATTR_NONNULL__(x)
#endif

#if __has_attribute(alloc_size)
#define	__ATTR_ALLOC_SIZE__(...) __attribute__((alloc_size(__VA_ARGS__)))
#else
#define	__ATTR_ALLOC_SIZE__(...)
#endif

#if __has_attribute(alloc_align)
#define	__ATTR_ALLOC_ALIGN__(x) __attribute__((alloc_align(x)))
#else
#define	__ATTR_ALLOC_ALIGN__(x)
#endif

#endif /* __GNUC__ */

extern void *malloc(size_t size) __ATTR_MALLOC__ __ATTR_ALLOC_SIZE__(1);

extern void *calloc(size_t nmemb, size_t size)
	__ATTR_MALLOC__ __ATTR_ALLOC_SIZE__(1, 2);

extern void *realloc(void *ptr, size_t size) __ATTR_ALLOC_SIZE__(2);

extern void free(void *ptr);

extern void cfree(void *ptr);

extern int posix_memalign(void **memptr, size_t alignment, size_t size)
	__ATTR_NONNULL__(1);

#ifdef	VMMALLOC_OVERRIDE_MEMALIGN
extern void *memalign(size_t boundary, size_t size)
	__ATTR_MALLOC__ __ATTR_ALLOC_ALIGN__(1) __ATTR_ALLOC_SIZE__(2);
#endif

#ifdef	VMMALLOC_OVERRIDE_ALIGNED_ALLOC
extern void *aligned_alloc(size_t alignment, size_t size)
	__ATTR_MALLOC__ __ATTR_ALLOC_ALIGN__(1) __ATTR_ALLOC_SIZE__(2);
#endif

#ifdef	VMMALLOC_OVERRIDE_VALLOC
extern void *valloc(size_t size) __ATTR_MALLOC__ __ATTR_ALLOC_SIZE__(1);

extern void *pvalloc(size_t size) __ATTR_MALLOC__ __ATTR_ALLOC_SIZE__(1);
#endif

extern size_t malloc_usable_size(void *ptr);

#ifdef __cplusplus
}
#endif
#endif	/* libvmmalloc.h */
