/*
    This file is a part of libcds - Concurrent Data Structures library

    (C) Copyright Maxim Khizhinsky (libcds.dev@gmail.com) 2006-2016

    Source code repo: http://github.com/khizmax/libcds/
    Download: http://sourceforge.net/projects/libcds/files/
    
    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice, this
      list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
    AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
    FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
    DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
    SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
    OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.     
*/

#ifndef CDSLIB_DETAILS_ALIGNED_TYPE_H
#define CDSLIB_DETAILS_ALIGNED_TYPE_H

#include <cds/details/defs.h>

namespace cds { namespace details {

    /// Aligned type
    /**
        This meta-algorithm solves compiler problem when you need to declare a type \p T with alignment
        equal to another type alignment. For example, the following declaration produces an error in Microsoft Visual Studio 2008 compiler:
        \code
            typedef double  my_double;
            typedef __declspec(align( __alignof(my_double) )) int   aligned_int;
        \endcode
        In MS VS, the __declspec(align(N)) construction requires that N must be a integer constant (1, 2, 4 and so on)
        but not an integer constant expression.

        The result of this meta-algo is a type \p aligned_type<T,Alignment>::type that is \p T aligned by \p Alignment.
        For example, with \p aligned_type the prevoius example will not generate an error:
        \code
            typedef double  my_double;
            typedef typename cds::details::aligned_type<int, __alignof(my_double)>::type   aligned_int;
        \endcode
        and result of this declaration is equivalent to
        \code
            typedef __declspec(align(8)) int   aligned_int;
        \endcode

        The \p Alignment template parameter must be a constant expression and its result must be power of two.
        The maximum of its value is 1024.

        See also \ref align_as
    */
    template <typename T, size_t Alignment>
    struct aligned_type
#ifdef CDS_DOXYGEN_INVOKED
        {}
#endif
;

    //@cond none
#   define CDS_ALIGNED_TYPE_impl(nAlign) template <typename T> struct aligned_type<T,nAlign> { typedef CDS_TYPE_ALIGNMENT(nAlign) T type; }
    CDS_ALIGNED_TYPE_impl(1);
    CDS_ALIGNED_TYPE_impl(2);
    CDS_ALIGNED_TYPE_impl(4);
    CDS_ALIGNED_TYPE_impl(8);
    CDS_ALIGNED_TYPE_impl(16);
    CDS_ALIGNED_TYPE_impl(32);
    CDS_ALIGNED_TYPE_impl(64);
    CDS_ALIGNED_TYPE_impl(128);
    CDS_ALIGNED_TYPE_impl(256);
    CDS_ALIGNED_TYPE_impl(512);
    CDS_ALIGNED_TYPE_impl(1024);
#   undef CDS_ALIGNED_TYPE_impl
    //@endcond

    /** Alignment by example

        This meta-algo is similar to \ref aligned_type <T, alignof(AlignAs)>.

        For example, the following code
        \code
        typedef typename cds::details::align_as<int, double>::type   aligned_int;
        \endcode
        declares type \p aligned_int which is \p int aligned like \p double.

        See also: \ref aligned_type
    */
    template <typename T, typename AlignAs>
    struct align_as {
        /// Result of meta-algo: type \p T aligned like type \p AlignAs
        typedef typename aligned_type<T, alignof(AlignAs)>::type type;
    };

}}  // namespace cds::details

#endif // #ifndef CDSLIB_DETAILS_ALIGNED_TYPE_H
