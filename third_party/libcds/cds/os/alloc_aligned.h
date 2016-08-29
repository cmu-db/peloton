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

#ifndef CDSLIB_OS_ALLOC_ALIGNED_H
#define CDSLIB_OS_ALLOC_ALIGNED_H

#include <cds/details/defs.h>

#if CDS_OS_TYPE == CDS_OS_WIN32 || CDS_OS_TYPE == CDS_OS_WIN64 || CDS_OS_TYPE == CDS_OS_MINGW
#   include <cds/os/win/alloc_aligned.h>
#elif CDS_OS_TYPE == CDS_OS_LINUX
#   include <cds/os/linux/alloc_aligned.h>
#elif CDS_OS_TYPE == CDS_OS_SUN_SOLARIS
#   include <cds/os/sunos/alloc_aligned.h>
#elif CDS_OS_TYPE == CDS_OS_HPUX
#   include <cds/os/hpux/alloc_aligned.h>
#elif CDS_OS_TYPE == CDS_OS_AIX
#   include <cds/os/aix/alloc_aligned.h>
#elif CDS_OS_TYPE == CDS_OS_FREE_BSD || CDS_OS_TYPE == CDS_OS_OPEN_BSD || CDS_OS_TYPE == CDS_OS_NET_BSD
#   include <cds/os/free_bsd/alloc_aligned.h>
#elif CDS_OS_TYPE == CDS_OS_OSX || CDS_OS_TYPE == CDS_OS_PTHREAD
#   include <cds/os/posix/alloc_aligned.h>
#else
#   error Unknown OS. Compilation aborted
#endif

#include <memory>
#include <cds/details/is_aligned.h>
#include <cds/algo/int_algo.h>

namespace cds {
    /// OS specific wrappers
    /**
        This namespace contains OS-specific implementations.
        Usually, the sub-namespaces contain OS-specific wrappers for a feature.

        Typical usage pattern:
        \code
            namespace cds {
            namespace OS {
                namespace Linux {
                    class Feature {
                        // ...
                    };
                }

                // import Linux::Feature class into cds::OS namespace
                using Linux::Feature;
                // now, cds::OS::Feature refers to cds::OS::Linux::Feature
            }
            }
        \endcode
    */
    namespace OS {
        /// Aligned allocator
        /**
            This allocator is intended for allocating of an aligned memory block. It uses wrappers around platform-specific
            function for allocating and deallocating the block of memory:
            \li \p aligned_malloc for allocating
            \li \p aligned_free for deallocating

            The \p aligned_malloc function wraps:
            \li \p for Win: \p _aligned_malloc function
            \li \p for other OSes: \p posix_memalign / \p memalign function

            The \p aligned_free function wraps:
            \li \p for Win: \p _aligned_free function
            \li \p for other OSes: \p free function

            This class should not be used directly. Use cds::details::AlignedAllocator instead.
        */
        template <typename T>
        class aligned_allocator
        {
            typedef std::allocator<T>  prototype    ;   ///< prototype (the source of typedefs)
        public:
            typedef typename prototype::value_type       value_type      ;  ///< value type
            typedef typename prototype::pointer          pointer         ;  ///< pointer to value type
            typedef typename prototype::reference        reference       ;  ///< value reference type
            typedef typename prototype::const_pointer    const_pointer   ;  ///< const pointer to value type
            typedef typename prototype::const_reference  const_reference ;  ///< const value reference type

            typedef typename prototype::size_type        size_type       ;  ///< size type
            typedef typename prototype::difference_type  difference_type ;  ///< difference type

            /// convert an aligned_allocator<T> to an aligned_allocator<OTHER>
            template<class OTHER>
            struct rebind
            {
                typedef aligned_allocator<OTHER> other; ///< Rebinding result
            };

        public:
            /// return address of mutable \p v
            pointer address(reference v) const
            {
                prototype a;
                return a.address( v );
            }

            /// return address of nonmutable \p v
            const_pointer address(const_reference v) const
            {
                prototype a;
                return a.address( v );
            }

            // construct default allocator (do nothing)
            aligned_allocator() throw()
            {}

            /// construct by copying (do nothing)
            aligned_allocator(const aligned_allocator<T>&) throw()
            {}

            /// construct from a related allocator (do nothing)
            template <class OTHER>
            aligned_allocator(const aligned_allocator<OTHER>&) throw()
            {}

            /// assign from a related allocator (do nothing)
            template <class OTHER>
            aligned_allocator<T>& operator=(const aligned_allocator<OTHER>&)
            {
                return (*this);
            }

            /// deallocate object at \p ptr, ignore size
            void deallocate(pointer ptr, size_type)
            {
                cds::OS::aligned_free( ptr );
            }

            /// allocate array of \p nCount elements
            /**
                The address returned is aligned by \p nAlign boundary.
                \p nAlign parameter should be power of 2.

                The function guarantees the alignment for first element of array only.
                To guarantee the alignment for each element of the array the size of an object of type \p T must be multiple of \p nAlign:
                \code
                sizeof(T) % nAlign == 0
                \endcode

                The function, like operator \p new does not return \p nullptr.
                In no memory situation the function throws \p std::bad_alloc exception.
            */
            pointer allocate( size_type nAlign, size_type nCount )
            {
                assert( cds::beans::is_power2( nAlign ) );
                pointer p = reinterpret_cast<T *>( cds::OS::aligned_malloc( sizeof(T) * nCount, nAlign ) );
                if ( !p )
                    throw std::bad_alloc();
                assert( cds::details::is_aligned( p, nAlign ));
                return p;
            }

            /// allocate array of \p nCount elements, ignore hint
            /**
                The address returned is aligned by \p nAlign boundary.
                \p nAlign parameter should be power of 2.

                The function guarantees alignment for first element of array only.
            */
            pointer allocate(size_type nAlign, size_type nCount, const void *)
            {
                return ( allocate( nAlign, nCount ) );
            }

            /// construct object at \p ptr with value \p val
            void construct(pointer ptr, const T& val)
            {
                prototype a;
                a.construct( ptr, val );
            }

            /// destroy object at \p ptr
            void destroy(pointer ptr)
            {
                prototype a;
                a.destroy( ptr );
            }

            /// estimate maximum array size
            size_type max_size() const throw()
            {
                prototype a;
                return a.max_size();
            }
        };
    }   // namespace OS
}  // namespace cds

#endif  // #ifndef CDSLIB_OS_ALLOC_ALIGNED_H
