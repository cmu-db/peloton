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

#ifndef CDSLIB_INTRUSIVE_OPTIONS_H
#define CDSLIB_INTRUSIVE_OPTIONS_H

#include <cds/opt/options.h>
#include <cds/details/allocator.h>

namespace cds { namespace intrusive {

    /// Common options for intrusive containers
    /** @ingroup cds_intrusive_helper
        This namespace contains options for intrusive containers.
        It imports all definitions from cds::opt namespace and introduces a lot
        of options specific for intrusive approach.
    */
    namespace opt {
        using namespace cds::opt;

        //@cond
        struct base_hook_tag;
        struct member_hook_tag;
        struct traits_hook_tag;
        //@endcond

        /// Hook option
        /**
            Hook is a class that a user must add as a base class or as a member to make the user class compatible with intrusive containers.
            \p Hook template parameter strongly depends on the type of intrusive container you use.
        */
        template <typename Hook>
        struct hook {
            //@cond
            template <typename Base> struct pack: public Base
            {
                typedef Hook hook;
            };
            //@endcond
        };

        /// Item disposer option setter
        /**
            The option specifies a functor that is used for dispose removed items.
            The interface of \p Type functor is:
            \code
            struct myDisposer {
                void operator ()( T * val );
            };
            \endcode

            Predefined types for \p Type:
            - \p opt::v::empty_disposer - the disposer that does nothing
            - \p opt::v::delete_disposer - the disposer that calls operator \p delete

            Usually, the disposer should be stateless default-constructible functor.
            It is called by garbage collector in deferred mode.
        */
        template <typename Type>
        struct disposer {
            //@cond
            template <typename Base> struct pack: public Base
            {
                typedef Type disposer;
            };
            //@endcond
        };

        /// Values of \ref cds::intrusive::opt::link_checker option
        enum link_check_type {
            never_check_link,    ///< no link checking performed
            debug_check_link,    ///< check only in debug build
            always_check_link    ///< check in debug and release build
        };

        /// Link checking
        /**
            The option specifies a type of link checking.
            Possible values for \p Value are is one of \ref link_check_type enum:
            - \ref never_check_link - no link checking performed
            - \ref debug_check_link - check only in debug build
            - \ref always_check_link - check in debug and release build (not yet implemented for release mode).

            When link checking is on, the container tests that the node's link fields
            must be \p nullptr before inserting the item. If the link is not \p nullptr an assertion is generated
        */
        template <link_check_type Value>
        struct link_checker {
            //@cond
            template <typename Base> struct pack: public Base
            {
                static const link_check_type link_checker = Value;
            };
            //@endcond
        };

        /// Predefined option values
        namespace v {
            using namespace cds::opt::v;

            //@cond
            /// No link checking
            template <typename Node>
            struct empty_link_checker
            {
                //@cond
                typedef Node node_type;

                static void is_empty( const node_type * /*pNode*/ )
                {}
                //@endcond
            };
            //@endcond

            /// Empty item disposer
            /**
                The disposer does nothing.
                This is one of possible values of opt::disposer option.
            */
            struct empty_disposer
            {
                /// Empty dispose functor
                template <typename T>
                void operator ()( T * )
                {}
            };

            /// Deletion item disposer
            /**
                Analogue of operator \p delete call.
                The disposer that calls \p T destructor and deallocates the item via \p Alloc allocator.
            */
            template <typename Alloc = CDS_DEFAULT_ALLOCATOR >
            struct delete_disposer
            {
                /// Dispose functor
                template <typename T>
                void operator ()( T * p )
                {
                    cds::details::Allocator<T, Alloc> alloc;
                    alloc.Delete( p );
                }
            };
        }   // namespace v

        //@cond
        // Lazy-list specific option (for split-list support)
        template <typename Type>
        struct boundary_node_type {
            //@cond
            template <typename Base> struct pack: public Base
            {
                typedef Type boundary_node_type;
            };
            //@endcond
        };
        //@endcond
    } // namespace opt

}} // namespace cds::intrusive

#endif // #ifndef CDSLIB_INTRUSIVE_OPTIONS_H
