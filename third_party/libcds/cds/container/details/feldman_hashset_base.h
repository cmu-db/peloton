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

#ifndef CDSLIB_CONTAINER_DETAILS_FELDMAN_HASHSET_BASE_H
#define CDSLIB_CONTAINER_DETAILS_FELDMAN_HASHSET_BASE_H

#include <cds/intrusive/details/feldman_hashset_base.h>
#include <cds/container/details/base.h>

namespace cds { namespace container {
    /// \p FeldmanHashSet related definitions
    /** @ingroup cds_nonintrusive_helper
    */
    namespace feldman_hashset {
        /// Hash accessor option
        /**
            @copydetails cds::intrusive::feldman_hashset::traits::hash_accessor
        */
        template <typename Accessor>
        using hash_accessor = cds::intrusive::feldman_hashset::hash_accessor< Accessor >;

        /// \p FeldmanHashSet internal statistics, see cds::intrusive::feldman_hashset::stat
        template <typename EventCounter = cds::atomicity::event_counter>
        using stat = cds::intrusive::feldman_hashset::stat< EventCounter >;

        /// \p FeldmanHashSet empty internal statistics
        typedef cds::intrusive::feldman_hashset::empty_stat empty_stat;

        /// Bit-wise memcmp-based comparator for hash value \p T
        template <typename T>
        using bitwise_compare = cds::intrusive::feldman_hashset::bitwise_compare< T >;

        /// \p FeldmanHashSet level statistics
        typedef cds::intrusive::feldman_hashset::level_statistics level_statistics;

        /// \p FeldmanHashSet traits
        struct traits
        {
            /// Mandatory functor to get hash value from data node
            /**
                @copydetails cds::intrusive::feldman_hashset::traits::hash_accessor
            */
            typedef cds::opt::none hash_accessor;

            /// Hash comparing functor
            /**
                @copydetails cds::intrusive::feldman_hashset::traits::compare
            */
            typedef cds::opt::none compare;

            /// Specifies binary predicate used for hash compare.
            /**
                @copydetails cds::intrusive::feldman_hashset::traits::less
            */
            typedef cds::opt::none less;

            /// Item counter
            /**
                @copydetails cds::intrusive::feldman_hashset::traits::item_counter
            */
            typedef cds::atomicity::item_counter item_counter;

            /// Item allocator
            /**
                Default is \ref CDS_DEFAULT_ALLOCATOR
            */
            typedef CDS_DEFAULT_ALLOCATOR allocator;

            /// Array node allocator
            /**
                @copydetails cds::intrusive::feldman_hashset::traits::node_allocator
            */
            typedef CDS_DEFAULT_ALLOCATOR node_allocator;

            /// C++ memory ordering model
            /**
                @copydetails cds::intrusive::feldman_hashset::traits::memory_model
            */
            typedef cds::opt::v::relaxed_ordering memory_model;

            /// Back-off strategy
            typedef cds::backoff::Default back_off;

            /// Internal statistics
            /**
                @copydetails cds::intrusive::feldman_hashset::traits::stat
            */
            typedef empty_stat stat;

            /// RCU deadlock checking policy (only for \ref cds_container_FeldmanHashSet_rcu "RCU-based FeldmanHashSet")
            /**
                @copydetails cds::intrusive::feldman_hashset::traits::rcu_check_deadlock
            */
            typedef cds::opt::v::rcu_throw_deadlock rcu_check_deadlock;
        };

        /// Metafunction converting option list to \p feldman_hashset::traits
        /**
            Supported \p Options are:
            - \p feldman_hashset::hash_accessor - mandatory option, hash accessor functor.
                @copydetails traits::hash_accessor
            - \p opt::allocator - item allocator
                @copydetails traits::allocator
            - \p opt::node_allocator - array node allocator.
                @copydetails traits::node_allocator
            - \p opt::compare - hash comparison functor. No default functor is provided.
                If the option is not specified, the \p opt::less is used.
            - \p opt::less - specifies binary predicate used for hash comparison.
                @copydetails cds::container::feldman_hashset::traits::less
            - \p opt::back_off - back-off strategy used. If the option is not specified, the \p cds::backoff::Default is used.
            - \p opt::item_counter - the type of item counting feature.
                @copydetails cds::intrusive::feldman_hashset::traits::item_counter
            - \p opt::memory_model - C++ memory ordering model. Can be \p opt::v::relaxed_ordering (relaxed memory model, the default)
                or \p opt::v::sequential_consistent (sequentially consisnent memory model).
            - \p opt::stat - internal statistics. By default, it is disabled (\p feldman_hashset::empty_stat).
                To enable it use \p feldman_hashset::stat
            - \p opt::rcu_check_deadlock - a deadlock checking policy for \ref cds_intrusive_FeldmanHashSet_rcu "RCU-based FeldmanHashSet"
                Default is \p opt::v::rcu_throw_deadlock
        */
        template <typename... Options>
        struct make_traits
        {
#   ifdef CDS_DOXYGEN_INVOKED
            typedef implementation_defined type ;   ///< Metafunction result
#   else
            typedef typename cds::opt::make_options<
                typename cds::opt::find_type_traits< traits, Options... >::type
                ,Options...
            >::type   type;
#   endif
        };
    } // namespace feldman_hashset

    //@cond
    // Forward declaration
    template < class GC, typename T, class Traits = cds::container::feldman_hashset::traits >
    class FeldmanHashSet;
    //@endcond

    //@cond
    namespace details {

        template <typename GC, typename T, typename Traits>
        struct make_feldman_hashset
        {
            typedef GC      gc;
            typedef T       value_type;
            typedef Traits  original_traits;

            typedef cds::details::Allocator< value_type, typename original_traits::allocator > cxx_node_allocator;

            struct node_disposer
            {
                void operator()( value_type * p ) const
                {
                    cxx_node_allocator().Delete( p );
                }
            };

            struct intrusive_traits: public original_traits
            {
                typedef node_disposer disposer;
            };

            // Metafunction result
            typedef cds::intrusive::FeldmanHashSet< GC, T, intrusive_traits > type;
        };
    } // namespace details
    //@endcond

}} // namespace cds::container

#endif // #ifndef CDSLIB_CONTAINER_DETAILS_FELDMAN_HASHSET_BASE_H
