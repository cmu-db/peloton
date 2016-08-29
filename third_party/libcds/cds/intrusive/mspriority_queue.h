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

#ifndef CDSLIB_INTRUSIVE_MSPRIORITY_QUEUE_H
#define CDSLIB_INTRUSIVE_MSPRIORITY_QUEUE_H

#include <mutex>  // std::unique_lock
#include <cds/intrusive/details/base.h>
#include <cds/sync/spinlock.h>
#include <cds/os/thread.h>
#include <cds/details/bit_reverse_counter.h>
#include <cds/intrusive/options.h>
#include <cds/opt/buffer.h>
#include <cds/opt/compare.h>
#include <cds/details/bounded_container.h>

namespace cds { namespace intrusive {

    /// MSPriorityQueue related definitions
    /** @ingroup cds_intrusive_helper
    */
    namespace mspriority_queue {

        /// MSPriorityQueue statistics
        template <typename Counter = cds::atomicity::event_counter>
        struct stat {
            typedef Counter   event_counter ; ///< Event counter type

            event_counter   m_nPushCount            ;   ///< Count of success push operation
            event_counter   m_nPopCount             ;   ///< Count of success pop operation
            event_counter   m_nPushFailCount        ;   ///< Count of failed ("the queue is full") push operation
            event_counter   m_nPopFailCount         ;   ///< Count of failed ("the queue is empty") pop operation
            event_counter   m_nPushHeapifySwapCount ;   ///< Count of item swapping when heapifying in push
            event_counter   m_nPopHeapifySwapCount  ;   ///< Count of item swapping when heapifying in pop

            //@cond
            void onPushSuccess()            { ++m_nPushCount            ;}
            void onPopSuccess()             { ++m_nPopCount             ;}
            void onPushFailed()             { ++m_nPushFailCount        ;}
            void onPopFailed()              { ++m_nPopFailCount         ;}
            void onPushHeapifySwap()        { ++m_nPushHeapifySwapCount ;}
            void onPopHeapifySwap()         { ++m_nPopHeapifySwapCount  ;}
            //@endcond
        };

        /// MSPriorityQueue empty statistics
        struct empty_stat {
            //@cond
            void onPushSuccess()            {}
            void onPopSuccess()             {}
            void onPushFailed()             {}
            void onPopFailed()              {}
            void onPushHeapifySwap()        {}
            void onPopHeapifySwap()         {}
            //@endcond
        };

        /// MSPriorityQueue traits
        struct traits {
            /// Storage type
            /**
                The storage type for the heap array. Default is \p cds::opt::v::initialized_dynamic_buffer.

                You may specify any type of buffer's value since at instantiation time
                the \p buffer::rebind member metafunction is called to change type
                of values stored in the buffer.
            */
            typedef opt::v::initialized_dynamic_buffer<void *>  buffer;

            /// Priority compare functor
            /**
                No default functor is provided. If the option is not specified, the \p less is used.
            */
            typedef opt::none       compare;

            /// Specifies binary predicate used for priority comparing.
            /**
                Default is \p std::less<T>.
            */
            typedef opt::none       less;

            /// Type of mutual-exclusion lock
            typedef cds::sync::spin lock_type;

            /// Back-off strategy
            typedef backoff::yield      back_off;

            /// Internal statistics
            /**
                Possible types: \p mspriority_queue::empty_stat (the default, no overhead), \p mspriority_queue::stat
                or any other with interface like \p %mspriority_queue::stat
            */
            typedef empty_stat      stat;
        };

        /// Metafunction converting option list to traits
        /**
            \p Options:
            - \p opt::buffer - the buffer type for heap array. Possible type are: \p opt::v::initialized_static_buffer, \p opt::v::initialized_dynamic_buffer.
                Default is \p %opt::v::initialized_dynamic_buffer.
                You may specify any type of value for the buffer since at instantiation time
                the \p buffer::rebind member metafunction is called to change the type of values stored in the buffer.
            - \p opt::compare - priority compare functor. No default functor is provided.
                If the option is not specified, the \p opt::less is used.
            - \p opt::less - specifies binary predicate used for priority compare. Default is \p std::less<T>.
            - \p opt::lock_type - lock type. Default is \p cds::sync::spin
            - \p opt::back_off - back-off strategy. Default is \p cds::backoff::yield
            - \p opt::stat - internal statistics. Available types: \p mspriority_queue::stat, \p mspriority_queue::empty_stat (the default, no overhead)
        */
        template <typename... Options>
        struct make_traits {
#   ifdef CDS_DOXYGEN_INVOKED
            typedef implementation_defined type ;   ///< Metafunction result
#   else
            typedef typename cds::opt::make_options<
                typename cds::opt::find_type_traits< traits, Options... >::type
                ,Options...
            >::type   type;
#   endif
        };

    }   // namespace mspriority_queue

    /// Michael & Scott array-based lock-based concurrent priority queue heap
    /** @ingroup cds_intrusive_priority_queue
        Source:
            - [1996] G.Hunt, M.Michael, S. Parthasarathy, M.Scott
                "An efficient algorithm for concurrent priority queue heaps"

        \p %MSPriorityQueue augments the standard array-based heap data structure with
        a mutual-exclusion lock on the heap's size and locks on each node in the heap.
        Each node also has a tag that indicates whether
        it is empty, valid, or in a transient state due to an update to the heap
        by an inserting thread.
        The algorithm allows concurrent insertions and deletions in opposite directions,
        without risking deadlock and without the need for special server threads.
        It also uses a "bit-reversal" technique to scatter accesses across the fringe
        of the tree to reduce contention.
        On large heaps the algorithm achieves significant performance improvements
        over serialized single-lock algorithm, for various insertion/deletion
        workloads. For small heaps it still performs well, but not as well as
        single-lock algorithm.

        Template parameters:
        - \p T - type to be stored in the queue. The priority is a part of \p T type.
        - \p Traits - type traits. See \p mspriority_queue::traits for explanation.
            It is possible to declare option-based queue with \p cds::container::mspriority_queue::make_traits
            metafunction instead of \p Traits template argument.
    */
    template <typename T, class Traits = mspriority_queue::traits >
    class MSPriorityQueue: public cds::bounded_container
    {
    public:
        typedef T           value_type  ;   ///< Value type stored in the queue
        typedef Traits      traits      ;   ///< Traits template parameter

#   ifdef CDS_DOXYGEN_INVOKED
        typedef implementation_defined key_comparator  ;    ///< priority comparing functor based on opt::compare and opt::less option setter.
#   else
        typedef typename opt::details::make_comparator< value_type, traits >::type key_comparator;
#   endif

        typedef typename traits::lock_type lock_type;   ///< heap's size lock type
        typedef typename traits::back_off  back_off;    ///< Back-off strategy
        typedef typename traits::stat      stat;        ///< internal statistics type

    protected:
        //@cond
        typedef cds::OS::ThreadId   tag_type;

        enum tag_value {
            Available   = -1,
            Empty       = 0
        };
        //@endcond

        //@cond
        /// Heap item type
        struct node {
            value_type *        m_pVal  ;   ///< A value pointer
            tag_type volatile   m_nTag  ;   ///< A tag
            mutable lock_type   m_Lock  ;   ///< Node-level lock

            /// Creates empty node
            node()
                : m_pVal( nullptr )
                , m_nTag( tag_type(Empty) )
            {}

            /// Lock the node
            void lock()
            {
                m_Lock.lock();
            }

            /// Unlock the node
            void unlock()
            {
                m_Lock.unlock();
            }
        };
        //@endcond

    public:
        typedef typename traits::buffer::template rebind<node>::other   buffer_type ;   ///< Heap array buffer type

        //@cond
        typedef cds::bitop::bit_reverse_counter<>           item_counter_type;
        typedef typename item_counter_type::counter_type    counter_type;
        //@endcond

    protected:
        item_counter_type   m_ItemCounter   ;   ///< Item counter
        mutable lock_type   m_Lock          ;   ///< Heap's size lock
        buffer_type         m_Heap          ;   ///< Heap array
        stat                m_Stat          ;   ///< internal statistics accumulator

    public:
        /// Constructs empty priority queue
        /**
            For \p cds::opt::v::initialized_static_buffer the \p nCapacity parameter is ignored.
        */
        MSPriorityQueue( size_t nCapacity )
            : m_Heap( nCapacity )
        {}

        /// Clears priority queue and destructs the object
        ~MSPriorityQueue()
        {
            clear();
        }

        /// Inserts a item into priority queue
        /**
            If the priority queue is full, the function returns \p false,
            no item has been added.
            Otherwise, the function inserts the pointer to \p val into the heap
            and returns \p true.

            The function does not make a copy of \p val.
        */
        bool push( value_type& val )
        {
            tag_type const curId = cds::OS::get_current_thread_id();

            // Insert new item at bottom of the heap
            m_Lock.lock();
            if ( m_ItemCounter.value() >= capacity() ) {
                // the heap is full
                m_Lock.unlock();
                m_Stat.onPushFailed();
                return false;
            }

            counter_type i = m_ItemCounter.inc();
            assert( i < m_Heap.capacity() );

            node& refNode = m_Heap[i];
            refNode.lock();
            m_Lock.unlock();
            refNode.m_pVal = &val;
            refNode.m_nTag = curId;
            refNode.unlock();

            // Move item towards top of the heap while it has higher priority than parent
            heapify_after_push( i, curId );

            m_Stat.onPushSuccess();
            return true;
        }

        /// Extracts item with high priority
        /**
            If the priority queue is empty, the function returns \p nullptr.
            Otherwise, it returns the item extracted.
        */
        value_type * pop()
        {
            m_Lock.lock();
            if ( m_ItemCounter.value() == 0 ) {
                // the heap is empty
                m_Lock.unlock();
                m_Stat.onPopFailed();
                return nullptr;
            }
            counter_type nBottom = m_ItemCounter.reversed_value();
            m_ItemCounter.dec();
            // Since m_Heap[0] is not used, capacity() returns m_Heap.capacity() - 1
            // Consequently, "<=" is here
            assert( nBottom <= capacity() );
            assert( nBottom > 0 );

            node& refBottom = m_Heap[ nBottom ];
            refBottom.lock();
            m_Lock.unlock();
            refBottom.m_nTag = tag_type(Empty);
            value_type * pVal = refBottom.m_pVal;
            refBottom.m_pVal = nullptr;
            refBottom.unlock();

            node& refTop = m_Heap[ 1 ];
            refTop.lock();
            if ( refTop.m_nTag == tag_type(Empty) ) {
                // nBottom == nTop
                refTop.unlock();
                m_Stat.onPopSuccess();
                return pVal;
            }

            std::swap( refTop.m_pVal, pVal );
            refTop.m_nTag = tag_type( Available );

            // refTop will be unlocked inside heapify_after_pop
            heapify_after_pop( 1, &refTop );

            m_Stat.onPopSuccess();
            return pVal;
        }

        /// Clears the queue (not atomic)
        /**
            This function is no atomic, but thread-safe
        */
        void clear()
        {
            clear_with( []( value_type const& /*src*/ ) {} );
        }

        /// Clears the queue (not atomic)
        /**
            This function is no atomic, but thread-safe.

            For each item removed the functor \p f is called.
            \p Func interface is:
            \code
                struct clear_functor
                {
                    void operator()( value_type& item );
                };
            \endcode
            A lambda function or a function pointer can be used as \p f.
        */
        template <typename Func>
        void clear_with( Func f )
        {
            while ( !empty() ) {
                value_type * pVal = pop();
                if ( pVal )
                    f( *pVal );
            }
        }

        /// Checks is the priority queue is empty
        bool empty() const
        {
            return size() == 0;
        }

        /// Checks if the priority queue is full
        bool full() const
        {
            return size() == capacity();
        }

        /// Returns current size of priority queue
        size_t size() const
        {
            std::unique_lock<lock_type> l( m_Lock );
            size_t nSize = (size_t) m_ItemCounter.value();
            return nSize;
        }

        /// Return capacity of the priority queue
        size_t capacity() const
        {
            // m_Heap[0] is not used
            return m_Heap.capacity() - 1;
        }

        /// Returns const reference to internal statistics
        stat const& statistics() const
        {
            return m_Stat;
        }

    protected:
        //@cond

        void heapify_after_push( counter_type i, tag_type curId )
        {
            key_comparator  cmp;
            back_off        bkoff;

            // Move item towards top of the heap while it has higher priority than parent
            while ( i > 1 ) {
                bool bProgress = true;
                counter_type nParent = i / 2;
                node& refParent = m_Heap[nParent];
                refParent.lock();
                node& refItem = m_Heap[i];
                refItem.lock();

                if ( refParent.m_nTag == tag_type(Available) && refItem.m_nTag == curId ) {
                    if ( cmp( *refItem.m_pVal, *refParent.m_pVal ) > 0 ) {
                        std::swap( refItem.m_nTag, refParent.m_nTag );
                        std::swap( refItem.m_pVal, refParent.m_pVal );
                        m_Stat.onPushHeapifySwap();
                        i = nParent;
                    }
                    else {
                        refItem.m_nTag = tag_type(Available);
                        i = 0;
                    }
                }
                else if ( refParent.m_nTag == tag_type(Empty) )
                    i = 0;
                else if ( refItem.m_nTag != curId )
                    i = nParent;
                else
                    bProgress = false;

                refItem.unlock();
                refParent.unlock();

                if ( !bProgress )
                    bkoff();
                else
                    bkoff.reset();
            }

            if ( i == 1 ) {
                node& refItem = m_Heap[i];
                refItem.lock();
                if ( refItem.m_nTag == curId )
                    refItem.m_nTag = tag_type(Available);
                refItem.unlock();
            }
        }

        void heapify_after_pop( counter_type nParent, node * pParent )
        {
            key_comparator cmp;

            while ( nParent < m_Heap.capacity() / 2 ) {
                counter_type nLeft = nParent * 2;
                counter_type nRight = nLeft + 1;
                node& refLeft = m_Heap[nLeft];
                node& refRight = m_Heap[nRight];
                refLeft.lock();
                refRight.lock();

                counter_type nChild;
                node * pChild;
                if ( refLeft.m_nTag == tag_type(Empty) ) {
                    refRight.unlock();
                    refLeft.unlock();
                    break;
                }
                else if ( refRight.m_nTag == tag_type(Empty) || cmp( *refLeft.m_pVal, *refRight.m_pVal ) > 0 ) {
                    refRight.unlock();
                    nChild = nLeft;
                    pChild = &refLeft;
                }
                else {
                    refLeft.unlock();
                    nChild = nRight;
                    pChild = &refRight;
                }

                // If child has higher priority that parent then swap
                // Otherwise stop
                if ( cmp( *pChild->m_pVal, *pParent->m_pVal ) > 0 ) {
                    std::swap( pParent->m_nTag, pChild->m_nTag );
                    std::swap( pParent->m_pVal, pChild->m_pVal );
                    pParent->unlock();
                    m_Stat.onPopHeapifySwap();
                    nParent = nChild;
                    pParent = pChild;
                }
                else {
                    pChild->unlock();
                    break;
                }
            }
            pParent->unlock();
        }
        //@endcond
    };

}} // namespace cds::intrusive

#endif // #ifndef CDSLIB_INTRUSIVE_MSPRIORITY_QUEUE_H
