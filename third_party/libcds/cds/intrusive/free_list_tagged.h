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

#ifndef CDSLIB_INTRUSIVE_FREE_LIST_TAGGED_H
#define CDSLIB_INTRUSIVE_FREE_LIST_TAGGED_H

#include <cds/algo/atomic.h>

namespace cds { namespace intrusive {

    /// Lock-free free list based on tagged pointers (required double-width CAS)
    /** @ingroup cds_intrusive_helper
        This variant of \p FreeList is intended for processor architectures that support double-width CAS.
        It uses <a href="https://en.wikipedia.org/wiki/Tagged_pointer">tagged pointer</a> technique to solve ABA problem.

        \b How to use
        \code
        #include <cds/intrusive/free_list_tagged.h>

        // Your struct should be derived from TaggedFreeList::node
        struct Foo: public cds::intrusive::TaggedFreeList::node
        {
            // Foo fields
        };

        // Simplified Foo allocator
        class FooAllocator
        {
        public:
            // free-list clear() must be explicitly called before destroying the free-list object
            ~FooAllocator()
            {
                m_FreeList.clear( []( freelist_node * p ) { delete static_cast<Foo *>( p ); });
            }

            Foo * alloc()
            {
                freelist_node * p = m_FreeList.get();
                if ( p )
                    return static_cast<Foo *>( p );
                return new Foo;
            };

            void dealloc( Foo * p )
            {
                m_FreeList.put( static_cast<freelist_node *>( p ));
            };

        private:
            typedef cds::intrusive::TaggedFreeList::node freelist_node;
            cds::intrusive::TaggedFreeList m_FreeList;
        };
        \endcode
    */
    class TaggedFreeList
    {
    public:
        struct node {
            //@cond
            atomics::atomic<node *> m_freeListNext;

            node()
                : m_freeListNext( nullptr )
            {}
            //@endcond
        };

    public:
        /// Creates empty free-list
        TaggedFreeList()
        {
            // Your platform must support double-width CAS
            assert( m_Head.is_lock_free());
        }

        /// Destroys the free list. Free-list must be empty.
        /**
            @warning dtor does not free elements of the list.
            To free elements you should manually call \p clear() with an appropriate disposer.
        */
        ~TaggedFreeList()
        {
            assert( empty() );
        }


        /// Puts \p pNode to the free list
        void put( node * pNode )
        {
            tagged_ptr currentHead = m_Head.load( atomics::memory_order_relaxed );
            tagged_ptr newHead = { pNode };
            do {
                newHead.tag = currentHead.tag + 1;
                pNode->m_freeListNext.store( currentHead.ptr, atomics::memory_order_relaxed );
            } while ( !m_Head.compare_exchange_weak( currentHead, newHead, atomics::memory_order_release, atomics::memory_order_relaxed ));
        }

        /// Gets a node from the free list. If the list is empty, returns \p nullptr
        node * get()
        {
            tagged_ptr currentHead = m_Head.load( atomics::memory_order_acquire );
            tagged_ptr newHead;
            while ( currentHead.ptr != nullptr ) {
                newHead.ptr = currentHead.ptr->m_freeListNext.load( atomics::memory_order_relaxed );
                newHead.tag = currentHead.tag + 1;
                if ( m_Head.compare_exchange_weak( currentHead, newHead, atomics::memory_order_release, atomics::memory_order_acquire ) )
                    break;
            }
            return currentHead.ptr;
        }

        /// Checks whether the free list is empty
        bool empty() const
        {
            return m_Head.load( atomics::memory_order_relaxed ).ptr == nullptr;
        }

        /// Clears the free list (not atomic)
        /**
            For each element \p disp disposer is called to free memory.
            The \p Disposer interface:
            \code
            struct disposer
            {
                void operator()( FreeList::node * node );
            };
            \endcode

            This method must be explicitly called before the free list destructor.
        */
        template <typename Disposer>
        void clear( Disposer disp )
        {
            node * head = m_Head.load( atomics::memory_order_relaxed ).ptr;
            m_Head.store( { nullptr }, atomics::memory_order_relaxed );
            while ( head ) {
                node * next = head->m_freeListNext.load( atomics::memory_order_relaxed );
                disp( head );
                head = next;
            }
        }

    private:
        //@cond
        struct tagged_ptr
        {
            node *    ptr;
            uintptr_t tag;

            tagged_ptr()
                : ptr( nullptr )
                , tag( 0 )
            {}
        };

        static_assert(sizeof( tagged_ptr ) == sizeof(void *) * 2, "sizeof( tagged_ptr ) violation" );

        atomics::atomic<tagged_ptr> m_Head;
        //@endcond    
    };

}} // namespace cds::intrusive

#endif // CDSLIB_INTRUSIVE_FREE_LIST_TAGGED_H