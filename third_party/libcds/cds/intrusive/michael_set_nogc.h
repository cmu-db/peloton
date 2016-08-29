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

#ifndef CDSLIB_INTRUSIVE_MICHAEL_SET_NOGC_H
#define CDSLIB_INTRUSIVE_MICHAEL_SET_NOGC_H

#include <cds/intrusive/details/michael_set_base.h>
#include <cds/gc/nogc.h>
#include <cds/details/allocator.h>

namespace cds { namespace intrusive {

    /// Michael's hash set (template specialization for gc::nogc)
    /** @ingroup cds_intrusive_map
        \anchor cds_intrusive_MichaelHashSet_nogc

        This specialization is so-called append-only when no item
        reclamation may be performed. The set does not support deleting of list item.

        See \ref cds_intrusive_MichaelHashSet_hp "MichaelHashSet" for description of template parameters.
        The template parameter \p OrderedList should be any \p cds::gc::nogc -derived ordered list, for example,
        \ref cds_intrusive_MichaelList_nogc "append-only MichaelList".
    */
    template <
        class OrderedList,
#ifdef CDS_DOXYGEN_INVOKED
        class Traits = michael_set::traits
#else
        class Traits
#endif
    >
    class MichaelHashSet< cds::gc::nogc, OrderedList, Traits >
    {
    public:
        typedef cds::gc::nogc gc;        ///< Garbage collector
        typedef OrderedList bucket_type; ///< Type of ordered list to be used as buckets
        typedef Traits      traits;     ///< Set traits

        typedef typename bucket_type::value_type     value_type;     ///< type of value to be stored in the set
        typedef typename bucket_type::key_comparator key_comparator; ///< key comparing functor
        typedef typename bucket_type::disposer       disposer;       ///< Node disposer functor

        /// Hash functor for \p value_type and all its derivatives that you use
        typedef typename cds::opt::v::hash_selector< typename traits::hash >::type hash;
        typedef typename traits::item_counter item_counter; ///< Item counter type

        /// Bucket table allocator
        typedef cds::details::Allocator< bucket_type, typename traits::allocator > bucket_table_allocator;

    protected:
        item_counter    m_ItemCounter; ///< Item counter
        hash            m_HashFunctor; ///< Hash functor
        bucket_type *   m_Buckets;     ///< bucket table

    private:
        //@cond
        const size_t    m_nHashBitmask;
        //@endcond

    protected:
        //@cond
        /// Calculates hash value of \p key
        template <typename Q>
        size_t hash_value( Q const & key ) const
        {
            return m_HashFunctor( key ) & m_nHashBitmask;
        }

        /// Returns the bucket (ordered list) for \p key
        template <typename Q>
        bucket_type&    bucket( Q const & key )
        {
            return m_Buckets[ hash_value( key ) ];
        }
        //@endcond

    public:
    ///@name Forward iterators
    //@{
        /// Forward iterator
        /**
            The forward iterator for Michael's set is based on \p OrderedList forward iterator and has some features:
            - it has no post-increment operator
            - it iterates items in unordered fashion

            The iterator interface:
            \code
            class iterator {
            public:
                // Default constructor
                iterator();

                // Copy construtor
                iterator( iterator const& src );

                // Dereference operator
                value_type * operator ->() const;

                // Dereference operator
                value_type& operator *() const;

                // Preincrement operator
                iterator& operator ++();

                // Assignment operator
                iterator& operator = (iterator const& src);

                // Equality operators
                bool operator ==(iterator const& i ) const;
                bool operator !=(iterator const& i ) const;
            };
            \endcode
        */
        typedef michael_set::details::iterator< bucket_type, false >    iterator;

        /// Const forward iterator
        /**
            For iterator's features and requirements see \ref iterator
        */
        typedef michael_set::details::iterator< bucket_type, true >     const_iterator;

        /// Returns a forward iterator addressing the first element in a set
        /**
            For empty set \code begin() == end() \endcode
        */
        iterator begin()
        {
            return iterator( m_Buckets[0].begin(), m_Buckets, m_Buckets + bucket_count() );
        }

        /// Returns an iterator that addresses the location succeeding the last element in a set
        /**
            Do not use the value returned by <tt>end</tt> function to access any item.
            The returned value can be used only to control reaching the end of the set.
            For empty set \code begin() == end() \endcode
        */
        iterator end()
        {
            return iterator( m_Buckets[bucket_count() - 1].end(), m_Buckets + bucket_count() - 1, m_Buckets + bucket_count() );
        }

        /// Returns a forward const iterator addressing the first element in a set
        const_iterator begin() const
        {
            return cbegin();
        }
        /// Returns a forward const iterator addressing the first element in a set
        const_iterator cbegin() const
        {
            return const_iterator( m_Buckets[0].cbegin(), m_Buckets, m_Buckets + bucket_count() );
        }

        /// Returns an const iterator that addresses the location succeeding the last element in a set
        const_iterator end() const
        {
            return cend();
        }
        /// Returns an const iterator that addresses the location succeeding the last element in a set
        const_iterator cend() const
        {
            return const_iterator( m_Buckets[bucket_count() - 1].cend(), m_Buckets + bucket_count() - 1, m_Buckets + bucket_count() );
        }
    //@}

    public:
        /// Initializes hash set
        /**
            The Michael's hash set is an unbounded container, but its hash table is non-expandable.
            At construction time you should pass estimated maximum item count and a load factor.
            The load factor is average size of one bucket - a small number between 1 and 10.
            The bucket is an ordered single-linked list, searching in the bucket has linear complexity <tt>O(nLoadFactor)</tt>.
            The constructor defines hash table size as rounding <tt>nMaxItemCount / nLoadFactor</tt> up to nearest power of two.
        */
        MichaelHashSet(
            size_t nMaxItemCount,   ///< estimation of max item count in the hash set
            size_t nLoadFactor      ///< load factor: estimation of max number of items in the bucket
        ) : m_nHashBitmask( michael_set::details::init_hash_bitmask( nMaxItemCount, nLoadFactor ))
        {
            // GC and OrderedList::gc must be the same
            static_assert( std::is_same<gc, typename bucket_type::gc>::value, "GC and OrderedList::gc must be the same");

            // atomicity::empty_item_counter is not allowed as a item counter
            static_assert( !std::is_same<item_counter, atomicity::empty_item_counter>::value,
                           "atomicity::empty_item_counter is not allowed as a item counter");

            m_Buckets = bucket_table_allocator().NewArray( bucket_count() );
        }

        /// Clears hash set object and destroys it
        ~MichaelHashSet()
        {
            clear();
            bucket_table_allocator().Delete( m_Buckets, bucket_count() );
        }

        /// Inserts new node
        /**
            The function inserts \p val in the set if it does not contain
            an item with key equal to \p val.

            Returns \p true if \p val is placed into the set, \p false otherwise.
        */
        bool insert( value_type& val )
        {
            bool bRet = bucket( val ).insert( val );
            if ( bRet )
                ++m_ItemCounter;
            return bRet;
        }

        /// Updates the element
        /**
            The operation performs inserting or changing data with lock-free manner.

            If the item \p val not found in the set, then \p val is inserted iff \p bAllowInsert is \p true.
            Otherwise, the functor \p func is called with item found.
            The functor signature is:
            \code
                struct functor {
                    void operator()( bool bNew, value_type& item, value_type& val );
                };
            \endcode
            with arguments:
            - \p bNew - \p true if the item has been inserted, \p false otherwise
            - \p item - item of the set
            - \p val - argument \p val passed into the \p %update() function
            If new item has been inserted (i.e. \p bNew is \p true) then \p item and \p val arguments
            refers to the same thing.

            The functor may change non-key fields of the \p item.

            Returns <tt> std::pair<bool, bool> </tt> where \p first is \p true if operation is successfull,
            \p second is \p true if new item has been added or \p false if the item with \p key
            already is in the set.

            @warning For \ref cds_intrusive_MichaelList_hp "MichaelList" as the bucket see \ref cds_intrusive_item_creating "insert item troubleshooting".
            \ref cds_intrusive_LazyList_hp "LazyList" provides exclusive access to inserted item and does not require any node-level
            synchronization.
        */
        template <typename Func>
        std::pair<bool, bool> update( value_type& val, Func func, bool bAllowInsert = true )
        {
            std::pair<bool, bool> bRet = bucket( val ).update( val, func, bAllowInsert );
            if ( bRet.second )
                ++m_ItemCounter;
            return bRet;
        }
        //@cond
        template <typename Func>
        CDS_DEPRECATED("ensure() is deprecated, use update()")
        std::pair<bool, bool> ensure( value_type& val, Func func )
        {
            return update( val, func, true );
        }
        //@endcond

        /// Checks whether the set contains \p key
        /**

            The function searches the item with key equal to \p key
            and returns the pointer to an element found or \p nullptr.

            Note the hash functor specified for class \p Traits template parameter
            should accept a parameter of type \p Q that can be not the same as \p value_type.
        */
        template <typename Q>
        value_type * contains( Q const& key )
        {
            return bucket( key ).contains( key );
        }
        //@cond
        template <typename Q>
        CDS_DEPRECATED("use contains()")
        value_type * find( Q const& key )
        {
            return contains( key );
        }
        //@endcond

        /// Checks whether the set contains \p key using \p pred predicate for searching
        /**
            The function is an analog of <tt>contains( key )</tt> but \p pred is used for key comparing.
            \p Less functor has the interface like \p std::less.
            \p Less must imply the same element order as the comparator used for building the set.
        */
        template <typename Q, typename Less>
        value_type * contains( Q const& key, Less pred )
        {
            return bucket( key ).contains( key, pred );
        }
        //@cond
        template <typename Q, typename Less>
        CDS_DEPRECATED("use contains()")
        value_type * find_with( Q const& key, Less pred )
        {
            return contains( key, pred );
        }
        //@endcond

        /// Finds the key \p key
        /** \anchor cds_intrusive_MichaelHashSet_nogc_find_func
            The function searches the item with key equal to \p key and calls the functor \p f for item found.
            The interface of \p Func functor is:
            \code
            struct functor {
                void operator()( value_type& item, Q& key );
            };
            \endcode
            where \p item is the item found, \p key is the <tt>find</tt> function argument.

            The functor can change non-key fields of \p item.
            The functor does not serialize simultaneous access to the set \p item. If such access is
            possible you must provide your own synchronization schema on item level to exclude unsafe item modifications.

            The \p key argument is non-const since it can be used as \p f functor destination i.e., the functor
            can modify both arguments.

            Note the hash functor specified for class \p Traits template parameter
            should accept a parameter of type \p Q that can be not the same as \p value_type.

            The function returns \p true if \p key is found, \p false otherwise.
        */
        template <typename Q, typename Func>
        bool find( Q& key, Func f )
        {
            return bucket( key ).find( key, f );
        }
        //@cond
        template <typename Q, typename Func>
        bool find( Q const& key, Func f )
        {
            return bucket( key ).find( key, f );
        }
        //@endcond

        /// Finds the key \p key using \p pred predicate for searching
        /**
            The function is an analog of \ref cds_intrusive_MichaelHashSet_nogc_find_func "find(Q&, Func)"
            but \p pred is used for key comparing.
            \p Less functor has the interface like \p std::less.
            \p pred must imply the same element order as the comparator used for building the set.
        */
        template <typename Q, typename Less, typename Func>
        bool find_with( Q& key, Less pred, Func f )
        {
            return bucket( key ).find_with( key, pred, f );
        }
        //@cond
        template <typename Q, typename Less, typename Func>
        bool find_with( Q const& key, Less pred, Func f )
        {
            return bucket( key ).find_with( key, pred, f );
        }
        //@endcond

        /// Clears the set (non-atomic)
        /**
            The function unlink all items from the set.
            The function is not atomic. It cleans up each bucket and then resets the item counter to zero.
            If there are a thread that performs insertion while \p %clear() is working the result is undefined in general case:
            <tt> empty() </tt> may return \p true but the set may contain item(s).
            Therefore, \p %clear() may be used only for debugging purposes.

            For each item the \p disposer is called after unlinking.
        */
        void clear()
        {
            for ( size_t i = 0; i < bucket_count(); ++i )
                m_Buckets[i].clear();
            m_ItemCounter.reset();
        }


        /// Checks if the set is empty
        /**
            Emptiness is checked by item counting: if item count is zero then the set is empty.
            Thus, the correct item counting feature is an important part of Michael's set implementation.
        */
        bool empty() const
        {
            return size() == 0;
        }

        /// Returns item count in the set
        size_t size() const
        {
            return m_ItemCounter;
        }

        /// Returns the size of hash table
        /**
            Since \p %MichaelHashSet cannot dynamically extend the hash table size,
            the value returned is an constant depending on object initialization parameters;
            see MichaelHashSet::MichaelHashSet for explanation.
        */
        size_t bucket_count() const
        {
            return m_nHashBitmask + 1;
        }

    };

}} // namespace cds::intrusive

#endif // #ifndef CDSLIB_INTRUSIVE_MICHAEL_SET_NOGC_H

