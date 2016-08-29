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

#ifndef CDSLIB_CONTAINER_IMPL_LAZY_LIST_H
#define CDSLIB_CONTAINER_IMPL_LAZY_LIST_H

#include <memory>
#include <cds/container/details/guarded_ptr_cast.h>

namespace cds { namespace container {

    /// Lazy ordered list
    /** @ingroup cds_nonintrusive_list
        @anchor cds_nonintrusive_LazyList_gc

        Usually, ordered single-linked list is used as a building block for the hash table implementation.
        The complexity of searching is <tt>O(N)</tt>.

        Source:
        - [2005] Steve Heller, Maurice Herlihy, Victor Luchangco, Mark Moir, William N. Scherer III, and Nir Shavit
                 "A Lazy Concurrent List-Based Set Algorithm"

        The lazy list is based on an optimistic locking scheme for inserts and removes,
        eliminating the need to use the equivalent of an atomically markable
        reference. It also has a novel wait-free membership \p find() operation
        that does not need to perform cleanup operations and is more efficient.

        It is non-intrusive version of \p cds::intrusive::LazyList class.

        Template arguments:
        - \p GC - garbage collector: \p gc::HP, \p gp::DHP
        - \p T - type to be stored in the list.
        - \p Traits - type traits, default is \p lazy_list::traits.
            It is possible to declare option-based list with \p lazy_list::make_traits metafunction istead of \p Traits template
            argument. For example, the following traits-based declaration of \p gc::HP lazy list
            \code
            #include <cds/container/lazy_list_hp.h>
            // Declare comparator for the item
            struct my_compare {
                int operator ()( int i1, int i2 )
                {
                    return i1 - i2;
                }
            };

            // Declare traits
            struct my_traits: public cds::container::lazy_list::traits
            {
                typedef my_compare compare;
            };

            // Declare traits-based list
            typedef cds::container::LazyList< cds::gc::HP, int, my_traits > traits_based_list;
            \endcode
            is equal to  the following option-based list:
            \code
            #include <cds/container/lazy_list_hp.h>

            // my_compare is the same

            // Declare option-based list
            typedef cds::container::LazyList< cds::gc::HP, int,
                typename cds::container::lazy_list::make_traits<
                    cds::container::opt::compare< my_compare >     // item comparator option
                >::type
            >     option_based_list;
            \endcode

        Unlike standard container, this implementation does not divide type \p T into key and value part and
        may be used as main building block for hash set algorithms.

        The key is a function (or a part) of type \p T, and the comparing function is specified by \p Traits::compare functor
        or \p Traits::less predicate.

        \p LazyKVList is a key-value version of lazy non-intrusive list that is closer to the C++ std library approach.

        \par Usage
        There are different specializations of this template for each garbage collecting schema used.
        You should include appropriate .h-file depending on GC you are using:
        - for gc::HP: <tt> <cds/container/lazy_list_hp.h> </tt>
        - for gc::DHP: <tt> <cds/container/lazy_list_dhp.h> </tt>
        - for \ref cds_urcu_desc "RCU": <tt> <cds/container/lazy_list_rcu.h> </tt>
        - for gc::nogc: <tt> <cds/container/lazy_list_nogc.h> </tt>
    */
    template <
        typename GC,
        typename T,
#ifdef CDS_DOXYGEN_INVOKED
        typename Traits = lazy_list::traits
#else
        typename Traits
#endif
    >
    class LazyList:
#ifdef CDS_DOXYGEN_INVOKED
        protected intrusive::LazyList< GC, T, Traits >
#else
        protected details::make_lazy_list< GC, T, Traits >::type
#endif
    {
        //@cond
        typedef details::make_lazy_list< GC, T, Traits > maker;
        typedef typename maker::type  base_class;
        //@endcond

    public:
        typedef GC gc;           ///< Garbage collector used
        typedef T  value_type;   ///< Type of value stored in the list
        typedef Traits traits;   ///< List traits

        typedef typename base_class::back_off     back_off;       ///< Back-off strategy used
        typedef typename maker::allocator_type    allocator_type; ///< Allocator type used for allocate/deallocate the nodes
        typedef typename base_class::item_counter item_counter;   ///< Item counting policy used
        typedef typename maker::key_comparator    key_comparator; ///< key comparison functor
        typedef typename base_class::memory_model memory_model;   ///< Memory ordering. See cds::opt::memory_model option

        static CDS_CONSTEXPR const size_t c_nHazardPtrCount = base_class::c_nHazardPtrCount; ///< Count of hazard pointer required for the algorithm

    protected:
        //@cond
        typedef typename base_class::value_type   node_type;
        typedef typename maker::cxx_allocator     cxx_allocator;
        typedef typename maker::node_deallocator  node_deallocator;
        typedef typename maker::intrusive_traits::compare  intrusive_key_comparator;

        typedef typename base_class::node_type head_type;
        //@endcond

    public:
        /// Guarded pointer
        typedef typename gc::template guarded_ptr< node_type, value_type, details::guarded_ptr_cast_set<node_type, value_type> > guarded_ptr;

    protected:
        //@cond
        static value_type& node_to_value( node_type& n )
        {
            return n.m_Value;
        }

        static value_type const& node_to_value( node_type const& n )
        {
            return n.m_Value;
        }

        template <typename Q>
        static node_type * alloc_node( Q const& v )
        {
            return cxx_allocator().New( v );
        }

        template <typename... Args>
        static node_type * alloc_node( Args&&... args )
        {
            return cxx_allocator().MoveNew( std::forward<Args>(args)... );
        }

        static void free_node( node_type * pNode )
        {
            cxx_allocator().Delete( pNode );
        }

        struct node_disposer {
            void operator()( node_type * pNode )
            {
                free_node( pNode );
            }
        };
        typedef std::unique_ptr< node_type, node_disposer >     scoped_node_ptr;

        head_type& head()
        {
            return base_class::m_Head;
        }

        head_type const& head() const
        {
            return base_class::m_Head;
        }

        head_type& tail()
        {
            return base_class::m_Tail;
        }

        head_type const&  tail() const
        {
            return base_class::m_Tail;
        }
        //@endcond

    protected:
                //@cond
        template <bool IsConst>
        class iterator_type: protected base_class::template iterator_type<IsConst>
        {
            typedef typename base_class::template iterator_type<IsConst>    iterator_base;

            iterator_type( head_type const& pNode )
                : iterator_base( const_cast<head_type *>( &pNode ))
            {}

            iterator_type( head_type const * pNode )
                : iterator_base( const_cast<head_type *>( pNode ))
            {}

            friend class LazyList;

        public:
            typedef typename cds::details::make_const_type<value_type, IsConst>::pointer   value_ptr;
            typedef typename cds::details::make_const_type<value_type, IsConst>::reference value_ref;

            iterator_type()
            {}

            iterator_type( const iterator_type& src )
                : iterator_base( src )
            {}

            value_ptr operator ->() const
            {
                typename iterator_base::value_ptr p = iterator_base::operator ->();
                return p ? &(p->m_Value) : nullptr;
            }

            value_ref operator *() const
            {
                return (iterator_base::operator *()).m_Value;
            }

            /// Pre-increment
            iterator_type& operator ++()
            {
                iterator_base::operator ++();
                return *this;
            }

            template <bool C>
            bool operator ==(iterator_type<C> const& i ) const
            {
                return iterator_base::operator ==(i);
            }
            template <bool C>
            bool operator !=(iterator_type<C> const& i ) const
            {
                return iterator_base::operator !=(i);
            }
        };
        //@endcond

    public:
    ///@name Forward iterators (only for debugging purpose)
    //@{
        /// Forward iterator
        /**
            The forward iterator for lazy list has some features:
            - it has no post-increment operator
            - to protect the value, the iterator contains a GC-specific guard + another guard is required locally for increment operator.
              For some GC (\p gc::HP), a guard is limited resource per thread, so an exception (or assertion) "no free guard"
              may be thrown if a limit of guard count per thread is exceeded.
            - The iterator cannot be moved across thread boundary since it contains GC's guard that is thread-private GC data.
            - Iterator ensures thread-safety even if you delete the item that iterator points to. However, in case of concurrent
              deleting operations it is no guarantee that you iterate all item in the list.
              Moreover, a crash is possible when you try to iterate the next element that has been deleted by concurrent thread.

            @warning Use this iterator on the concurrent container for debugging purpose only.
        */
        typedef iterator_type<false>    iterator;

        /// Const forward iterator
        /**
            For iterator's features and requirements see \ref iterator
        */
        typedef iterator_type<true>     const_iterator;

        /// Returns a forward iterator addressing the first element in a list
        /**
            For empty list \code begin() == end() \endcode
        */
        iterator begin()
        {
            iterator it( head() );
            ++it        ;   // skip dummy head node
            return it;
        }

        /// Returns an iterator that addresses the location succeeding the last element in a list
        /**
            Do not use the value returned by <tt>end</tt> function to access any item.

            The returned value can be used only to control reaching the end of the list.
            For empty list \code begin() == end() \endcode
        */
        iterator end()
        {
            return iterator( tail() );
        }

        /// Returns a forward const iterator addressing the first element in a list
        const_iterator begin() const
        {
            const_iterator it( head() );
            ++it        ;   // skip dummy head node
            return it;
        }

        /// Returns a forward const iterator addressing the first element in a list
        const_iterator cbegin() const
        {
            const_iterator it( head() );
            ++it        ;   // skip dummy head node
            return it;
        }

        /// Returns an const iterator that addresses the location succeeding the last element in a list
        const_iterator end() const
        {
            return const_iterator( tail() );
        }

        /// Returns an const iterator that addresses the location succeeding the last element in a list
        const_iterator cend() const
        {
            return const_iterator( tail() );
        }
    //@}

    public:
        /// Default constructor
        LazyList()
        {}

        /// Destructor clears the list
        ~LazyList()
        {
            clear();
        }

        /// Inserts new node
        /**
            The function creates a node with copy of \p val value
            and then inserts the node created into the list.

            The type \p Q should contain as minimum the complete key of the node.
            The object of \ref value_type should be constructible from \p val of type \p Q.
            In trivial case, \p Q is equal to \ref value_type.

            Returns \p true if inserting successful, \p false otherwise.
        */
        template <typename Q>
        bool insert( Q const& val )
        {
            return insert_at( head(), val );
        }

        /// Inserts new node
        /**
            This function inserts new node with default-constructed value and then it calls
            \p func functor with signature
            \code void func( value_type& item ) ;\endcode

            The argument \p item of user-defined functor \p func is the reference
            to the list's item inserted.
            When \p func is called it has exclusive access to the item.
            The user-defined functor is called only if the inserting is success.

            The type \p Q should contain the complete key of the node.
            The object of \p value_type should be constructible from \p key of type \p Q.

            The function allows to split creating of new item into two part:
            - create item from \p key with initializing key-fields only;
            - insert new item into the list;
            - if inserting is successful, initialize non-key fields of item by calling \p func functor

            This can be useful if complete initialization of object of \p value_type is heavyweight and
            it is preferable that the initialization should be completed only if inserting is successful.
        */
        template <typename Q, typename Func>
        bool insert( Q const& key, Func func )
        {
            return insert_at( head(), key, func );
        }

        /// Inserts data of type \p value_type constructed from \p args
        /**
            Returns \p true if inserting successful, \p false otherwise.
        */
        template <typename... Args>
        bool emplace( Args&&... args )
        {
            return emplace_at( head(), std::forward<Args>(args)... );
        }

        /// Updates data by \p key
        /**
            The operation performs inserting or replacing the element with lock-free manner.

            If the \p key not found in the list, then the new item created from \p key
            will be inserted iff \p bAllowInsert is \p true.
            Otherwise, if \p key is found, the functor \p func is called with item found.

            The functor \p Func signature is:
            \code
                struct my_functor {
                    void operator()( bool bNew, value_type& item, Q const& val );
                };
            \endcode

            with arguments:
            - \p bNew - \p true if the item has been inserted, \p false otherwise
            - \p item - item of the list
            - \p val - argument \p key passed into the \p %update() function

            The functor may change non-key fields of the \p item;
            during \p func call \p item is locked so it is safe to modify the item in
            multi-threaded environment.

            Returns <tt> std::pair<bool, bool> </tt> where \p first is true if operation is successfull,
            \p second is true if new item has been added or \p false if the item with \p key
            already exists.
        */
        template <typename Q, typename Func>
        std::pair<bool, bool> update( Q const& key, Func func, bool bAllowInsert = true )
        {
            return update_at( head(), key, func, bAllowInsert );
        }
        //@cond
        template <typename Q, typename Func>
        CDS_DEPRECATED("ensure() is deprecated, use update()")
        std::pair<bool, bool> ensure( Q const& key, Func f )
        {
            return update( key, f, true );
        }
        //@endcond

        /// Deletes \p key from the list
        /** \anchor cds_nonintrusive_LazyList_hp_erase_val
            Since the key of LazyList's item type \p T is not explicitly specified,
            template parameter \p Q defines the key type searching in the list.
            The list item comparator should be able to compare the type \p T of list item
            and the type \p Q.

            Return \p true if key is found and deleted, \p false otherwise
        */
        template <typename Q>
        bool erase( Q const& key )
        {
            return erase_at( head(), key, intrusive_key_comparator(), [](value_type const&){} );
        }

        /// Deletes the item from the list using \p pred predicate for searching
        /**
            The function is an analog of \ref cds_nonintrusive_LazyList_hp_erase_val "erase(Q const&)"
            but \p pred is used for key comparing.
            \p Less functor has the interface like \p std::less.
            \p pred must imply the same element order as the comparator used for building the list.
        */
        template <typename Q, typename Less>
        bool erase_with( Q const& key, Less pred )
        {
            CDS_UNUSED( pred );
            return erase_at( head(), key, typename maker::template less_wrapper<Less>::type(), [](value_type const&){} );
        }

        /// Deletes \p key from the list
        /** \anchor cds_nonintrusive_LazyList_hp_erase_func
            The function searches an item with key \p key, calls \p f functor with item found
            and deletes the item. If \p key is not found, the functor is not called.

            The functor \p Func interface:
            \code
            struct extractor {
                void operator()(const value_type& val) { ... }
            };
            \endcode

            Since the key of LazyList's item type \p T is not explicitly specified,
            template parameter \p Q defines the key type searching in the list.
            The list item comparator should be able to compare the type \p T of list item
            and the type \p Q.

            Return \p true if key is found and deleted, \p false otherwise

            See also: \ref erase
        */
        template <typename Q, typename Func>
        bool erase( Q const& key, Func f )
        {
            return erase_at( head(), key, intrusive_key_comparator(), f );
        }

        /// Deletes the item from the list using \p pred predicate for searching
        /**
            The function is an analog of \ref cds_nonintrusive_LazyList_hp_erase_func "erase(Q const&, Func)"
            but \p pred is used for key comparing.
            \p Less functor has the interface like \p std::less.
            \p pred must imply the same element order as the comparator used for building the list.
        */
        template <typename Q, typename Less, typename Func>
        bool erase_with( Q const& key, Less pred, Func f )
        {
            CDS_UNUSED( pred );
            return erase_at( head(), key, typename maker::template less_wrapper<Less>::type(), f );
        }

        /// Extracts the item from the list with specified \p key
        /** \anchor cds_nonintrusive_LazyList_hp_extract
            The function searches an item with key equal to \p key,
            unlinks it from the list, and returns it as \p guarded_ptr.
            If \p key is not found the function returns an empty guarded pointer.

            Note the compare functor should accept a parameter of type \p Q that can be not the same as \p value_type.

            @note Each \p guarded_ptr object uses the GC's guard that can be limited resource.

            Usage:
            \code
            typedef cds::container::LazyList< cds::gc::HP, foo, my_traits >  ord_list;
            ord_list theList;
            // ...
            {
                ord_list::guarded_ptr gp(theList.extract( 5 ));
                if ( gp ) {
                    // Deal with gp
                    // ...
                }
                // Destructor of gp releases internal HP guard and frees the item
            }
            \endcode
        */
        template <typename Q>
        guarded_ptr extract( Q const& key )
        {
            guarded_ptr gp;
            extract_at( head(), gp.guard(), key, intrusive_key_comparator() );
            return gp;
        }

        /// Extracts the item from the list with comparing functor \p pred
        /**
            The function is an analog of \ref cds_nonintrusive_LazyList_hp_extract "extract(Q const&)"
            but \p pred predicate is used for key comparing.

            \p Less functor has the semantics like \p std::less but should take arguments of type \ref value_type and \p Q
            in any order.
            \p pred must imply the same element order as the comparator used for building the list.
        */
        template <typename Q, typename Less>
        guarded_ptr extract_with( Q const& key, Less pred )
        {
            CDS_UNUSED( pred );
            guarded_ptr gp;
            extract_at( head(), gp.guard(), key, typename maker::template less_wrapper<Less>::type() );
            return gp;
        }

        /// Checks whether the list contains \p key
        /**
            The function searches the item with key equal to \p key
            and returns \p true if it is found, and \p false otherwise.
        */
        template <typename Q>
        bool contains( Q const& key )
        {
            return find_at( head(), key, intrusive_key_comparator() );
        }
        //@cond
        template <typename Q>
        CDS_DEPRECATED("deprecated, use contains()")
        bool find( Q const& key )
        {
            return contains( key );
        }
        //@endcond

        /// Checks whether the list contains \p key using \p pred predicate for searching
        /**
            The function is an analog of <tt>contains( key )</tt> but \p pred is used for key comparing.
            \p Less functor has the interface like \p std::less.
            \p pred must imply the same element order as the comparator used for building the list.
        */
        template <typename Q, typename Less>
        bool contains( Q const& key, Less pred )
        {
            CDS_UNUSED( pred );
            return find_at( head(), key, typename maker::template less_wrapper<Less>::type() );
        }
        //@cond
        template <typename Q, typename Less>
        CDS_DEPRECATED("deprecated, use contains()")
        bool find_with( Q const& key, Less pred )
        {
            return contains( key, pred );
        }
        //@endcond
        /// Finds the key \p key and performs an action with it
        /** \anchor cds_nonintrusive_LazyList_hp_find_func
            The function searches an item with key equal to \p key and calls the functor \p f for the item found.
            The interface of \p Func functor is:
            \code
            struct functor {
                void operator()( value_type& item, Q& key );
            };
            \endcode
            where \p item is the item found, \p key is the <tt>find</tt> function argument.

            The functor may change non-key fields of \p item. Note that the function is only guarantee
            that \p item cannot be deleted during functor is executing.
            The function does not serialize simultaneous access to the list \p item. If such access is
            possible you must provide your own synchronization schema to exclude unsafe item modifications.

            The \p key argument is non-const since it can be used as \p f functor destination i.e., the functor
            may modify both arguments.

            The function returns \p true if \p key is found, \p false otherwise.
        */
        template <typename Q, typename Func>
        bool find( Q& key, Func f )
        {
            return find_at( head(), key, intrusive_key_comparator(), f );
        }
        //@cond
        template <typename Q, typename Func>
        bool find( Q const& key, Func f )
        {
            return find_at( head(), key, intrusive_key_comparator(), f );
        }
        //@endcond

        /// Finds the key \p key using \p pred predicate for searching
        /**
            The function is an analog of \ref cds_nonintrusive_LazyList_hp_find_func "find(Q&, Func)"
            but \p pred is used for key comparing.
            \p Less functor has the interface like \p std::less.
            \p pred must imply the same element order as the comparator used for building the list.
        */
        template <typename Q, typename Less, typename Func>
        bool find_with( Q& key, Less pred, Func f )
        {
            CDS_UNUSED( pred );
            return find_at( head(), key, typename maker::template less_wrapper<Less>::type(), f );
        }
        //@cond
        template <typename Q, typename Less, typename Func>
        bool find_with( Q const& key, Less pred, Func f )
        {
            CDS_UNUSED( pred );
            return find_at( head(), key, typename maker::template less_wrapper<Less>::type(), f );
        }
        //@endcond

        /// Finds the key \p key and return the item found
        /** \anchor cds_nonintrusive_LazyList_hp_get
            The function searches the item with key equal to \p key
            and returns the item found as \p guarded_ptr.
            If \p key is not found the function returns an empty guarded pointer.

            @note Each \p guarded_ptr object uses one GC's guard which can be limited resource.

            Usage:
            \code
            typedef cds::container::LazyList< cds::gc::HP, foo, my_traits >  ord_list;
            ord_list theList;
            // ...
            {
                ord_list::guarded_ptr gp( theList.get( 5 ));
                if ( gp ) {
                    // Deal with gp
                    //...
                }
                // Destructor of guarded_ptr releases internal HP guard and frees the item
            }
            \endcode

            Note the compare functor specified for class \p Traits template parameter
            should accept a parameter of type \p Q that can be not the same as \p value_type.
        */
        template <typename Q>
        guarded_ptr get( Q const& key )
        {
            guarded_ptr gp;
            get_at( head(), gp.guard(), key, intrusive_key_comparator() );
            return gp;
        }

        /// Finds the key \p key and return the item found
        /**
            The function is an analog of \ref cds_nonintrusive_LazyList_hp_get "get( Q const&)"
            but \p pred is used for comparing the keys.

            \p Less functor has the semantics like \p std::less but should take arguments of type \ref value_type and \p Q
            in any order.
            \p pred must imply the same element order as the comparator used for building the list.
        */
        template <typename Q, typename Less>
        guarded_ptr get_with( Q const& key, Less pred )
        {
            CDS_UNUSED( pred );
            guarded_ptr gp;
            get_at( head(), gp.guard(), key, typename maker::template less_wrapper<Less>::type() );
            return gp;
        }

        /// Checks whether the list is empty
        bool empty() const
        {
            return base_class::empty();
        }

        /// Returns list's item count
        /**
            The value returned depends on \p Traits::item_counter type. For \p atomicity::empty_item_counter,
            this function always returns 0.

            @note Even if you use real item counter and it returns 0, this fact is not mean that the list
            is empty. To check list emptyness use \ref empty() method.
        */
        size_t size() const
        {
            return base_class::size();
        }

        /// Clears the list
        void clear()
        {
            base_class::clear();
        }

    protected:
        //@cond
        bool insert_node( node_type * pNode )
        {
            return insert_node_at( head(), pNode );
        }

        bool insert_node_at( head_type& refHead, node_type * pNode )
        {
            assert( pNode != nullptr );
            scoped_node_ptr p( pNode );

            if ( base_class::insert_at( &refHead, *pNode )) {
                p.release();
                return true;
            }

            return false;
        }

        template <typename Q>
        bool insert_at( head_type& refHead, const Q& val )
        {
            return insert_node_at( refHead, alloc_node( val ));
        }

        template <typename... Args>
        bool emplace_at( head_type& refHead, Args&&... args )
        {
            return insert_node_at( refHead, alloc_node( std::forward<Args>(args)... ));
        }

        template <typename Q, typename Func>
        bool insert_at( head_type& refHead, const Q& key, Func f )
        {
            scoped_node_ptr pNode( alloc_node( key ));

            if ( base_class::insert_at( &refHead, *pNode, [&f](node_type& node){ f( node_to_value(node) ); } )) {
                pNode.release();
                return true;
            }
            return false;
        }

        template <typename Q, typename Compare, typename Func>
        bool erase_at( head_type& refHead, const Q& key, Compare cmp, Func f )
        {
            return base_class::erase_at( &refHead, key, cmp, [&f](node_type const& node){ f( node_to_value(node) ); } );
        }

        template <typename Q, typename Compare>
        bool extract_at( head_type& refHead, typename guarded_ptr::native_guard& guard, Q const& key, Compare cmp )
        {
            return base_class::extract_at( &refHead, guard, key, cmp );
        }

        template <typename Q, typename Func>
        std::pair<bool, bool> update_at( head_type& refHead, const Q& key, Func f, bool bAllowInsert )
        {
            scoped_node_ptr pNode( alloc_node( key ));

            std::pair<bool, bool> ret = base_class::update_at( &refHead, *pNode,
                [&f, &key](bool bNew, node_type& node, node_type&){f( bNew, node_to_value(node), key );},
                bAllowInsert );
            if ( ret.first && ret.second )
                pNode.release();

            return ret;
        }

        template <typename Q, typename Compare>
        bool find_at( head_type& refHead, Q const& key, Compare cmp )
        {
            return base_class::find_at( &refHead, key, cmp );
        }

        template <typename Q, typename Compare, typename Func>
        bool find_at( head_type& refHead, Q& val, Compare cmp, Func f )
        {
            return base_class::find_at( &refHead, val, cmp, [&f](node_type& node, Q& val){ f( node_to_value(node), val ); });
        }

        template <typename Q, typename Compare>
        bool get_at( head_type& refHead, typename guarded_ptr::native_guard& guard, Q const& key, Compare cmp )
        {
            return base_class::get_at( &refHead, guard, key, cmp );
        }

        //@endcond
    };

}} // namespace cds::container

#endif // #ifndef CDSLIB_CONTAINER_IMPL_LAZY_LIST_H
