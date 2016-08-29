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

#ifndef CDSLIB_CONTAINER_SPLIT_LIST_SET_H
#define CDSLIB_CONTAINER_SPLIT_LIST_SET_H

#include <cds/intrusive/split_list.h>
#include <cds/container/details/make_split_list_set.h>

namespace cds { namespace container {

    /// Split-ordered list set
    /** @ingroup cds_nonintrusive_set
        \anchor cds_nonintrusive_SplitListSet_hp

        Hash table implementation based on split-ordered list algorithm discovered by Ori Shalev and Nir Shavit, see
        - [2003] Ori Shalev, Nir Shavit "Split-Ordered Lists - Lock-free Resizable Hash Tables"
        - [2008] Nir Shavit "The Art of Multiprocessor Programming"

        See \p intrusive::SplitListSet for a brief description of the split-list algorithm.

        Template parameters:
        - \p GC - Garbage collector used
        - \p T - type to be stored in the split-list.
        - \p Traits - type traits, default is \p split_list::traits. Instead of declaring \p split_list::traits -based
            struct you may apply option-based notation with \p split_list::make_traits metafunction.

        There are the specializations:
        - for \ref cds_urcu_desc "RCU" - declared in <tt>cd/container/split_list_set_rcu.h</tt>,
            see \ref cds_nonintrusive_SplitListSet_rcu "SplitListSet<RCU>".
        - for \ref cds::gc::nogc declared in <tt>cds/container/split_list_set_nogc.h</tt>,
            see \ref cds_nonintrusive_SplitListSet_nogc "SplitListSet<gc::nogc>".

        \par Usage

        You should decide what garbage collector you want, and what ordered list you want to use as a base. Split-ordered list
        is original data structure based on an ordered list.

        Suppose, you want construct split-list set based on \p gc::DHP GC
        and \p LazyList as ordered list implementation. So, you beginning your program with following include:
        \code
        #include <cds/container/lazy_list_dhp.h>
        #include <cds/container/split_list_set.h>

        namespace cc = cds::container;

        // The data belonged to split-ordered list
        sturuct foo {
            int     nKey;   // key field
            std::string strValue    ;   // value field
        };
        \endcode
        The inclusion order is important: first, include header for ordered-list implementation (for this example, <tt>cds/container/lazy_list_dhp.h</tt>),
        then the header for split-list set <tt>cds/container/split_list_set.h</tt>.

        Now, you should declare traits for split-list set. The main parts of traits are a hash functor for the set and a comparing functor for ordered list.
        Note that we define several function in <tt>foo_hash</tt> and <tt>foo_less</tt> functors for different argument types since we want call our \p %SplitListSet
        object by the key of type <tt>int</tt> and by the value of type <tt>foo</tt>.

        The second attention: instead of using \p %LazyList in \p %SplitListSet traits we use a tag \p cds::contaner::lazy_list_tag for the lazy list.
        The split-list requires significant support from underlying ordered list class and it is not good idea to dive you
        into deep implementation details of split-list and ordered list interrelations. The tag paradigm simplifies split-list interface.

        \code
        // foo hash functor
        struct foo_hash {
            size_t operator()( int key ) const { return std::hash( key ) ; }
            size_t operator()( foo const& item ) const { return std::hash( item.nKey ) ; }
        };

        // foo comparator
        struct foo_less {
            bool operator()(int i, foo const& f ) const { return i < f.nKey ; }
            bool operator()(foo const& f, int i ) const { return f.nKey < i ; }
            bool operator()(foo const& f1, foo const& f2) const { return f1.nKey < f2.nKey; }
        };

        // SplitListSet traits
        struct foo_set_traits: public cc::split_list::traits
        {
            typedef cc::lazy_list_tag   ordered_list; // what type of ordered list we want to use
            typedef foo_hash            hash;         // hash functor for our data stored in split-list set

            // Type traits for our LazyList class
            struct ordered_list_traits: public cc::lazy_list::traits
            {
                typedef foo_less less   ;   // use our foo_less as comparator to order list nodes
            };
        };
        \endcode

        Now you are ready to declare our set class based on \p %SplitListSet:
        \code
        typedef cc::SplitListSet< cds::gc::DHP, foo, foo_set_traits > foo_set;
        \endcode

        You may use the modern option-based declaration instead of classic traits-based one:
        \code
        typedef cc::SplitListSet<
            cs::gc::DHP             // GC used
            ,foo                    // type of data stored
            ,cc::split_list::make_traits<      // metafunction to build split-list traits
                cc::split_list::ordered_list<cc::lazy_list_tag>  // tag for underlying ordered list implementation
                ,cc::opt::hash< foo_hash >               // hash functor
                ,cc::split_list::ordered_list_traits<    // ordered list traits desired
                    cc::lazy_list::make_traits<          // metafunction to build lazy list traits
                        cc::opt::less< foo_less >        // less-based compare functor
                    >::type
                >
            >::type
        >  foo_set;
        \endcode
        In case of option-based declaration using split_list::make_traits metafunction
        the struct \p foo_set_traits is not required.

        Now, the set of type \p foo_set is ready to use in your program.

        Note that in this example we show only mandatory \p traits parts, optional ones is the default and they are inherited
        from \p cds::container::split_list::traits.
        There are many other options for deep tuning the split-list and ordered-list containers.
    */
    template <
        class GC,
        class T,
#ifdef CDS_DOXYGEN_INVOKED
        class Traits = split_list::traits
#else
        class Traits
#endif
    >
    class SplitListSet:
#ifdef CDS_DOXYGEN_INVOKED
        protected intrusive::SplitListSet<GC, typename Traits::ordered_list, Traits>
#else
        protected details::make_split_list_set< GC, T, typename Traits::ordered_list, split_list::details::wrap_set_traits<T, Traits> >::type
#endif
    {
    protected:
        //@cond
        typedef details::make_split_list_set< GC, T, typename Traits::ordered_list, split_list::details::wrap_set_traits<T, Traits> > maker;
        typedef typename maker::type  base_class;
        //@endcond

    public:
        typedef GC      gc;         ///< Garbage collector
        typedef T       value_type; ///< Type of vlue to be stored in split-list
        typedef Traits  traits;     ///< \p Traits template argument
        typedef typename maker::ordered_list ordered_list; ///< Underlying ordered list class
        typedef typename base_class::key_comparator key_comparator; ///< key compare functor

        /// Hash functor for \p %value_type and all its derivatives that you use
        typedef typename base_class::hash         hash;
        typedef typename base_class::item_counter item_counter; ///< Item counter type
        typedef typename base_class::stat         stat; ///< Internal statistics

        /// Count of hazard pointer required
        static CDS_CONSTEXPR const size_t c_nHazardPtrCount = base_class::c_nHazardPtrCount;

    protected:
        //@cond
        typedef typename maker::cxx_node_allocator    cxx_node_allocator;
        typedef typename maker::node_type             node_type;
        //@endcond

    public:
        /// Guarded pointer
        typedef typename gc::template guarded_ptr< node_type, value_type, details::guarded_ptr_cast_set<node_type, value_type> > guarded_ptr;

    protected:
        //@cond
        template <typename Q>
        static node_type * alloc_node(Q const& v )
        {
            return cxx_node_allocator().New( v );
        }

        template <typename... Args>
        static node_type * alloc_node( Args&&... args )
        {
            return cxx_node_allocator().MoveNew( std::forward<Args>( args )... );
        }

        static void free_node( node_type * pNode )
        {
            cxx_node_allocator().Delete( pNode );
        }

        template <typename Q, typename Func>
        bool find_( Q& val, Func f )
        {
            return base_class::find( val, [&f]( node_type& item, Q& val ) { f(item.m_Value, val) ; } );
        }

        template <typename Q, typename Less, typename Func>
        bool find_with_( Q& val, Less pred, Func f )
        {
            CDS_UNUSED( pred );
            return base_class::find_with( val, typename maker::template predicate_wrapper<Less>::type(),
                [&f]( node_type& item, Q& val ) { f(item.m_Value, val) ; } );
        }

        struct node_disposer {
            void operator()( node_type * pNode )
            {
                free_node( pNode );
            }
        };
        typedef std::unique_ptr< node_type, node_disposer >     scoped_node_ptr;

        bool insert_node( node_type * pNode )
        {
            assert( pNode != nullptr );
            scoped_node_ptr p(pNode);

            if ( base_class::insert( *pNode )) {
                p.release();
                return true;
            }
            return false;
        }

        //@endcond

    protected:
        //@cond
        template <bool IsConst>
        class iterator_type: protected base_class::template iterator_type<IsConst>
        {
            typedef typename base_class::template iterator_type<IsConst> iterator_base_class;
            friend class SplitListSet;

        public:
            /// Value pointer type (const for const iterator)
            typedef typename cds::details::make_const_type<value_type, IsConst>::pointer   value_ptr;
            /// Value reference type (const for const iterator)
            typedef typename cds::details::make_const_type<value_type, IsConst>::reference value_ref;

        public:
            /// Default ctor
            iterator_type()
            {}

            /// Copy ctor
            iterator_type( iterator_type const& src )
                : iterator_base_class( src )
            {}

        protected:
            explicit iterator_type( iterator_base_class const& src )
                : iterator_base_class( src )
            {}

        public:
            /// Dereference operator
            value_ptr operator ->() const
            {
                return &(iterator_base_class::operator->()->m_Value);
            }

            /// Dereference operator
            value_ref operator *() const
            {
                return iterator_base_class::operator*().m_Value;
            }

            /// Pre-increment
            iterator_type& operator ++()
            {
                iterator_base_class::operator++();
                return *this;
            }

            /// Assignment operator
            iterator_type& operator = (iterator_type const& src)
            {
                iterator_base_class::operator=(src);
                return *this;
            }

            /// Equality operator
            template <bool C>
            bool operator ==(iterator_type<C> const& i ) const
            {
                return iterator_base_class::operator==(i);
            }

            /// Equality operator
            template <bool C>
            bool operator !=(iterator_type<C> const& i ) const
            {
                return iterator_base_class::operator!=(i);
            }
        };
        //@endcond

    public:
        /// Initializes split-ordered list of default capacity
        /**
            The default capacity is defined in bucket table constructor.
            See \p intrusive::split_list::expandable_bucket_table, \p intrusive::split_list::static_bucket_table
            which selects by \p split_list::dynamic_bucket_table option.
        */
        SplitListSet()
            : base_class()
        {}

        /// Initializes split-ordered list
        SplitListSet(
            size_t nItemCount           ///< estimated average of item count
            , size_t nLoadFactor = 1    ///< the load factor - average item count per bucket. Small integer up to 8, default is 1.
            )
            : base_class( nItemCount, nLoadFactor )
        {}

    public:
    ///@name Forward iterators (only for debugging purpose)
    //@{
        /// Forward iterator
        /**
            The forward iterator for a split-list has the following features:
            - it has no post-increment operator
            - it depends on underlying ordered list iterator
            - The iterator object cannot be moved across thread boundary because it contains GC's guard that is thread-private GC data.
            - Iterator ensures thread-safety even if you delete the item that iterator points to. However, in case of concurrent
              deleting operations it is no guarantee that you iterate all item in the split-list.
              Moreover, a crash is possible when you try to iterate the next element that has been deleted by concurrent thread.

              @warning Use this iterator on the concurrent container for debugging purpose only.

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
        typedef iterator_type<false>  iterator;

        /// Const forward iterator
        typedef iterator_type<true>    const_iterator;

        /// Returns a forward iterator addressing the first element in a set
        /**
            For empty set \code begin() == end() \endcode
        */
        iterator begin()
        {
            return iterator( base_class::begin());
        }

        /// Returns an iterator that addresses the location succeeding the last element in a set
        /**
            Do not use the value returned by <tt>end</tt> function to access any item.
            The returned value can be used only to control reaching the end of the set.
            For empty set \code begin() == end() \endcode
        */
        iterator end()
        {
            return iterator( base_class::end());
        }

        /// Returns a forward const iterator addressing the first element in a set
        const_iterator begin() const
        {
            return cbegin();
        }
        /// Returns a forward const iterator addressing the first element in a set
        const_iterator cbegin() const
        {
            return const_iterator( base_class::cbegin());
        }

        /// Returns an const iterator that addresses the location succeeding the last element in a set
        const_iterator end() const
        {
            return cend();
        }
        /// Returns an const iterator that addresses the location succeeding the last element in a set
        const_iterator cend() const
        {
            return const_iterator( base_class::cend());
        }
    //@}

    public:
        /// Inserts new node
        /**
            The function creates a node with copy of \p val value
            and then inserts the node created into the set.

            The type \p Q should contain as minimum the complete key for the node.
            The object of \ref value_type should be constructible from a value of type \p Q.
            In trivial case, \p Q is equal to \ref value_type.

            Returns \p true if \p val is inserted into the set, \p false otherwise.
        */
        template <typename Q>
        bool insert( Q const& val )
        {
            return insert_node( alloc_node( val ));
        }

        /// Inserts new node
        /**
            The function allows to split creating of new item into two part:
            - create item with key only
            - insert new item into the set
            - if inserting is success, calls  \p f functor to initialize value-field of \p val.

            The functor signature is:
            \code
                void func( value_type& val );
            \endcode
            where \p val is the item inserted.

            The user-defined functor is called only if the inserting is success.

            @warning For \ref cds_intrusive_MichaelList_hp "MichaelList" as the bucket see \ref cds_intrusive_item_creating "insert item troubleshooting".
            \ref cds_intrusive_LazyList_hp "LazyList" provides exclusive access to inserted item and does not require any node-level
            synchronization.
        */
        template <typename Q, typename Func>
        bool insert( Q const& val, Func f )
        {
            scoped_node_ptr pNode( alloc_node( val ));

            if ( base_class::insert( *pNode, [&f](node_type& node) { f( node.m_Value ) ; } )) {
                pNode.release();
                return true;
            }
            return false;
        }

        /// Inserts data of type \p value_type created from \p args
        /**
            Returns \p true if inserting successful, \p false otherwise.
        */
        template <typename... Args>
        bool emplace( Args&&... args )
        {
            return insert_node( alloc_node( std::forward<Args>(args)...));
        }

        /// Updates the node
        /**
            The operation performs inserting or changing data with lock-free manner.

            If \p key is not found in the set, then \p key is inserted iff \p bAllowInsert is \p true.
            Otherwise, the functor \p func is called with item found.

            The functor signature is:
            \code
                struct my_functor {
                    void operator()( bool bNew, value_type& item, const Q& val );
                };
            \endcode
            with arguments:
            - \p bNew - \p true if the item has been inserted, \p false otherwise
            - \p item - item of the set
            - \p val - argument \p val passed into the \p %update() function

            The functor may change non-key fields of the \p item.

            Returns <tt> std::pair<bool, bool> </tt> where \p first is true if operation is successfull,
            \p second is true if new item has been added or \p false if the item with \p key
            already is in the map.

            @warning For \ref cds_intrusive_MichaelList_hp "MichaelList" as the bucket see \ref cds_intrusive_item_creating "insert item troubleshooting".
            \ref cds_intrusive_LazyList_hp "LazyList" provides exclusive access to inserted item and does not require any node-level
            synchronization.
        */
        template <typename Q, typename Func>
        std::pair<bool, bool> update( Q const& val, Func func, bool bAllowInsert = true )
        {
            scoped_node_ptr pNode( alloc_node( val ));

            std::pair<bool, bool> bRet = base_class::update( *pNode,
                [&func, &val]( bool bNew, node_type& item,  node_type const& /*val*/ ) {
                    func( bNew, item.m_Value, val );
                }, bAllowInsert );

            if ( bRet.first && bRet.second )
                pNode.release();
            return bRet;
        }
        //@cond
        template <typename Q, typename Func>
        CDS_DEPRECATED("ensure() is deprecated, use update()")
        std::pair<bool, bool> ensure( Q const& val, Func func )
        {
            return update( val, func, true );
        }
        //@endcond

        /// Deletes \p key from the set
        /** \anchor cds_nonintrusive_SplitListSet_erase_val

            The item comparator should be able to compare the values of type \p value_type
            and the type \p Q.

            Return \p true if key is found and deleted, \p false otherwise
        */
        template <typename Q>
        bool erase( Q const& key )
        {
            return base_class::erase( key );
        }

        /// Deletes the item from the set using \p pred predicate for searching
        /**
            The function is an analog of \ref cds_nonintrusive_SplitListSet_erase_val "erase(Q const&)"
            but \p pred is used for key comparing.
            \p Less functor has the interface like \p std::less.
            \p Less must imply the same element order as the comparator used for building the set.
        */
        template <typename Q, typename Less>
        bool erase_with( Q const& key, Less pred )
        {
            CDS_UNUSED( pred );
            return base_class::erase_with( key, typename maker::template predicate_wrapper<Less>::type());
        }

        /// Deletes \p key from the set
        /** \anchor cds_nonintrusive_SplitListSet_erase_func

            The function searches an item with key \p key, calls \p f functor
            and deletes the item. If \p key is not found, the functor is not called.

            The functor \p Func interface:
            \code
            struct extractor {
                void operator()(value_type const& val);
            };
            \endcode

            Since the key of split-list \p value_type is not explicitly specified,
            template parameter \p Q defines the key type searching in the list.
            The list item comparator should be able to compare the values of the type \p value_type
            and the type \p Q.

            Return \p true if key is found and deleted, \p false otherwise
        */
        template <typename Q, typename Func>
        bool erase( Q const& key, Func f )
        {
            return base_class::erase( key, [&f](node_type& node) { f( node.m_Value ); } );
        }

        /// Deletes the item from the set using \p pred predicate for searching
        /**
            The function is an analog of \ref cds_nonintrusive_SplitListSet_erase_func "erase(Q const&, Func)"
            but \p pred is used for key comparing.
            \p Less functor has the interface like \p std::less.
            \p Less must imply the same element order as the comparator used for building the set.
        */
        template <typename Q, typename Less, typename Func>
        bool erase_with( Q const& key, Less pred, Func f )
        {
            CDS_UNUSED( pred );
            return base_class::erase_with( key, typename maker::template predicate_wrapper<Less>::type(),
                [&f](node_type& node) { f( node.m_Value ); } );
        }

        /// Extracts the item with specified \p key
        /** \anchor cds_nonintrusive_SplitListSet_hp_extract
            The function searches an item with key equal to \p key,
            unlinks it from the set, and returns it as \p guarded_ptr.
            If \p key is not found the function returns an empty guarded pointer.

            Note the compare functor should accept a parameter of type \p Q that may be not the same as \p value_type.

            The extracted item is freed automatically when returned \p guarded_ptr object will be destroyed or released.
            @note Each \p guarded_ptr object uses the GC's guard that can be limited resource.

            Usage:
            \code
            typedef cds::container::SplitListSet< your_template_args > splitlist_set;
            splitlist_set theSet;
            // ...
            {
                splitlist_set::guarded_ptr gp(theSet.extract( 5 ));
                if ( gp ) {
                    // Deal with gp
                    // ...
                }
                // Destructor of gp releases internal HP guard
            }
            \endcode
        */
        template <typename Q>
        guarded_ptr extract( Q const& key )
        {
            guarded_ptr gp;
            extract_( gp.guard(), key );
            return gp;
        }

        /// Extracts the item using compare functor \p pred
        /**
            The function is an analog of \ref cds_nonintrusive_SplitListSet_hp_extract "extract(Q const&)"
            but \p pred predicate is used for key comparing.

            \p Less functor has the semantics like \p std::less but should take arguments of type \ref value_type and \p Q
            in any order.
            \p pred must imply the same element order as the comparator used for building the set.
        */
        template <typename Q, typename Less>
        guarded_ptr extract_with( Q const& key, Less pred )
        {
            guarded_ptr gp;
            extract_with_( gp.guard(), key, pred );
            return gp;
        }

        /// Finds the key \p key
        /** \anchor cds_nonintrusive_SplitListSet_find_func

            The function searches the item with key equal to \p key and calls the functor \p f for item found.
            The interface of \p Func functor is:
            \code
            struct functor {
                void operator()( value_type& item, Q& key );
            };
            \endcode
            where \p item is the item found, \p key is the <tt>find</tt> function argument.

            The functor may change non-key fields of \p item. Note that the functor is only guarantee
            that \p item cannot be disposed during functor is executing.
            The functor does not serialize simultaneous access to the set's \p item. If such access is
            possible you must provide your own synchronization schema on item level to exclude unsafe item modifications.

            The \p key argument is non-const since it can be used as \p f functor destination i.e., the functor
            may modify both arguments.

            Note the hash functor specified for class \p Traits template parameter
            should accept a parameter of type \p Q that can be not the same as \p value_type.

            The function returns \p true if \p key is found, \p false otherwise.
        */
        template <typename Q, typename Func>
        bool find( Q& key, Func f )
        {
            return find_( key, f );
        }
        //@cond
        template <typename Q, typename Func>
        bool find( Q const& key, Func f )
        {
            return find_( key, f );
        }
        //@endcond

        /// Finds the key \p key using \p pred predicate for searching
        /**
            The function is an analog of \ref cds_nonintrusive_SplitListSet_find_func "find(Q&, Func)"
            but \p pred is used for key comparing.
            \p Less functor has the interface like \p std::less.
            \p Less must imply the same element order as the comparator used for building the set.
        */
        template <typename Q, typename Less, typename Func>
        bool find_with( Q& key, Less pred, Func f )
        {
            return find_with_( key, pred, f );
        }
        //@cond
        template <typename Q, typename Less, typename Func>
        bool find_with( Q const& key, Less pred, Func f )
        {
            return find_with_( key, pred, f );
        }
        //@endcond

        /// Checks whether the set contains \p key
        /**
            The function searches the item with key equal to \p key
            and returns \p true if it is found, and \p false otherwise.

            Note the hash functor specified for class \p Traits template parameter
            should accept a parameter of type \p Q that can be not the same as \p value_type.
            Otherwise, you may use \p contains( Q const&, Less pred ) functions with explicit predicate for key comparing.
        */
        template <typename Q>
        bool contains( Q const& key )
        {
            return base_class::contains( key );
        }
        //@cond
        template <typename Q>
        CDS_DEPRECATED("deprecated, use contains()")
        bool find( Q const& key )
        {
            return contains( key );
        }
        //@endcond

        /// Checks whether the map contains \p key using \p pred predicate for searching
        /**
            The function is similar to <tt>contains( key )</tt> but \p pred is used for key comparing.
            \p Less functor has the interface like \p std::less.
            \p Less must imply the same element order as the comparator used for building the map.
        */
        template <typename Q, typename Less>
        bool contains( Q const& key, Less pred )
        {
            CDS_UNUSED( pred );
            return base_class::contains( key, typename maker::template predicate_wrapper<Less>::type());
        }
        //@cond
        template <typename Q, typename Less>
        CDS_DEPRECATED("deprecated, use contains()")
        bool find_with( Q const& key, Less pred )
        {
            return contains( key, pred );
        }
        //@endcond

        /// Finds the key \p key and return the item found
        /** \anchor cds_nonintrusive_SplitListSet_hp_get
            The function searches the item with key equal to \p key
            and returns the item found as \p guarded_ptr.
            If \p key is not found the function returns an empty guarded pointer.

            @note Each \p guarded_ptr object uses one GC's guard which can be limited resource.

            Usage:
            \code
            typedef cds::container::SplitListSet< your_template_params >  splitlist_set;
            splitlist_set theSet;
            // ...
            {
                splitlist_set::guarded_ptr gp(theSet.get( 5 ));
                if ( gp ) {
                    // Deal with gp
                    //...
                }
                // Destructor of guarded_ptr releases internal HP guard
            }
            \endcode

            Note the compare functor specified for split-list set
            should accept a parameter of type \p Q that can be not the same as \p value_type.
        */
        template <typename Q>
        guarded_ptr get( Q const& key )
        {
            guarded_ptr gp;
            get_( gp.guard(), key );
            return gp;
        }

        /// Finds \p key and return the item found
        /**
            The function is an analog of \ref cds_nonintrusive_SplitListSet_hp_get "get( Q const&)"
            but \p pred is used for comparing the keys.

            \p Less functor has the semantics like \p std::less but should take arguments of type \ref value_type and \p Q
            in any order.
            \p pred must imply the same element order as the comparator used for building the set.
        */
        template <typename Q, typename Less>
        guarded_ptr get_with( Q const& key, Less pred )
        {
            guarded_ptr gp;
            get_with_( gp.guard(), key, pred );
            return gp;
        }

        /// Clears the set (not atomic)
        void clear()
        {
            base_class::clear();
        }

        /// Checks if the set is empty
        /**
            Emptiness is checked by item counting: if item count is zero then assume that the set is empty.
            Thus, the correct item counting feature is an important part of split-list set implementation.
        */
        bool empty() const
        {
            return base_class::empty();
        }

        /// Returns item count in the set
        size_t size() const
        {
            return base_class::size();
        }

        /// Returns internal statistics
        stat const& statistics() const
        {
            return base_class::statistics();
        }

    protected:
        //@cond
        using base_class::extract_;
        using base_class::get_;

        template <typename Q, typename Less>
        bool extract_with_( typename guarded_ptr::native_guard& guard, Q const& key, Less pred )
        {
            CDS_UNUSED( pred );
            return base_class::extract_with_( guard, key, typename maker::template predicate_wrapper<Less>::type());
        }

        template <typename Q, typename Less>
        bool get_with_( typename guarded_ptr::native_guard& guard, Q const& key, Less pred )
        {
            CDS_UNUSED( pred );
            return base_class::get_with_( guard, key, typename maker::template predicate_wrapper<Less>::type());
        }

        //@endcond

    };


}}  // namespace cds::container

#endif // #ifndef CDSLIB_CONTAINER_SPLIT_LIST_SET_H
