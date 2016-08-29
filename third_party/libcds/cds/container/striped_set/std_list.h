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

#ifndef CDSLIB_CONTAINER_STRIPED_SET_STD_LIST_ADAPTER_H
#define CDSLIB_CONTAINER_STRIPED_SET_STD_LIST_ADAPTER_H

#include <functional>   // ref
#include <list>
#include <algorithm>    // std::lower_bound
#include <cds/container/striped_set/adapter.h>

#undef CDS_STD_LIST_SIZE_CXX11_CONFORM
#if !( defined(__GLIBCXX__ ) && (!defined(_GLIBCXX_USE_CXX11_ABI) || _GLIBCXX_USE_CXX11_ABI == 0 ))
#   define CDS_STD_LIST_SIZE_CXX11_CONFORM
#endif

//@cond
namespace cds { namespace container {
    namespace striped_set {

        // Copy policy for std::list
        template <typename T, typename Alloc>
        struct copy_item_policy< std::list< T, Alloc > >
        {
            typedef std::list< T, Alloc > list_type;
            typedef typename list_type::iterator iterator;

            void operator()( list_type& list, iterator itInsert, iterator itWhat )
            {
                itInsert = list.insert( itInsert, *itWhat );
            }
        };

        // Swap policy for std::list
        template <typename T, typename Alloc>
        struct swap_item_policy< std::list< T, Alloc > >
        {
            typedef std::list< T, Alloc > list_type;
            typedef typename list_type::iterator iterator;

            void operator()( list_type& list, iterator itInsert, iterator itWhat )
            {
                typename list_type::value_type newVal;
                itInsert = list.insert( itInsert, newVal );
                std::swap( *itWhat, *itInsert );
            }
        };

        // Move policy for std::list
        template <typename T, typename Alloc>
        struct move_item_policy< std::list< T, Alloc > >
        {
            typedef std::list< T, Alloc > list_type;
            typedef typename list_type::iterator iterator;

            void operator()( list_type& list, iterator itInsert, iterator itWhat )
            {
                list.insert( itInsert, std::move( *itWhat ) );
            }
        };
    }   // namespace striped_set
}} // namespace cds::container

namespace cds { namespace intrusive { namespace striped_set {

    /// std::list adapter for hash set bucket
    template <typename T, class Alloc, typename... Options>
    class adapt< std::list<T, Alloc>, Options... >
    {
    public:
        typedef std::list<T, Alloc>     container_type          ;   ///< underlying container type

    private:
        /// Adapted container type
        class adapted_container: public cds::container::striped_set::adapted_sequential_container
        {
        public:
            typedef typename container_type::value_type value_type  ;   ///< value type stored in the container
            typedef typename container_type::iterator      iterator ;   ///< container iterator
            typedef typename container_type::const_iterator const_iterator ;    ///< container const iterator

            static bool const has_find_with = true;
            static bool const has_erase_with = true;

        private:
            //@cond
            typedef typename cds::opt::details::make_comparator_from_option_list< value_type, Options... >::type key_comparator;


            typedef typename cds::opt::select<
                typename cds::opt::value<
                    typename cds::opt::find_option<
                        cds::opt::copy_policy< cds::container::striped_set::move_item >
                        , Options...
                    >::type
                >::copy_policy
                , cds::container::striped_set::copy_item, cds::container::striped_set::copy_item_policy<container_type>
                , cds::container::striped_set::swap_item, cds::container::striped_set::swap_item_policy<container_type>
                , cds::container::striped_set::move_item, cds::container::striped_set::move_item_policy<container_type>
            >::type copy_item;

            struct find_predicate
            {
                bool operator()( value_type const& i1, value_type const& i2) const
                {
                    return key_comparator()( i1, i2 ) < 0;
                }

                template <typename Q>
                bool operator()( Q const& i1, value_type const& i2) const
                {
                    return key_comparator()( i1, i2 ) < 0;
                }

                template <typename Q>
                bool operator()( value_type const& i1, Q const& i2) const
                {
                    return key_comparator()( i1, i2 ) < 0;
                }
            };
            //@endcond

        private:
            //@cond
            container_type  m_List;
#       if !defined(CDS_STD_LIST_SIZE_CXX11_CONFORM)
            // GCC C++ lib bug:
            // In GCC, the complexity of std::list::size() is O(N)
            // (see http://gcc.gnu.org/bugzilla/show_bug.cgi?id=49561)
            // Fixed in GCC 5
            size_t          m_nSize ;   // list size
#       endif
            //@endcond

        public:
            adapted_container()
#       if !defined(CDS_STD_LIST_SIZE_CXX11_CONFORM)
                : m_nSize(0)
#       endif
            {}

            template <typename Q, typename Func>
            bool insert( const Q& val, Func f )
            {
                iterator it = std::lower_bound( m_List.begin(), m_List.end(), val, find_predicate() );
                if ( it == m_List.end() || key_comparator()( val, *it ) != 0 ) {
                    value_type newItem( val );
                    it = m_List.insert( it, newItem );
                    f( *it );

#           if !defined(CDS_STD_LIST_SIZE_CXX11_CONFORM)
                    ++m_nSize;
#           endif
                    return true;
                }

                // key already exists
                return false;
            }

            template <typename... Args>
            bool emplace( Args&&... args )
            {
                value_type val(std::forward<Args>(args)...);
                iterator it = std::lower_bound( m_List.begin(), m_List.end(), val, find_predicate() );
                if ( it == m_List.end() || key_comparator()( val, *it ) != 0 ) {
                    it = m_List.emplace( it, std::move( val ) );
#           if !defined(CDS_STD_LIST_SIZE_CXX11_CONFORM)
                    ++m_nSize;
#           endif
                    return true;
                }
                return false;
            }

            template <typename Q, typename Func>
            std::pair<bool, bool> update( const Q& val, Func func, bool bAllowInsert )
            {
                iterator it = std::lower_bound( m_List.begin(), m_List.end(), val, find_predicate() );
                if ( it == m_List.end() || key_comparator()( val, *it ) != 0 ) {
                    // insert new
                    if ( !bAllowInsert )
                        return std::make_pair( false, false );

                    value_type newItem( val );
                    it = m_List.insert( it, newItem );
                    func( true, *it, val );
#           if !defined(CDS_STD_LIST_SIZE_CXX11_CONFORM)
                    ++m_nSize;
#           endif
                    return std::make_pair( true, true );
                }
                else {
                    // already exists
                    func( false, *it, val );
                    return std::make_pair( true, false );
                }
            }

            template <typename Q, typename Func>
            bool erase( const Q& key, Func f )
            {
                iterator it = std::lower_bound( m_List.begin(), m_List.end(), key, find_predicate() );
                if ( it == m_List.end() || key_comparator()( key, *it ) != 0 )
                    return false;

                // key exists
                f( *it );
                m_List.erase( it );
#           if !defined(CDS_STD_LIST_SIZE_CXX11_CONFORM)
                --m_nSize;
#           endif

                return true;
            }

            template <typename Q, typename Less, typename Func>
            bool erase( Q const& key, Less pred, Func f )
            {
                iterator it = std::lower_bound( m_List.begin(), m_List.end(), key, pred );
                if ( it == m_List.end() || pred( key, *it ) || pred( *it, key ) )
                    return false;

                // key exists
                f( *it );
                m_List.erase( it );
#           if !defined(CDS_STD_LIST_SIZE_CXX11_CONFORM)
                --m_nSize;
#           endif

                return true;
            }

            template <typename Q, typename Func>
            bool find( Q& val, Func f )
            {
                iterator it = std::lower_bound( m_List.begin(), m_List.end(), val, find_predicate() );
                if ( it == m_List.end() || key_comparator()( val, *it ) != 0 )
                    return false;

                // key exists
                f( *it, val );
                return true;
            }

            template <typename Q, typename Less, typename Func>
            bool find( Q& val, Less pred, Func f )
            {
                iterator it = std::lower_bound( m_List.begin(), m_List.end(), val, pred );
                if ( it == m_List.end() || pred( val, *it ) || pred( *it, val ) )
                    return false;

                // key exists
                f( *it, val );
                return true;
            }

            /// Clears the container
            void clear()
            {
                m_List.clear();
            }

            iterator begin()                { return m_List.begin(); }
            const_iterator begin() const    { return m_List.begin(); }
            iterator end()                  { return m_List.end(); }
            const_iterator end() const      { return m_List.end(); }

            void move_item( adapted_container& /*from*/, iterator itWhat )
            {
                iterator it = std::lower_bound( m_List.begin(), m_List.end(), *itWhat, find_predicate() );
                assert( it == m_List.end() || key_comparator()( *itWhat, *it ) != 0 );

                copy_item()( m_List, it, itWhat );
#           if !defined(CDS_STD_LIST_SIZE_CXX11_CONFORM)
                ++m_nSize;
#           endif
            }

            size_t size() const
            {
#           if !defined(CDS_STD_LIST_SIZE_CXX11_CONFORM)
                return m_nSize;
#           else
                return m_List.size();
#           endif

            }
        };

    public:
        typedef adapted_container type ; ///< Result of \p adapt metafunction

    };
}}}


//@endcond

#endif // #ifndef CDSLIB_CONTAINER_STRIPED_SET_STD_LIST_ADAPTER_H
