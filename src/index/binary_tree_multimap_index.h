/*-------------------------------------------------------------------------
 *
 * btree_multimap_index.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/index/btree_multimap_index.cpp
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <map>
#include <iostream>

#include "index/index.h"
#include "storage/tuple.h"
#include "common/logger.h"

namespace nstore {
namespace index {

//===--------------------------------------------------------------------===//
// Binary Tree Multimap Index
//===--------------------------------------------------------------------===//

/**
 * Index implemented as a Binary Tree Multimap.
 * @see TableIndex
 */
template<typename KeyType, class KeyComparator, class KeyEqualityChecker>
class BinaryTreeMultiMapIndex : public TableIndex
{

    friend class TableIndexFactory;

    typedef std::multimap<KeyType, const void*, KeyComparator> MapType;
    typedef typename MapType::const_iterator MMCIter;
    typedef typename MapType::iterator MMIter;
    typedef typename MapType::const_reverse_iterator MMCRIter;
    typedef typename MapType::reverse_iterator MMRIter;

public:

    ~BinaryTreeMultiMapIndex() {};

    bool addEntry(const storage::Tuple *tuple)
    {
        m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
        return addEntryPrivate(tuple, m_tmp1);
    }

    bool deleteEntry(const storage::Tuple *tuple)
    {
        m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
        return deleteEntryPrivate(tuple, m_tmp1);
    }

    bool replaceEntry(const storage::Tuple *oldTupleValue,
                      const storage::Tuple* newTupleValue)
    {
        // this can probably be optimized
        m_tmp1.setFromTuple(oldTupleValue, column_indices_, m_keySchema);
        m_tmp2.setFromTuple(newTupleValue, column_indices_, m_keySchema);
        if (m_eq(m_tmp1, m_tmp2))
        {
            // no update is needed for this index
            return true;
        }

        // It looks like we're deleting the new value and inserting the new value
        // The lookup is on the index keys, but the GetData of the current tuple
        //  (which has the new key value) is needed for this non-unique index
        //  to determine which of the tuples with a given key need to be deleted.
        bool deleted = deleteEntryPrivate(newTupleValue, m_tmp1);
        bool inserted = addEntryPrivate(newTupleValue, m_tmp2);
        --m_deletes;
        --m_inserts;
        ++m_updates;
        return (deleted && inserted);
    }

    bool setEntryToNewAddress(const storage::Tuple *tuple, const void* GetData) {
        m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
        ++m_updates;

//        int i = 0;
        std::pair<MMIter,MMIter> key_iter;
        for (key_iter = m_entries.equal_range(m_tmp1);
             key_iter.first != key_iter.second;
             ++(key_iter.first))
        {
//            VOLT_INFO("iteration %d", i++);

//            if (key_iter.first->second == tuple->GetData()) {
                m_entries.erase(key_iter.first);

                //std::pair<typename MapType::iterator, bool> retval = m_entries.insert(std::pair<KeyType, const void*>(m_tmp1, GetData));
                //return retval.second;

                m_entries.insert(std::pair<KeyType, const void*>(m_tmp1, GetData));
                return true;
//            }
        }

        LOG_INFO("Tuple not found.");

//        return true;

        //key exists, but not this tuple
        return false;
    }

    bool checkForIndexChange(const storage::Tuple *lhs, const storage::Tuple *rhs)
    {
        m_tmp1.setFromTuple(lhs, column_indices_, m_keySchema);
        m_tmp2.setFromTuple(rhs, column_indices_, m_keySchema);
        return !(m_eq(m_tmp1, m_tmp2));
    }

    bool exists(const storage::Tuple* values)
    {
        ++m_lookups;
        m_tmp1.setFromTuple(values, column_indices_, m_keySchema);
        //m_keyIter = m_entries.lower_bound(m_tmp1);
        return (m_entries.find(m_tmp1) != m_entries.end());
    }

    bool SetDataToKey(const storage::Tuple *searchKey)
    {
        m_tmp1.setFromKey(searchKey);
        return SetDataToKey(m_tmp1);
    }

    bool SetDataToTuple(const storage::Tuple *searchTuple)
    {
        m_tmp1.setFromTuple(searchTuple, column_indices_, m_keySchema);
        return SetDataToKey(m_tmp1);
    }

    void SetDataToKeyOrGreater(const storage::Tuple *searchKey)
    {
        ++m_lookups;
        m_begin = true;
        m_tmp1.setFromKey(searchKey);
        m_seqIter = m_entries.lower_bound(m_tmp1);
    }

    void SetDataToGreaterThanKey(const storage::Tuple *searchKey)
    {
        ++m_lookups;
        m_begin = true;
        m_tmp1.setFromKey(searchKey);
        m_seqIter = m_entries.upper_bound(m_tmp1);
    }

    void SetDataToEnd(bool begin)
    {
        ++m_lookups;
        m_begin = begin;
        if (begin)
            m_seqIter = m_entries.begin();
        else
            m_seqRIter = m_entries.rbegin();
    }

    storage::Tuple nextValue()
    {
        storage::Tuple retval(m_tupleSchema);

        if (m_begin) {
            if (m_seqIter == m_entries.end())
                return storage::Tuple();

            retval.SetData(const_cast<void*>(m_seqIter->second));
            ++m_seqIter;
        } else {
            if (m_seqRIter == (typename MapType::const_reverse_iterator) m_entries.rend())
                return storage::Tuple();
            retval.SetData(const_cast<void*>(m_seqRIter->second));
            ++m_seqRIter;
        }

        return retval;
    }

    storage::Tuple nextValueAtKey()
    {
        if (m_match.IsNull()) return m_match;
        storage::Tuple retval = m_match;
        ++(m_keyIter.first);
        if (m_keyIter.first == m_keyIter.second)
            m_match.SetData(NULL);
        else
            m_match.SetData(const_cast<void*>(m_keyIter.first->second));
        return retval;
    }

    bool advanceToNextKey()
    {
        if (m_keyIter.second == m_entries.end())
            return false;
        return SetDataToKey(m_keyIter.second->first);
    }

    size_t getSize() const { return m_entries.size(); }

    int64_t getMemoryEstimate() const {
        return 0;
        // return m_entries.bytesAllocated();
    }

    std::string getTypeName() const { return "BinaryTreeMultiMapIndex"; };

protected:
    BinaryTreeMultiMapIndex(const TableIndexScheme &scheme) :
        TableIndex(scheme),
        m_entries(KeyComparator(m_keySchema)),
        m_begin(true),
        m_eq(m_keySchema)
    {
        m_match = storage::Tuple(m_tupleSchema);
    }

    inline bool addEntryPrivate(const storage::Tuple *tuple, const KeyType &key)
    {
        ++m_inserts;
        m_entries.insert(std::pair<KeyType, const void*>(key, tuple->GetData()));
        return true;
    }

    inline bool deleteEntryPrivate(const storage::Tuple *tuple, const KeyType &key)
    {
        ++m_deletes;
        std::pair<MMIter,MMIter> key_iter;
        for (key_iter = m_entries.equal_range(key);
             key_iter.first != key_iter.second;
             ++(key_iter.first))
        {
            if (key_iter.first->second == tuple->GetData())
            {
                m_entries.erase(key_iter.first);
                //deleted
                return true;
            }
        }
        //key exists, but tuple not exists
        return false;
    }

    bool SetDataToKey(const KeyType &key)
    {
        ++m_lookups;
        m_begin = true;
        m_keyIter = m_entries.equal_range(key);
        if (m_keyIter.first == m_keyIter.second)
        {
            m_match.SetData(NULL);
            return false;
        }
        m_match.SetData(const_cast<void*>(m_keyIter.first->second));
        return !m_match.IsNull();
    }

    MapType m_entries;
    KeyType m_tmp1;
    KeyType m_tmp2;

    // iteration stuff
    bool m_begin;
    typename std::pair<MMCIter, MMCIter> m_keyIter;
    MMCIter m_seqIter;
    MMCRIter m_seqRIter;
    storage::Tuple m_match;

    // comparison stuff
    KeyEqualityChecker m_eq;
};

} // End index namespace
} // End nstore namespace
