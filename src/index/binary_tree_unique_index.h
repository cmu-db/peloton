/*-------------------------------------------------------------------------
 *
 * btree_unique_index.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/index/btree_unique_index.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

//#include <map>
#include "stx/btree_map.h"
#include <iostream>

#include "common/logger.h"
#include "storage/tuple.h"
#include "index/index.h"

namespace nstore {
namespace index {

//===--------------------------------------------------------------------===//
// Binary Tree Unique Index
//===--------------------------------------------------------------------===//

/**
 * Index implemented as a Binary Unique Map.
 * @see TableIndex
 */
template<typename KeyType, class KeyComparator, class KeyEqualityChecker>
class BinaryTreeUniqueIndex : public TableIndex
{
    friend class TableIndexFactory;

    //typedef std::map<KeyType, const void*, KeyComparator> MapType;
    typedef stx::btree_map<KeyType, const void*, KeyComparator> MapType;

public:

    ~BinaryTreeUniqueIndex() {};

    bool addEntry(const storage::Tuple* tuple)
    {
        m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
        return addEntryPrivate(tuple, m_tmp1);
    }

    bool deleteEntry(const storage::Tuple* tuple)
    {
        m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
        return deleteEntryPrivate(m_tmp1);
    }

    bool replaceEntry(const storage::Tuple* oldTupleValue,
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

        bool deleted = deleteEntryPrivate(m_tmp1);
        bool inserted = addEntryPrivate(newTupleValue, m_tmp2);
        --m_deletes;
        --m_inserts;
        ++m_updates;
        return (deleted && inserted);
    }
    
    bool setEntryToNewAddress(const storage::Tuple *tuple, const void* GetData) {
        // set the key from the tuple 
        m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
        ++m_updates; 
        
        m_entries.erase(m_tmp1); 
        std::pair<typename MapType::iterator, bool> retval = m_entries.insert(std::pair<KeyType, const void*>(m_tmp1, GetData));
        return retval.second;
    }

    bool checkForIndexChange(const storage::Tuple* lhs, const storage::Tuple* rhs)
    {
        m_tmp1.setFromTuple(lhs, column_indices_, m_keySchema);
        m_tmp2.setFromTuple(rhs, column_indices_, m_keySchema);
        return !(m_eq(m_tmp1, m_tmp2));
    }

    bool exists(const storage::Tuple* values)
    {
        ++m_lookups;
        m_tmp1.setFromTuple(values, column_indices_, m_keySchema);
        return (m_entries.find(m_tmp1) != m_entries.end());
    }

    bool SetDataToKey(const storage::Tuple* searchKey)
    {
        ++m_lookups;
        m_begin = true;
        m_tmp1.setFromKey(searchKey);
        m_keyIter = m_entries.find(m_tmp1);
        if (m_keyIter == m_entries.end()) {
            m_match.SetData(NULL);
            return false;
        }
        m_match.SetData(const_cast<void*>(m_keyIter->second));
        return !m_match.IsNull();
    }

    bool SetDataToTuple(const storage::Tuple* searchTuple)
    {
        ++m_lookups;
        m_begin = true;
        m_tmp1.setFromTuple(searchTuple, column_indices_, m_keySchema);
        m_keyIter = m_entries.find(m_tmp1);
        if (m_keyIter == m_entries.end()) {
            m_match.SetData(NULL);
            return false;
        }
        m_match.SetData(const_cast<void*>(m_keyIter->second));
        return !m_match.IsNull();
    }

    void SetDataToKeyOrGreater(const storage::Tuple* searchKey)
    {
        ++m_lookups;
        m_begin = true;
        m_tmp1.setFromKey(searchKey);
        m_keyIter = m_entries.lower_bound(m_tmp1);
    }

    void SetDataToGreaterThanKey(const storage::Tuple* searchKey)
    {
        ++m_lookups;
        m_begin = true;
        m_tmp1.setFromKey(searchKey);
        m_keyIter = m_entries.upper_bound(m_tmp1);
    }

    void SetDataToEnd(bool begin)
    {
        ++m_lookups;
        m_begin = begin;
        if (begin)
            m_keyIter = m_entries.begin();
        else
            m_keyRIter = m_entries.rbegin();
    }

    storage::Tuple nextValue()
    {
        storage::Tuple retval(m_tupleSchema);

        if (m_begin) {
            if (m_keyIter == m_entries.end())
                return storage::Tuple();
            retval.SetData(const_cast<void*>(m_keyIter->second));
            ++m_keyIter;
        } else {
            if (m_keyRIter == (typename MapType::const_reverse_iterator) m_entries.rend())
                return storage::Tuple();
            retval.SetData(const_cast<void*>(m_keyRIter->second));
            ++m_keyRIter;
        }

        return retval;
    }

    storage::Tuple nextValueAtKey()
    {
        storage::Tuple retval = m_match;
        m_match.SetData(NULL);
        return retval;
    }

    bool advanceToNextKey()
    {
        if (m_begin) {
            ++m_keyIter;
            if (m_keyIter == m_entries.end())
            {
                m_match.SetData(NULL);
                return false;
            }
            m_match.SetData(const_cast<void*>(m_keyIter->second));
        } else {
            ++m_keyRIter;
            if (m_keyRIter == (typename MapType::const_reverse_iterator) m_entries.rend())
            {
                m_match.SetData(NULL);
                return false;
            }
            m_match.SetData(const_cast<void*>(m_keyRIter->second));
        }

        return !m_match.IsNull();
    }

    size_t getSize() const { return m_entries.size(); }
    int64_t getMemoryEstimate() const {
        return 0;
        // return m_entries.bytesAllocated();
    }
    
    std::string getTypeName() const { return "BinaryTreeUniqueIndex"; };
    std::string debug() const
    {
        std::ostringstream buffer;
        buffer << TableIndex::debug() << std::endl;

        typename MapType::const_iterator i = m_entries.begin();
        while (i != m_entries.end()) {
            storage::Tuple retval(m_tupleSchema);
            retval.SetData(const_cast<void*>(i->second));
            buffer << retval << std::endl;
            ++i;
        }
        std::string ret(buffer.str());
        return (ret);
    }
protected:
    BinaryTreeUniqueIndex(const TableIndexScheme &scheme) :
        TableIndex(scheme),
        m_entries(KeyComparator(m_keySchema)),
        m_begin(true),
        m_eq(m_keySchema)
    {
        m_match = storage::Tuple(m_tupleSchema);
    }

    inline bool addEntryPrivate(const storage::Tuple* tuple, const KeyType &key)
    {
        ++m_inserts;
        std::pair<typename MapType::iterator, bool> retval =
            m_entries.insert(std::pair<KeyType, const void*>(key,
                                                             tuple->GetData()));
        return retval.second;
    }

    inline bool deleteEntryPrivate(const KeyType &key)
    {
        ++m_deletes;
        return m_entries.erase(key);
    }
        
    /*
    inline bool setEntryToNullPrivate(const KeyType &key)
    {        
        ++m_updates; 
        
        m_entries.erase(key); 
        std::pair<typename MapType::iterator, bool> retval = m_entries.insert(std::pair<KeyType, const void*>(key, NULL));
        return retval.second;
    }
     */

    MapType m_entries;
    KeyType m_tmp1;
    KeyType m_tmp2;

    // iteration stuff
    bool m_begin;
    typename MapType::const_iterator m_keyIter;
    typename MapType::const_reverse_iterator m_keyRIter;
    storage::Tuple m_match;

    // comparison stuff
    KeyEqualityChecker m_eq;
};

} // End index namespace
} // End nstore namespace
