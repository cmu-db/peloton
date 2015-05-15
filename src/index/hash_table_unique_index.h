/*-------------------------------------------------------------------------
 *
 * hash_table_unique_index.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/index/hash_table_unique_index.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <iostream>

#include "index/index.h"
#include "boost/unordered_map.hpp"

namespace nstore {
namespace index {

//===--------------------------------------------------------------------===//
// Hash Table Unique Index
//===--------------------------------------------------------------------===//

/**
 * Index implemented as a Hash Table Unique Map.
 * @see TableIndex
 */
template<typename KeyType, class KeyHasher, class KeyEqualityChecker>
class HashTableUniqueIndex : public TableIndex {
    friend class TableIndexFactory;

    typedef boost::unordered_map<KeyType, const void*, KeyHasher, KeyEqualityChecker> MapType;

public:

    ~HashTableUniqueIndex() {};

    bool addEntry(const storage::Tuple *tuple) {
        m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
        return addEntryPrivate(tuple, m_tmp1);
    }

    bool deleteEntry(const storage::Tuple *tuple) {
        m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
        return deleteEntryPrivate(m_tmp1);
    }

    bool replaceEntry(const storage::Tuple *oldTupleValue, const storage::Tuple* newTupleValue) {
        // this can probably be optimized
        m_tmp1.setFromTuple(oldTupleValue, column_indices_, m_keySchema);
        m_tmp2.setFromTuple(newTupleValue, column_indices_, m_keySchema);

        if (m_eq(m_tmp1, m_tmp2)) return true; // no update is needed for this index

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
        
        // erase the entry and add a entry with the same key and a NULL value
        bool deleted = deleteEntryPrivate(m_tmp1);
//        m_entries.erase(m_tmp1); 
        std::pair<typename MapType::iterator, bool> retval = m_entries.insert(std::pair<KeyType, const void*>(m_tmp1, GetData));
        return (retval.second & deleted);
    }

    bool checkForIndexChange(const storage::Tuple *lhs, const storage::Tuple *rhs) {
        m_tmp1.setFromTuple(lhs, column_indices_, m_keySchema);
        m_tmp2.setFromTuple(rhs, column_indices_, m_keySchema);
        return !(m_eq(m_tmp1, m_tmp2));
    }
    bool exists(const storage::Tuple* values) {
        ++m_lookups;
        m_tmp1.setFromTuple(values, column_indices_, m_keySchema);
        return (m_entries.find(m_tmp1) != m_entries.end());
    }
    bool SetDataToKey(const storage::Tuple *searchKey) {
        ++m_lookups;
        m_tmp1.setFromKey(searchKey);
        m_keyIter = m_entries.find(m_tmp1);
        if (m_keyIter == m_entries.end()) {
            m_match.SetData(NULL);
            return false;
        }
        m_match.SetData(const_cast<void*>(m_keyIter->second));
        return m_match.GetData() != NULL;
    }
    bool SetDataToTuple(const storage::Tuple *searchTuple) {
        ++m_lookups;
        m_tmp1.setFromTuple(searchTuple, column_indices_, m_keySchema);
        m_keyIter = m_entries.find(m_tmp1);
        if (m_keyIter == m_entries.end()) {
            m_match.SetData(NULL);
            return false;
        }
        m_match.SetData(const_cast<void*>(m_keyIter->second));
        return m_match.GetData() != NULL;
    }
    storage::Tuple nextValueAtKey() {
        storage::Tuple retval = m_match;
        m_match.SetData(NULL);
        return retval;
    }

    virtual void ensureCapacity(uint32_t capacity) {
        m_entries.rehash(capacity * 2);
    }

    size_t getSize() const { return m_entries.size(); }
    int64_t getMemoryEstimate() const {
        return 0;
        // return m_entries.bytesAllocated();
    }
    std::string getTypeName() const { return "HashTableUniqueIndex"; };

    // print out info about lookup usage
    virtual void printReport() {
        TableIndex::printReport();
        std::cout << "  Loadfactor: " << m_entries.load_factor() << std::endl;
    }

protected:
    HashTableUniqueIndex(const TableIndexScheme &scheme) :
        TableIndex(scheme),
        m_entries(100, KeyHasher(m_keySchema), KeyEqualityChecker(m_keySchema)),
        m_eq(m_keySchema)
    {
        m_match = storage::Tuple(m_tupleSchema);
        m_entries.max_load_factor(.75f);
        //m_entries.rehash(200000);
    }

    inline bool addEntryPrivate(const storage::Tuple *tuple, const KeyType &key) {
        ++m_inserts;
        std::pair<typename MapType::iterator, bool> retval = m_entries.insert(std::pair<KeyType, const void*>(key, tuple->GetData()));
        return retval.second;
    }

    inline bool deleteEntryPrivate(const KeyType &key) {
        ++m_deletes;
        typename MapType::iterator mapiter = m_entries.find(key);
        if (mapiter == m_entries.end())
            return false; //key not exists
        m_entries.erase(mapiter);
        return true; //deleted
    }

    MapType m_entries;
    KeyType m_tmp1;
    KeyType m_tmp2;

    // iteration stuff
    typename MapType::const_iterator m_keyIter;
    storage::Tuple m_match;

    // comparison stuff
    typename MapType::key_equal m_eq;
};

} // End index namespace
} // End nstore namespace
