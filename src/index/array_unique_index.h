/*-------------------------------------------------------------------------
 *
 * array_unique_index.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/index/array_unique_index.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "storage/tuple.h"
#include "index/index.h"

#include <vector>
#include <string>
#include <map>

namespace nstore {
namespace index {

//===--------------------------------------------------------------------===//
// Array Unique Index
//===--------------------------------------------------------------------===//

class Table;

const int ARRAY_INDEX_INITIAL_SIZE = 131072; // (2^17)

/**
 * Unique Index specialized for 1 integer.
 * This is implemented as a giant array, which gives optimal performance as far
 * as the entry value is assured to be sequential and limited to a small number.
 * @see TableIndex
 */
class ArrayUniqueIndex : public TableIndex {
    friend class TableIndexFactory;

    public:
        ~ArrayUniqueIndex();
        bool addEntry(const storage::Tuple *tuples);
        bool deleteEntry(const storage::Tuple *tuple);
        bool replaceEntry(const storage::Tuple *oldTupleValue, const storage::Tuple* newTupleValue);
        bool setEntryToNewAddress(const storage::Tuple *tuple, const void* address);
        bool exists(const storage::Tuple* values);
        bool moveToKey(const storage::Tuple *searchKey);
        bool moveToTuple(const storage::Tuple *searchTuple);
        storage::Tuple nextValueAtKey();
        bool advanceToNextKey();

        bool checkForIndexChange(const storage::Tuple *lhs, const storage::Tuple *rhs);

        size_t getSize() const { return 0; }
        int64_t getMemoryEstimate() const {
            return 0;
        }
        std::string getTypeName() const { return "ArrayIntUniqueIndex"; };

    protected:
        ArrayUniqueIndex(const TableIndexScheme &scheme);

        void **entries_;
        int32_t allocated_entries_;
        int32_t match_i_;
};

} // End index namespace
} // End nstore namespace

