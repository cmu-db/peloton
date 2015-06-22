/*-------------------------------------------------------------------------
 *
 * index.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/index.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/catalog/abstract_catalog_object.h"

#include <mutex>
#include <vector>
#include <iostream>

namespace nstore {

namespace index {
class Index;
}

namespace catalog {

class Column;


/**
 * Index Catalog Object
 */
class Index : public AbstractCatalogObject {

public:

    Index(std::string name, IndexType type, bool unique, std::vector<Column*> columns)
        : AbstractCatalogObject(static_cast<oid_t>(1), name), // FIXME
          type(type),
          unique(unique),
          columns(columns) {
    }

    ~Index() {
        // don't clean up columns here
        // they will cleaned in their respective tables
    }
    
    //===--------------------------------------------------------------------===//
    // ACCESSORS
    //===--------------------------------------------------------------------===//

    IndexType GetType() const {
        return type;
    }

    bool IsUnique() const {
        return unique;
    }

    std::vector<Column*> GetColumns() const {
        return columns;
    }

    // FIXME: REMOVE THIS!
    void SetPhysicalIndex(index::Index* index) {
        physical_index = index;
    }

    // FIXME: REMOVE THIS!
    index::Index *GetPhysicalIndex() {
        return physical_index;
    }

    // Get a string representation of this index
    friend std::ostream& operator<<(std::ostream& os, const Index& index);

private:

    //===--------------------------------------------------------------------===//
    // MEMBERS
    //===--------------------------------------------------------------------===//
    
    // What data structure is the index using ?
    IndexType type = INDEX_TYPE_INVALID;

    // Can the index contain duplicate keys?
    bool unique = false;

    // Columns referenced by the index
    std::vector<Column*> columns;

    // underlying physical index
    index::Index* physical_index = nullptr; // FIXME: REMOVE THIS!

};

} // End catalog namespace
} // End nstore namespace
