/*-------------------------------------------------------------------------
 *
 * column.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/catalog/abstract_catalog_object.h"

namespace peloton {
namespace catalog {

/**
 * Column Catalog Object 
 */
class Column : public AbstractCatalogObject {

public:
    Column(std::string name, ValueType type, oid_t offset, size_t size, bool not_null)
        : AbstractCatalogObject(static_cast<oid_t>(1), name), // FIXME
          type(type),
          offset(offset),
          size(size),
          not_null(not_null) {
    }
    
    //===--------------------------------------------------------------------===//
    // ACCESSORS
    //===--------------------------------------------------------------------===//

    ValueType GetType() const {
        return type;
    }

    oid_t GetOffset() const {
        return offset;
    }

    size_t GetSize() const {
        return size;
    }

    bool IsNotNullable() const {
        return not_null;
    }

    // Get a string representation of this column
    friend std::ostream& operator<<(std::ostream& os, const Column& column);

private:

    // The data type of the column
    ValueType type = VALUE_TYPE_INVALID;

    // The column's order in the table
    oid_t offset = 0;

    // Size of the column
    size_t size = 0;

    // Is it not nullable ?
    bool not_null = false;

};

} // End catalog namespace
} // End peloton namespace
