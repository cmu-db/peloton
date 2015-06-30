/*-------------------------------------------------------------------------
 *
 * constraint.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/constraint.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"


#include <string>
#include <vector>
#include <iostream>

namespace peloton {

namespace index {
class Index;
}

namespace storage {
class DataTable;
}

namespace catalog {

/**
 * Constraint Catalog Object
 */
class Constraint
{

public:
    // FIXME: Not all constraints will have foreign key information
    //        We should make this information optional
    Constraint( std::string name, ConstraintType type,
                index::Index* index, storage::DataTable* foreign_key_table)
        : name(name), type(type), index(index), foreign_key_table(foreign_key_table)
    {
    }
    
    //===--------------------------------------------------------------------===//
    // ACCESSORS
    //===--------------------------------------------------------------------===//

    std::string GetName() const {
        return name;
    }

    ConstraintType GetType() const {
        return type;
    }

    index::Index *GetIndex() const {
        return index;
    }

    storage::DataTable *GetForeignKeyTable() const {
        return foreign_key_table;
    }

    // Get a string representation of this constraint
    friend std::ostream& operator<<(std::ostream& os, const Constraint& constraint);

private:
    
    //===--------------------------------------------------------------------===//
    // MEMBERS
    //===--------------------------------------------------------------------===//
    
    // Constraint name 
    std::string name = "";

    // The type of constraint
    ConstraintType type = CONSTRAINT_TYPE_INVALID;

    // The index used by this constraint (if needed)
    index::Index* index = nullptr;

    // The table referenced by the foreign key (if needed)
    storage::DataTable* foreign_key_table = nullptr;
};

} // End catalog namespace
} // End peloton namespace
