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
    Constraint( ConstraintType type, std::string name = "")
                : type(type), name(name) {}
    
    //===--------------------------------------------------------------------===//
    // ACCESSORS
    //===--------------------------------------------------------------------===//

    ConstraintType GetType() const {
        return type;
    }

    std::string GetName() const {
        return name;
    }

    index::Index *GetIndex() const {
        return primary_key_index;
    }

    storage::DataTable *GetForeignKeyTable() const {
        return foreign_key_table;
    }

    void SetIndex(index::Index* index){
      primary_key_index = index;       
    }

    void SetForeignKeyTable(storage::DataTable* table){
      foreign_key_table = table;
    }

    // Get a string representation of this constraint
    friend std::ostream& operator<<(std::ostream& os, const Constraint& constraint);

private:
    
    //===--------------------------------------------------------------------===//
    // MEMBERS
    //===--------------------------------------------------------------------===//
    
    // The type of constraint
    ConstraintType type = CONSTRAINT_TYPE_INVALID;

    // Constraint name (if needed)
    std::string name = "";

    // The index used by this constraint (if needed)
    index::Index* primary_key_index = nullptr;

    // The table referenced by the foreign key (if needed)
    storage::DataTable* foreign_key_table = nullptr;
};

} // End catalog namespace
} // End peloton namespace
