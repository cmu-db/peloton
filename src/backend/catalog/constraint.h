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
//#include "backend/index/index.h"
//#include "backend/storage/data_table.h"


#include <string>
#include <vector>
#include <iostream>

namespace peloton {
namespace catalog {

//class Column;

/**
 * Constraint Catalog Object
 */
class Constraint 
{

public:
    // FIXME: Not all constraints will have foreign key information
    //        We should make this information optional
    Constraint( ConstraintType type )//,
                //index::Index* index, 
                //storage::DataTable* foreign_key_table)
                //std::vector<Column*> columns, 
                //std::vector<Column*> foreign_columns)
        : type(type)//,
          //index(index),
          //foreign_key_table(foreign_key_table)
          //columns(columns),
          //foreign_columns(foreign_columns)
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

/*
    index::Index *GetIndex() const {
        return index;
    }

    storage::DataTable *GetForeignKeyTable() const {
        return foreign_key_table;
    }

    std::vector<Column*> GetColumns() const {
        return columns;
    }

    std::vector<Column*> GetForeignColumns() const {
        return foreign_columns;
    }

*/
    // Get a string representation of this constraint
    friend std::ostream& operator<<(std::ostream& os, const Constraint& constraint);

private:
    
    //===--------------------------------------------------------------------===//
    // MEMBERS
    //===--------------------------------------------------------------------===//
    
    // Constraint name 
    std::string name ;

    // The type of constraint
    ConstraintType type = CONSTRAINT_TYPE_INVALID;

    // The index used by this constraint (if needed)
/*
    index::Index* index = nullptr;

    // The table referenced by the foreign key (if needed)
    storage::DataTable* foreign_key_table = nullptr;

    // The columns in the table referenced by the constraint (if needed)
    std::vector<Column*> columns;

    // The columns in the foreign table referenced by the constraint (if needed)
    std::vector<Column*> foreign_columns;
*/

};

} // End catalog namespace
} // End peloton namespace
