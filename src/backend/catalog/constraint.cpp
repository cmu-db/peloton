/*-------------------------------------------------------------------------
 *
 * constraint.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include "backend/catalog/constraint.h"
#include "backend/catalog/column.h"

namespace peloton {
namespace catalog {

std::ostream& operator<<(std::ostream& os, const Constraint& constraint) {

    os << "\tCONSTRAINT ";

    os << constraint.GetName() << " " << ConstraintTypeToString(constraint.type) << "\n";

    if(constraint.type == CONSTRAINT_TYPE_PRIMARY) {
        os << "\t\tPrimary Key Columns : ";
        for(auto col : constraint.columns) {
            os << col->GetName() << " ";
        }
        os << "\n";
    }
    else if(constraint.type == CONSTRAINT_TYPE_FOREIGN) {
        os << "\t\tLocal Columns : ";
        for(auto col : constraint.columns) {
            os << col->GetName() << " ";
        }
        os << "\n";

        os << "\t\tForeign Columns : ";
        for(auto col : constraint.foreign_columns) {
            os << col->GetName() << " ";
        }
        os << "\n";
    }

    os << "\n";

    return os;
}


} // End catalog namespace
} // End peloton namespace


