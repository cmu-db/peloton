/*-------------------------------------------------------------------------
 *
 * table.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/table.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/catalog/table.h"

namespace nstore {
namespace catalog {

bool Table::AddColumn(Column* column) {
    if(std::find(columns.begin(), columns.end(), column) != columns.end())
        return false;
    columns.push_back(column);
    return true;
}

Column* Table::GetColumn(const std::string &column_name) const {
    for(auto column : columns)
        if(column->GetName() == column_name)
            return column;
    return nullptr;
}

bool Table::RemoveColumn(const std::string &column_name) {
    for(auto itr = columns.begin(); itr != columns.end() ; itr++)
        if((*itr)->GetName() == column_name) {
            columns.erase(itr);
            return true;
        }
    return false;
}

bool Table::AddIndex(Index* index) {
    if(std::find(indexes.begin(), indexes.end(), index) != indexes.end())
        return false;
    indexes.push_back(index);
    return true;
}

Index* Table::GetIndex(const std::string &index_name) const {
    for(auto index : indexes)
        if(index->GetName() == index_name)
            return index;
    return nullptr;
}

bool Table::RemoveIndex(const std::string &index_name) {
    for(auto itr = indexes.begin(); itr != indexes.end() ; itr++)
        if((*itr)->GetName() == index_name) {
            indexes.erase(itr);
            return true;
        }
    return false;
}

bool Table::AddConstraint(Constraint* constraint) {
    if(std::find(constraints.begin(), constraints.end(), constraint) != constraints.end())
        return false;
    constraints.push_back(constraint);
    return true;
}

Constraint* Table::GetConstraint(const std::string &constraint_name) const {
    for(auto constraint : constraints)
        if(constraint->GetName() == constraint_name)
            return constraint;
    return nullptr;
}

bool Table::RemoveConstraint(const std::string &constraint_name) {
    for(auto itr = constraints.begin(); itr != constraints.end() ; itr++)
        if((*itr)->GetName() == constraint_name) {
            constraints.erase(itr);
            return true;
        }
    return false;
}

std::ostream& operator<<(std::ostream& os, const Table& table) {

    os << "\tTABLE : " << table.GetName() << "\n";

    for(auto column : table.columns) {
        os << (*column);
    }

    os << "\n";

    for(auto index : table.indexes) {
        os << (*index);
    }

    os << "\n";

    for(auto constraint : table.constraints) {
        os << (*constraint);
    }

    return os;
}


} // End catalog namespace
} // End nstore namespace


