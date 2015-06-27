/*-------------------------------------------------------------------------
 *
 * database.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include "backend/catalog/database.h"

namespace nstore {
namespace catalog {

bool Database::AddTable(Table* table) {
    if(std::find(tables.begin(), tables.end(), table) != tables.end())
        return false;
    tables.push_back(table);
    return true;
}

Table* Database::GetTable(const std::string &table_name) const {
    for(auto table : tables)
        if(table->GetName() == table_name)
            return table;
    return nullptr;
}

bool Database::RemoveTable(const std::string &table_name) {
    for(auto itr = tables.begin(); itr != tables.end() ; itr++)
        if((*itr)->GetName() == table_name) {
            tables.erase(itr);
            return true;
        }
    return false;
}

std::ostream& operator<<(std::ostream& os, const Database& database) {

    os << "\t-----------------------------------------------------------\n";
    os << "\tDATABASE " << database.GetName() << "\n\n";
    for(auto table : database.tables) {
        os << (*table);
    }
    os << "\t-----------------------------------------------------------\n";

    return os;
}


} // End catalog namespace
} // End nstore namespace

