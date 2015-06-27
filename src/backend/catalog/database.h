/*-------------------------------------------------------------------------
 *
 * database.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/catalog/abstract_catalog_object.h"
#include "backend/catalog/table.h"

#include <iostream>
#include <mutex>

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Database
//===--------------------------------------------------------------------===//

class Database : public AbstractCatalogObject {

public:

    Database(std::string name)
        : AbstractCatalogObject(static_cast<oid_t>(1), name) { // FIXME
    }

    ~Database() {

        // clean up tables
        for(auto table : tables) {
            delete table;
        }
    }
    
    //===--------------------------------------------------------------------===//
    // ACCESSORS
    //===--------------------------------------------------------------------===//

    bool AddTable(Table* table);
    Table* GetTable(const std::string &table_name) const;
    bool RemoveTable(const std::string &table_name);

    // Get a string representation of this database
    friend std::ostream& operator<<(std::ostream& os, const Database& database);

private:
    
    //===--------------------------------------------------------------------===//
    // MEMBERS
    //===--------------------------------------------------------------------===//

    // tables in db
    std::vector<Table*> tables;
};

} // End catalog namespace
} // End nstore namespace
