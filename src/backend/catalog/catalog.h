/*-------------------------------------------------------------------------
 *
 * catalog.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/catalog.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/catalog/database.h"

#include <iostream>
#include <mutex>

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Catalog
//===--------------------------------------------------------------------===//

class Catalog {

public:

    /**
     * @deprecated We need to get rid of this!
     */
    static Catalog& GetInstance();

    // create default db
    Catalog() {
        Database* db = new Database("default"); // FIXME
        databases.push_back(db);
    }

    ~Catalog() {
        // clean up databases
        for(auto db : databases)
            delete db;
    }

    bool AddDatabase(Database* db);
    Database* GetDatabase(const std::string &db_name) const;
    bool RemoveDatabase(const std::string &db_name);

    // Get a string representation of this catalog
    friend std::ostream& operator<<(std::ostream& os, const Catalog& catalog);

    void Lock() {
        catalog_mtx.lock();
    }

    void Unlock() {
        catalog_mtx.unlock();
    }

private:

    // list of databases in catalog
    std::vector<Database*> databases;

    std::mutex catalog_mtx;

};

} // End catalog namespace
} // End nstore namespace
