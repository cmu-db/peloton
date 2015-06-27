/*-------------------------------------------------------------------------
 *
 * table.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/table.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/catalog/abstract_catalog_object.h"
#include "backend/catalog/column.h"
#include "backend/catalog/index.h"

#include <iostream>


namespace nstore {

namespace storage {
class DataTable;
}

namespace catalog {

/**
 * Table Catalog Object 
 */
class Table : public AbstractCatalogObject {

public:

    Table(std::string name)
        : AbstractCatalogObject(static_cast<oid_t>(1), name) { // FIXME
        // Light em up!
    }

    ~Table() {

        // clean up indices
        for(auto index : indexes)
            delete index;

        // clean up constraints
        for(auto constraint : constraints)
            delete constraint;

        // clean up columns
        for(auto col : columns)
            delete col;
    }
    
    //===--------------------------------------------------------------------===//
    // ACCESSORS
    //===--------------------------------------------------------------------===//

    std::vector<Column*> GetColumns() const {
        return columns;
    }

    std::vector<Index*> GetIndices() const {
        return indexes;
    }

    std::vector<Constraint*> GetConstraints() const {
        return constraints;
    }

    // TODO: REMOVE THIS!
    storage::DataTable *GetTable() const {
        return physical_table;
    }

    // TODO: REMOVE THIS!
    void SetPhysicalTable(storage::DataTable* table_) {
        physical_table = table_;
    }

    // TODO: REMOVE THIS!
    storage::DataTable *GetPhysicalTable() {
        return physical_table;
    }

    bool AddColumn(Column* column);
    Column* GetColumn(const std::string &column_name) const;
    bool RemoveColumn(const std::string &column_name);

    bool AddIndex(Index* index);
    Index* GetIndex(const std::string &index_name) const;
    bool RemoveIndex(const std::string &index_name);

    bool AddConstraint(Constraint* constraint);
    Constraint* GetConstraint(const std::string &constraint_name) const;
    bool RemoveConstraint(const std::string &constraint_name);

    // Get a string representation of this table
    friend std::ostream& operator<<(std::ostream& os, const Table& table);
    

private:
    
    //===--------------------------------------------------------------------===//
    // MEMBERS
    //===--------------------------------------------------------------------===//

    // columns in table
    std::vector<Column*> columns;

    // indexes for table
    std::vector<Index*> indexes;

    // constraints for column
    std::vector<Constraint*> constraints;

    // underlying physical table
    storage::DataTable* physical_table = nullptr; // TODO: REMOVE THIS!

};

} // End catalog namespace
} // End nstore namespace

