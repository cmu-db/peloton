#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Constraint
//===--------------------------------------------------------------------===//

class Index;
class Table;
class ColumnRef;
/**
 * A constraint on a database table
 */
class Constraint : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Constraint>;

protected:
    Constraint(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    int32_t m_type;
    std::string m_oncommit;
    CatalogType* m_index;
    CatalogType* m_foreignkeytable;
    CatalogMap<ColumnRef> m_foreignkeycols;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Constraint();

    /** GETTER: The type of constraint */
    int32_t type() const;
    /** GETTER: (currently unused) */
    const std::string & oncommit() const;
    /** GETTER: The index used by this constraint (if needed) */
    const Index * index() const;
    /** GETTER: The table referenced by the foreign key (if needed) */
    const Table * foreignkeytable() const;
    /** GETTER: The columns in the foreign table referenced by the constraint (if needed) */
    const CatalogMap<ColumnRef> & foreignkeycols() const;
};

} // End catalog namespace
} // End nstore namespace
