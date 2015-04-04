#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// ConflictPair
//===--------------------------------------------------------------------===//

class Statement;
class TableRef;
/**
 * A pair of Statements that have a conflict
 */
class ConflictPair : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<ConflictPair>;

protected:
    ConflictPair(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogType* m_statement0;
    CatalogType* m_statement1;
    CatalogMap<TableRef> m_tables;
    bool m_alwaysConflicting;
    int32_t m_conflictType;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~ConflictPair();

    /** GETTER: The source Statement */
    const Statement * statement0() const;
    /** GETTER: The destination Statement */
    const Statement * statement1() const;
    /** GETTER: The list of tables that caused this conflict */
    const CatalogMap<TableRef> & tables() const;
    /** GETTER: If true, then this ConflictPair will always cause a conflict */
    bool alwaysConflicting() const;
    /** GETTER: Type of conflict (ConflictType) */
    int32_t conflictType() const;
};

} // End catalog namespace
} // End nstore namespace
