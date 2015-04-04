#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// ConflictSet
//===--------------------------------------------------------------------===//

class Procedure;
class ConflictPair;
/**
 * A set of conflicts with another procedures
 */
class ConflictSet : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<ConflictSet>;

protected:
    ConflictSet(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogType* m_procedure;
    CatalogMap<ConflictPair> m_readWriteConflicts;
    CatalogMap<ConflictPair> m_writeWriteConflicts;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~ConflictSet();

    /** GETTER: The other procedure that this conflict set is for */
    const Procedure * procedure() const;
    /** GETTER: ConflictPairs that the parent Procedure has a read-write conflict with the target procedure */
    const CatalogMap<ConflictPair> & readWriteConflicts() const;
    /** GETTER: ConflictPairs that the parent Procedure has a write-write conflict with the target procedure */
    const CatalogMap<ConflictPair> & writeWriteConflicts() const;
};

} // End catalog namespace
} // End nstore namespace
