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

public:
    ~ConflictSet();

    /** GETTER: The other procedure that this conflict set is for */
    const Procedure * GetProcedure() const;

    /** GETTER: ConflictPairs that the parent Procedure has a read-write conflict with the target procedure */
    const CatalogMap<ConflictPair> & GetReadWriteConflicts() const;

    /** GETTER: ConflictPairs that the parent Procedure has a write-write conflict with the target procedure */
    const CatalogMap<ConflictPair> & GetWriteWriteConflicts() const;

protected:
    ConflictSet(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    CatalogType* m_procedure;

    CatalogMap<ConflictPair> m_read_write_conflicts;

    CatalogMap<ConflictPair> m_write_write_conflicts;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
