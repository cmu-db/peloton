#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// ConstraintRef
//===--------------------------------------------------------------------===//

class Constraint;
/**
 * A reference to a table constraint
 */
class ConstraintRef : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<ConstraintRef>;

protected:
    ConstraintRef(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogType* m_constraint;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~ConstraintRef();

    /** GETTER: The constraint that is referenced */
    const Constraint * constraint() const;
};

} // End catalog namespace
} // End nstore namespace
