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

public:
    ~ConstraintRef();

    /** GETTER: The constraint that is referenced */
    const Constraint * GetConstraint() const;

protected:
    ConstraintRef(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogType* m_constraint;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
