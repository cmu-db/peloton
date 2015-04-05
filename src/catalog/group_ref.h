#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// GroupRef
//===--------------------------------------------------------------------===//

class Group;
class GroupRef : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<GroupRef>;

public:
    ~GroupRef();

    const Group * GetGroup() const;

protected:
    GroupRef(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogType* m_group;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
