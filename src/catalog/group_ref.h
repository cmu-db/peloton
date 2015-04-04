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

protected:
    GroupRef(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogType* m_group;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~GroupRef();

    const Group * group() const;
};

} // End catalog namespace
} // End nstore namespace
