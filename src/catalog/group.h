#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Group
//===--------------------------------------------------------------------===//

class UserRef;
class Group : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Group>;

protected:
    Group(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogMap<UserRef> m_users;
    bool m_sysproc;
    bool m_adhoc;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Group();

    const CatalogMap<UserRef> & users() const;
    /** GETTER: Can invoke system procedures */
    bool sysproc() const;
    /** GETTER: Can invoke the adhoc system procedure */
    bool adhoc() const;
};

} // End catalog namespace
} // End nstore namespace
