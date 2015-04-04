#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// User
//===--------------------------------------------------------------------===//

class GroupRef;
class User : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<User>;

protected:
    User(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogMap<GroupRef> m_groups;
    bool m_sysproc;
    bool m_adhoc;
    std::string m_shadowPassword;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~User();

    const CatalogMap<GroupRef> & groups() const;
    /** GETTER: Can invoke system procedures */
    bool sysproc() const;
    /** GETTER: Can invoke the adhoc system procedure */
    bool adhoc() const;
    /** GETTER: SHA-1 double hashed hex encoded version of the password */
    const std::string & shadowPassword() const;
};

} // End catalog namespace
} // End nstore namespace
