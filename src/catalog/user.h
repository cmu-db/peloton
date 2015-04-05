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

public:
    ~User();

    const CatalogMap<GroupRef> & GetGroups() const;

    /** GETTER: Can invoke system procedures */
    bool CanInvokeSysproc() const;

    /** GETTER: Can invoke the adhoc system procedure */
    bool CanInvokeAdhoc() const;

    /** GETTER: SHA-1 double hashed hex encoded version of the password */
    const std::string & GetShadowPassword() const;

protected:
    User(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    CatalogMap<GroupRef> m_groups;

    bool m_sysproc;

    bool m_adhoc;

    std::string m_shadow_password;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
