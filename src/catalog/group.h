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

public:
    ~Group();

    const CatalogMap<UserRef> & users() const;

    /** GETTER: Can invoke system procedures */
    bool CanInvokeSysProc() const;

    /** GETTER: Can invoke the adhoc system procedure */
    bool CanInvokeAdhoc() const;

protected:
    Group(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    CatalogMap<UserRef> m_users;

    bool m_sysproc;

    bool m_adhoc;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
