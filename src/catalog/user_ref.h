#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// UserRef
//===--------------------------------------------------------------------===//

class User;
class UserRef : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<UserRef>;

public:
    ~UserRef();

    const User * GetUser() const;

protected:
    UserRef(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    CatalogType* m_user;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
