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

protected:
    UserRef(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogType* m_user;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~UserRef();

    const User * user() const;
};

} // End catalog namespace
} // End nstore namespace
