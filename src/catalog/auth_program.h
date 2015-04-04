#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// AuthProgram
//===--------------------------------------------------------------------===//

/**
 * The name of a program with access to a specific procedure. This is effectively a weak reference to a 'program'
 */
class AuthProgram : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<AuthProgram>;

protected:
    AuthProgram(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool RemoveChild(const std::string &collectionName, const std::string &childName);

public:
    ~AuthProgram();

};

} // End catalog namespace
} // End nstore namespace
