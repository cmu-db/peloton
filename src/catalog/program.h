#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Program
//===--------------------------------------------------------------------===//

/**
 * The name of a program with access (effectively a username for an app server)
 */
class Program : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Program>;

protected:
    Program(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Program();

};

} // End catalog namespace
} // End nstore namespace
