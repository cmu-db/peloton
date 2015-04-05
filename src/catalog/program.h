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

public:
    ~Program();

protected:
    Program(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
