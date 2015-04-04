#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// ConstantValue
//===--------------------------------------------------------------------===//

/**
 * What John Hugg doesn't want me to have
 */
class ConstantValue : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<ConstantValue>;

protected:
    ConstantValue(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    std::string m_value;
    bool m_is_null;
    int32_t m_type;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~ConstantValue();

    /** GETTER: A string representation of the value */
    const std::string & value() const;
    /** GETTER: Whether the value is null */
    bool is_null() const;
    /** GETTER: The type of the value (int/double/date/etc) */
    int32_t type() const;
};

} // End catalog namespace
} // End nstore namespace
