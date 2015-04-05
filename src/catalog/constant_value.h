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
 * What John Hugg doesn't want me to have :D
 */
class ConstantValue : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<ConstantValue>;
public:
    ~ConstantValue();

    /** GETTER: A string representation of the value */
    const std::string & GetValue() const;

    /** GETTER: Whether the value is null */
    bool IsNull() const;

    /** GETTER: The type of the value (int/double/date/etc) */
    int32_t GetType() const;

protected:
    ConstantValue(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    std::string m_value;

    bool m_is_null;

    int32_t m_type;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
