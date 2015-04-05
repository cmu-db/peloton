#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// ProcParameter
//===--------------------------------------------------------------------===//

/**
 * Metadata for a parameter to a stored procedure
 */
class ProcParameter : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<ProcParameter>;

public:
    ~ProcParameter();

    /** GETTER: The data type for the parameter (int/float/date/etc) */
    int32_t GetType() const;

    /** GETTER: Is the parameter an array of value */
    bool IsArray() const;

    /** GETTER: The index of the parameter within the list of parameters for the stored procedure */
    int32_t GetIndex() const;

protected:
    ProcParameter(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    int32_t m_type;

    bool m_is_array;

    int32_t m_index;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
