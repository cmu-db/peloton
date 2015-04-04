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

protected:
    ProcParameter(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    int32_t m_type;
    bool m_isarray;
    int32_t m_index;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~ProcParameter();

    /** GETTER: The data type for the parameter (int/float/date/etc) */
    int32_t type() const;
    /** GETTER: Is the parameter an array of value */
    bool isarray() const;
    /** GETTER: The index of the parameter within the list of parameters for the stored procedure */
    int32_t index() const;
};

} // End catalog namespace
} // End nstore namespace
