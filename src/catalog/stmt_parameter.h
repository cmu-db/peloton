#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// StmtParameter
//===--------------------------------------------------------------------===//

class ProcParameter;
/**
 * A parameter for a parameterized SQL statement
 */
class StmtParameter : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<StmtParameter>;

protected:
    StmtParameter(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    int32_t m_sqltype;
    int32_t m_javatype;
    int32_t m_index;
    CatalogType* m_procparameter;
    int32_t m_procparameteroffset;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~StmtParameter();

    /** GETTER: The SQL type of the parameter (int/float/date/etc) */
    int32_t sqltype() const;
    /** GETTER: The Java class of the parameter (int/float/date/etc) */
    int32_t javatype() const;
    /** GETTER: The index of the parameter in the set of statement parameters */
    int32_t index() const;
    /** GETTER: Reference back to original input parameter */
    const ProcParameter * procparameter() const;
    /** GETTER: If the ProcParameter is an array, which index in that array are we paired to */
    int32_t procparameteroffset() const;
};

} // End catalog namespace
} // End nstore namespace
