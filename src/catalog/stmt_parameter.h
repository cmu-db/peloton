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

 public:
  ~StmtParameter();

  /** GETTER: The SQL type of the parameter (int/float/date/etc) */
  int32_t GetSqlType() const;

  /** GETTER: The Java class of the parameter (int/float/date/etc) */
  int32_t GetJavaType() const;

  /** GETTER: The index of the parameter in the set of statement parameters */
  int32_t GetIndex() const;

  /** GETTER: Reference back to original input parameter */
  const ProcParameter * GetProcParameter() const;

  /** GETTER: If the ProcParameter is an array, which index in that array are we paired to */
  int32_t GetProcParameterOffset() const;

 protected:
  StmtParameter(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

  int32_t m_sql_type;

  int32_t m_java_type;

  int32_t m_index;

  CatalogType* m_proc_parameter;

  int32_t m_proc_parameter_offset;

  virtual void Update();

  virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
  virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
  virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
