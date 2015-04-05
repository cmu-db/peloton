#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Index
//===--------------------------------------------------------------------===//

class ColumnRef;
/**
 * A index structure on a database table's columns
 */
class Index : public CatalogType {
  friend class Catalog;
  friend class CatalogMap<Index>;

 public:
  ~Index();

  /** GETTER: May the index contain duplicate keys? */
  bool IsUnique() const;

  /** GETTER: What data structure is the index using and what kinds of keys does it support? */
  int32_t GetType() const;

  /** GETTER: Columns referenced by the index */
  const CatalogMap<ColumnRef> & GetColumns() const;

 protected:
  Index(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

  CatalogMap<ColumnRef> m_columns;

  bool m_unique;

  int32_t m_type;

  virtual void Update();

  virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
  virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
  virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
