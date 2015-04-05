#include "catalog_type.h"

#include <cctype>
#include <cstdlib>
#include "catalog.h"
#include "catalog_map.h"
#include "common/exception.h"

namespace nstore {
namespace catalog {

CatalogType::CatalogType(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string & name) {
  m_catalog = catalog;
  m_parent = parent;
  m_name = name;
  m_path = path;
  m_relativeIndex = -1;

  if (this != m_catalog) {
    m_catalog->RegisterGlobally(this);
  }
}

CatalogType::~CatalogType() {
  if (this != m_catalog) {
    m_catalog->UnregisterGlobally(this);
  }
}

void CatalogType::Set(const std::string &field, const std::string &value) {
  CatalogValue val;
  int32_t indicator = tolower(value[0]);
  // paths
  if (indicator == '/') {
    //printf("Adding a path ref for %s[%s]:\n    %s\n", path().c_str(), field.c_str(), value.c_str());
    //fflush(stdout);
    CatalogType *type = m_catalog->ItemForRef(value);
    if (!type) {
      //printf("Adding unresolved info for path:\n    %s\n", value.c_str());
      //fflush(stdout);
      m_catalog->AddUnresolvedInfo(value, this, field);
      Update();
      return;
    }
    val.typeValue = type;
  }
  // null paths
  else if (indicator == 'n')
    val.intValue = 0;
  // std::strings
  else if (indicator == '\"')
    val.strValue = value.substr(1, value.length() - 2);
  // boolean (true)
  else if (indicator == 't')
    val.intValue = 1;
  // boolean (false)
  else if (indicator == 'f')
    val.intValue = 0;
  // Integers (including negatives)
  else if (isdigit(indicator) || (indicator == '-' && value.length() > 1 && isdigit(value[1])))
    val.intValue = atoi(value.c_str());
  else {
    std::string msg = "Invalid value '" + value + "' for field '" + field + "'";
    throw CatalogException(msg.c_str());
  }

  m_fields[field] = val;
  Update();
}

std::string CatalogType::GetName() const {
  return m_name;
}

std::string CatalogType::GetPath() const {
  return m_path;
}

CatalogType * CatalogType::GetParent() const {
  return m_parent;
}

Catalog *CatalogType::GetCatalog() const {
  return m_catalog;
}

int32_t CatalogType::GetRelativeIndex() const {
  return m_relativeIndex;
}

} // End catalog namespace
} // End nstore namespace

