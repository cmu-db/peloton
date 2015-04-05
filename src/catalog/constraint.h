#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Constraint
//===--------------------------------------------------------------------===//

class Index;
class Table;
class ColumnRef;
/**
 * A constraint on a database table
 */
class Constraint : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Constraint>;

public:
    ~Constraint();

    /** GETTER: The type of constraint */
    int32_t GetType() const;

    /** GETTER: (currently unused) */
    const std::string & OnCommit() const;

    /** GETTER: The index used by this constraint (if needed) */
    const Index * GetIndex() const;

    /** GETTER: The table referenced by the foreign key (if needed) */
    const Table * GetForeignKeyTable() const;

    /** GETTER: The columns in the foreign table referenced by the constraint (if needed) */
    const CatalogMap<ColumnRef> & GetForeignKeyCols() const;

protected:
    Constraint(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    int32_t m_type;

    std::string m_on_commit;

    CatalogType* m_index;

    CatalogType* m_foreign_key_table;

    CatalogMap<ColumnRef> m_foreign_key_cols;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);


};

} // End catalog namespace
} // End nstore namespace
