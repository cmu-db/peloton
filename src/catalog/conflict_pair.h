#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// ConflictPair
//===--------------------------------------------------------------------===//

class Statement;
class TableRef;
/**
 * A pair of Statements that have a conflict
 */
class ConflictPair : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<ConflictPair>;

public:
    ~ConflictPair();

    /** GETTER: The source Statement */
    const Statement *GetSourceStatement() const;

    /** GETTER: The destination Statement */
    const Statement *GetDestinationStatement() const;

    /** GETTER: The list of tables that caused this conflict */
    const CatalogMap<TableRef>& GetTables() const;

    /** GETTER: If true, then this ConflictPair will always cause a conflict */
    bool IsAlwaysConflicting() const;

    /** GETTER: Type of conflict (ConflictType) */
    int32_t GetConflictType() const;

protected:

    ConflictPair(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name);

    CatalogType *m_source_statement;

    CatalogType *m_destination_statement;

    CatalogMap<TableRef> m_tables;

    bool m_always_conflicting;

    int32_t m_conflict_type;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
