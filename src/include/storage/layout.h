//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// layout.h
//
// Identification: src/include/storage/layout.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>

#include "common/internal_types.h"
#include "common/printable.h"

//@{
/** @brief  Pre-defined OIDs for ROW and COLUMN store.  */
#define ROW_STORE_OID 0
#define COLUMN_STORE_OID 1
//@}

namespace peloton {

namespace catalog {
class Schema;
}

namespace storage {

/**
 * @brief   Class to store the physical layout of a TileGroup.
 */
class Layout : public Printable {
 public:

  /**
   * @brief   Constructor for predefined layout types.
   * @param   num_columns Number of columns in the layouts.
   * @param   layout_type Has to be LayoutType::ROW or LayoutType::COLUMN.
   */
  Layout (const oid_t num_columns, LayoutType layout_type = LayoutType::ROW);

  /**
   * @brief   Constructor for arbitrary column_maps.
   *          If the column_map is of type LayoutType::HYBRID,
   *          the layout_oid_ is set to INVALID_OID. For all other
   *          layouts, the pre-defined OIDs are used. Should be used
   *          only for testing & TempTables, the layout isn't persistent.
   * @param   column_map Column map of the layout to be constructed
   *
   */
  Layout(const column_map_type& column_map);

  /**
   * @brief   Constructor for arbitrary column_maps.
   * @param   column_map Column map of the layout to be constructed.
   * @param   layout_oid Per-table unique OID. Generted by DataTable.
   */
  Layout(const column_map_type& column_map, oid_t layout_oid);

  /** @brief  Check whether this layout is a row store. */
  inline bool IsRowStore() const { return (layout_type_ == LayoutType::ROW); }

  /** @brief  Check whether this layout is a column store. */
  inline bool IsColumnStore() const { return (layout_type_ == LayoutType::COLUMN); }

  /** @brief  Return the layout_oid_ of this object. */
  oid_t  GetOid() const { return layout_oid_; }

  /** @brief  Sets the tile id and column id w.r.t. of the tile corresponding
   *          to the specified tile group column id.
   */
  void LocateTileAndColumn(oid_t column_offset, oid_t &tile_offset,
                           oid_t &tile_column_offset) const;

  /** @brief    Returns the layout difference w.r.t. the other Layout.
   *  @double   The delta between the layouts. Used by LayoutTuner class.
   */
  double GetLayoutDifference(const storage::Layout &other) const;

  /** @brief  Returns the tile_id of the given column_id. */
  oid_t GetTileIdFromColumnId(oid_t column_id) const;

  /** @brief  Returns the column offset of the column_id. */
  oid_t GetTileColumnOffset(oid_t column_id) const;

  /** @brief  Returns the number of columns in the layout. */
  oid_t GetColumnCount() const { return num_columns_;}

  /** @brief  Constructs the schema for the given layout.  Thid function
   *          is used only in TempTables and LogicalTiles.
   */
  std::vector<catalog::Schema> GetLayoutSchemas(catalog::Schema* const schema) const;

  /** @brief  Returns the layout statistics used by the LayoutTuner. */
  std::map<oid_t, oid_t> GetLayoutStats() const;

  /** @brief  Serialzies the column_map to be added to pg_layout. */
  std::string SerializeColumnMap() const;

  /** @brief  Deserializes the string that has been read from pg_layout. */
  static column_map_type DeserializeColumnMap(oid_t num_columns,
                                              std::string column_map_str);

  /** @brief  Returns a string containing the column_map of this layout. */
  std::string GetColumnMapInfo() const;

  /** @brief  Returns a string representation for debugging function. */
  const std::string GetInfo() const;

  //@{
  /** @brief  Operators for checking equality of two layouts. */
  friend bool operator==(const Layout& lhs, const Layout& rhs);
  friend bool operator!=(const Layout& lhs, const Layout& rhs);
  //@}

 private:

  /** @brief  Layout Oid of the layout object. */
  oid_t layout_oid_;

  /** @brief  Number of columns in the layout. */
  oid_t num_columns_; 

  /** @brief  column_map of the layout. */
  column_map_type column_layout_;

  /** @brief  Layout type is always (ROW, COLUMN or HYBRID). */
  LayoutType layout_type_;

};

} //namespace storage
} //namespace peloton