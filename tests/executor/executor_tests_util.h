/**
 * @brief Header file for utility functions for executor tests.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "backend/common/types.h"

namespace nstore {

//===--------------------------------------------------------------------===//
// Utils
//===--------------------------------------------------------------------===//

namespace catalog {
class ColumnInfo;
class Manager;
}

namespace executor {
class AbstractExecutor;
class LogicalTile;
}

namespace storage {
class AbstractBackend;
class TileGroup;
class DataTable;
class Tuple;
}

#define TESTS_TUPLES_PER_TILEGROUP   5
#define DEFAULT_TILEGROUP_COUNT      3

namespace test {

class ExecutorTestsUtil {
public:

    /**
     * @brief Creates a basic tile group with allocated but not populated
     *        tuples.
     */
    static storage::TileGroup *CreateTileGroup(
        storage::AbstractBackend *backend,
        int allocate_tuple_count = TESTS_TUPLES_PER_TILEGROUP);

    /** @brief Creates a basic table with allocated but not populated tuples */
    static storage::DataTable *CreateTable(int tuples_per_tilegroup_count = TESTS_TUPLES_PER_TILEGROUP);

    /** @brief Creates a basic table with allocated and populated tuples */
    static storage::DataTable *CreateAndPopulateTable();

    static void PopulateTable(storage::DataTable *table, int num_rows, bool mutate = false, bool random = false);

    static void PopulateTiles(storage::TileGroup *tile_group, int num_rows);

    static catalog::ColumnInfo GetColumnInfo(int index);

    static executor::LogicalTile *ExecuteTile(
        executor::AbstractExecutor *executor,
        executor::LogicalTile *source_logical_tile);

    /**
     * @brief Returns the value populated at the specified field.
     * @param tuple_id Row of the specified field.
     * @param column_id Column of the specified field.
     *
     * This method defines the values that are populated by PopulateTiles().
     *
     * @return Populated value.
     */
    inline static int PopulatedValue(const oid_t tuple_id, const oid_t column_id) {
        return 10 * tuple_id + column_id;
    }

    static storage::Tuple *GetTuple(storage::DataTable *table, oid_t tuple_id);
    static storage::Tuple *GetNullTuple(storage::DataTable *table);
};

} // namespace test
} // namespace nstore
