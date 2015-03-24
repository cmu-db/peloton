/*-------------------------------------------------------------------------
 *
 * logical_schema.h
 * Schema for logical tile.
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/executor/logical_schema.h
 *
 *-------------------------------------------------------------------------
 */

 #pragma once

 #include <utility>
 #include <vector>

 #include "common/types.h"

 namespace nstore {
 namespace executor {

 class LogicalSchema {
  private:
   // Pairs of base tile id and column id (in that base tile) that a given column belongs to.
   std::vector<std::pair<oid_t, id_t> > origin_columns;
 };

 } // namespace executor
 } // namespace nstore
