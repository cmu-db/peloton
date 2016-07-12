//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_tuner.cpp
//
// Identification: src/brain/index_tuner.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/index_tuner.h"
#include "brain/clusterer.h"

#include "catalog/schema.h"
#include "common/logger.h"
#include "common/macros.h"
#include "index/index_factory.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"

namespace peloton {
namespace brain {

IndexTuner &IndexTuner::GetInstance() {
  static IndexTuner index_tuner;
  return index_tuner;
}

IndexTuner::IndexTuner(){
  // Nothing to do here !
}

IndexTuner::~IndexTuner(){
  // Nothing to do here !
}

void IndexTuner::Start(){

  // Set signal
  index_tuning_stop = false;

  // Launch thread
  index_tuner_thread = std::thread(&brain::IndexTuner::Tune, this);

}

// Create an ad-hoc index
static void CreateIndex(storage::DataTable* table) {

  // PRIMARY INDEXES
  std::vector<oid_t> key_attrs;
  auto index_count = table->GetIndexCount();
  auto index_oid = index_count + 1;

  auto tuple_schema = table->GetSchema();
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  bool unique;

  key_attrs = {0};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "primary_index",
      index_oid,
      INDEX_TYPE_SKIPLIST,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY,
      tuple_schema,
      key_schema,
      unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);

  // Add index
  table->AddIndex(pkey_index);

}

void IndexTuner::BuildIndex(index::Index *index,
                            storage::DataTable *table) {

  oid_t start_tile_group_count = START_OID;
  oid_t table_tile_group_count = table->GetTileGroupCount();
  auto table_schema = table->GetSchema();
  std::unique_ptr<storage::Tuple> tuple_ptr(new storage::Tuple(table_schema, true));

  while (start_tile_group_count < table_tile_group_count) {
    LOG_TRACE("Build index");

    table_tile_group_count = table->GetTileGroupCount();
    auto tile_group = table->GetTileGroup(start_tile_group_count++);
    auto tile_group_id = tile_group->GetTileGroupId();

    oid_t active_tuple_count = tile_group->GetNextTupleSlot();

    for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
      // Copy over the tuple
      tile_group->CopyTuple(tuple_id, tuple_ptr.get());

      // Set the location
      ItemPointer location(tile_group_id, tuple_id);

      // TODO: Adds an entry in ALL the indexes
      // (should only insert in specific index)
      table->InsertInIndexes(tuple_ptr.get(), location);
    }

    // Update indexed tile group offset (set of tgs indexed)
    index->IncrementIndexedTileGroupOffset();

    // Sleep a bit
    std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
  }

}

void IndexTuner::IndexTuneHelper(storage::DataTable* table) {

  // Process all samples in table
  auto& samples = table->GetIndexSamples();

  // TODO: Check if we have any samples
  if (samples.empty()) {
    //return;
  }

  auto index_count = table->GetIndexCount();

  // Create ad-hoc index if needed
  if(index_count == 0) {
    LOG_TRACE("Create index");
    CreateIndex(table);
  }

  // Build index
  BuildIndex(table->GetIndex(0), table);

  // Clear all samples in table
  table->ClearIndexSamples();

}

void IndexTuner::Tune(){


  // Continue till signal is not false
  while(index_tuning_stop == false) {

    // Go over all tables
    for(auto table : tables) {

      // Update indices periodically
      IndexTuneHelper(table);

    }

  }

}

void IndexTuner::Stop(){

  // Stop tuning
  index_tuning_stop = true;

  // Stop thread
  index_tuner_thread.join();

}

void IndexTuner::AddTable(storage::DataTable* table){
  {
    std::lock_guard<std::mutex> lock(index_tuner_mutex);
    LOG_INFO("table : %p", table);

    tables.push_back(table);
  }
}

void IndexTuner::ClearTables() {
  {
    std::lock_guard<std::mutex> lock(index_tuner_mutex);
    tables.clear();
  }
}


}  // End brain namespace
}  // End peloton namespace
