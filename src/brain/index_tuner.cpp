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

#include<unordered_map>

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
    //std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
  }

}

typedef std::pair<brain::Sample, oid_t> sample_frequency_map_entry;

bool sample_frequency_entry_comparator(sample_frequency_map_entry a,
                                       sample_frequency_map_entry b){
    return a.second > b.second;
}

void IndexTuner::Analyze(UNUSED_ATTRIBUTE storage::DataTable* table,
                         const std::vector<brain::Sample>& samples) {

  double rd_wr_ratio = 0;

  double total_rd_duration = 0;
  double total_wr_duration = 0;
  double max_rd_wr_ratio = 10000;

  std::unordered_map<brain::Sample, oid_t> sample_frequency_map;

  // Go over all samples
  for(auto sample : samples){

    if(sample.sample_type_ == SAMPLE_TYPE_ACCESS){
      total_rd_duration += sample.weight_;

      // Update sample count
      sample_frequency_map[sample]++;
    }
    else if(sample.sample_type_ == SAMPLE_TYPE_UPDATE){
      total_wr_duration += sample.weight_;

      // Ignore update samples
    }
    else {
      throw Exception("Unknown sample type : " + std::to_string(sample.sample_type_));
    }

  }

  // Find frequent samples
  size_t frequency_rank_threshold = 3;

  std::vector<sample_frequency_map_entry> sample_frequency_entry_list;

  for(auto sample_frequency_map_entry : sample_frequency_map){
    auto entry = std::make_pair(sample_frequency_map_entry.first,
                                sample_frequency_map_entry.second);

    sample_frequency_entry_list.push_back(entry);
  }

  std::sort(sample_frequency_entry_list.begin(), sample_frequency_entry_list.end(),
            sample_frequency_entry_comparator);

  // Print top-k frequent samples for table
  std::vector<std::vector<double>> suggested_indices;

  for(size_t entry_itr = 0;
      entry_itr < frequency_rank_threshold && entry_itr < sample_frequency_entry_list.size();
      entry_itr++){
    auto& entry = sample_frequency_entry_list[entry_itr];
    auto& sample = entry.first;
    LOG_INFO("%s Frequency : %u", sample.GetInfo().c_str(), entry.second);

    // Add to suggested index list
    suggested_indices.push_back(sample.columns_accessed_);
  }

  // Compute read write ratio
  if(total_wr_duration == 0) {
    rd_wr_ratio = max_rd_wr_ratio;
  }
  else{
    rd_wr_ratio = total_rd_duration / total_wr_duration;
  }

  LOG_INFO("Read Write Ratio : %.2lf", rd_wr_ratio);


  // TODO: Use read write ratio to throttle index creation

  // Construct indices in suggested index list

}

void IndexTuner::IndexTuneHelper(storage::DataTable* table) {

  // Process all samples in table
  auto& samples = table->GetIndexSamples();
  auto sample_count = samples.size();

  // Check if we have sufficient number of samples
  if (sample_count < sample_count_threshold) {
    return;
  }

  Analyze(table, samples);

  LOG_INFO("Found %lu samples", samples.size());
  auto index_count = table->GetIndexCount();

  // Create ad-hoc index if needed
  if(index_count == 0) {
    LOG_INFO("Create index");
    CreateIndex(table);
  }

  // Build index
  //BuildIndex(table->GetIndex(0), table);

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
