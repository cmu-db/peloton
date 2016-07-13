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

// Add an ad-hoc index
static void AddIndex(storage::DataTable* table,
                     std::set<oid_t> suggested_index_attrs) {

  // Construct index metadata
  std::vector<oid_t> key_attrs(suggested_index_attrs.size());
  std::copy(suggested_index_attrs.begin(),
            suggested_index_attrs.end(),
            key_attrs.begin());

  auto index_count = table->GetIndexCount();
  auto index_oid = index_count + 1;

  auto tuple_schema = table->GetSchema();
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  bool unique;

  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "adhoc_index_" + std::to_string(index_oid),
      index_oid,
      INDEX_TYPE_SKIPLIST,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY,
      tuple_schema,
      key_schema,
      key_attrs,
      unique);

  index::Index *adhoc_index = index::IndexFactory::GetInstance(index_metadata);

  // Add index
  table->AddIndex(adhoc_index);

  LOG_INFO("Added suggested index");
}

void IndexTuner::BuildIndex(storage::DataTable *table,
                            index::Index *index) {

  auto table_schema = table->GetSchema();
  auto index_tile_group_offset = index->GetIndexedTileGroupOff();
  auto table_tile_group_count = table->GetTileGroupCount();
  oid_t tile_groups_indexed = 0;

  while (index_tile_group_offset < table_tile_group_count &&
      (tile_groups_indexed < max_tile_groups_indexed)) {

    std::unique_ptr<storage::Tuple> tuple_ptr(new storage::Tuple(table_schema, true));

    auto tile_group = table->GetTileGroup(index_tile_group_offset);
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

    index_tile_group_offset++;
    tile_groups_indexed++;
  }

}

void IndexTuner::BuildIndices(storage::DataTable *table) {

  oid_t index_count = table->GetIndexCount();

  for(oid_t index_itr = 0; index_itr < index_count; index_itr++){

    // Get index
    auto index = table->GetIndex(index_itr);

    // Build index
    BuildIndex(table, index);
  }

}

typedef std::pair<brain::Sample, oid_t> sample_frequency_map_entry;

bool sample_frequency_entry_comparator(sample_frequency_map_entry a,
                                       sample_frequency_map_entry b){
  return a.second > b.second;
}

void IndexTuner::Analyze(storage::DataTable* table) {

  // Process all samples in table
  auto& samples = table->GetIndexSamples();
  auto sample_count = samples.size();

  // Check if we have sufficient number of samples
  if (sample_count < sample_count_threshold) {
    return;
  }

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
  oid_t index_count = table->GetIndexCount();

  for(auto suggested_index : suggested_indices) {

    std::set<oid_t> suggested_index_set(suggested_index.begin(),
                                        suggested_index.end());

    // Go over all indices
    bool suggested_index_found = false;
    for(oid_t index_itr = 0; index_itr < index_count; index_itr++){

      auto index_attrs = table->GetIndexAttrs(index_itr);

      // Some attribute did not match
      if(index_attrs != suggested_index_set) {
        continue;
      }

      // Exact match
      suggested_index_found = true;
      break;
    }

    // Did we find suggested index ?
    if(suggested_index_found == false) {
      LOG_INFO("Did not find suggested index. Going to create it.");

      // Add adhoc index
      AddIndex(table, suggested_index_set);

      LOG_INFO("-------------------");
    }
    else {
      LOG_INFO("Found suggested index.");
    }

  }

  // Clear all current samples in table
  table->ClearIndexSamples();

}

void IndexTuner::IndexTuneHelper(storage::DataTable* table) {

  // Add required indices
  Analyze(table);

  // Build desired indices
  BuildIndices(table);

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
