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

#include <algorithm>
#include "brain/brain_util.h"

#include "concurrency/transaction_manager_factory.h"
#include "brain/clusterer.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/container_tuple.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/timer.h"
#include "index/index_factory.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"

namespace peloton {
namespace brain {

IndexTuner& IndexTuner::GetInstance() {
  static IndexTuner index_tuner;
  return index_tuner;
}

IndexTuner::IndexTuner() {
  tile_groups_indexed_ = 0;
  // Nothing to do here !
}

IndexTuner::~IndexTuner() {}

void IndexTuner::Start() {
  // Set signal
  index_tuning_stop = false;

  // Launch thread
  index_tuner_thread = std::thread(&brain::IndexTuner::Tune, this);

  LOG_INFO("Started index tuner");
}

// Add an ad-hoc index
static void AddIndex(storage::DataTable* table,
                     std::set<oid_t> suggested_index_attrs) {
  // Construct index metadata
  std::vector<oid_t> key_attrs(suggested_index_attrs.size());
  std::copy(suggested_index_attrs.begin(), suggested_index_attrs.end(),
            key_attrs.begin());

  auto index_count = table->GetIndexCount();
  auto index_oid = index_count + 1;

  auto tuple_schema = table->GetSchema();
  catalog::Schema* key_schema;
  index::IndexMetadata* index_metadata;
  bool unique;

  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "adhoc_index_" + std::to_string(index_oid), index_oid, table->GetOid(),
      table->GetDatabaseOid(), IndexType::BWTREE,
      IndexConstraintType::PRIMARY_KEY, tuple_schema, key_schema, key_attrs,
      unique);

  // Set initial utility ratio
  double intial_utility_ratio = 0.5;
  index_metadata->SetUtility(intial_utility_ratio);

  std::shared_ptr<index::Index> adhoc_index(
      index::IndexFactory::GetIndex(index_metadata));

  // Add index
  table->AddIndex(adhoc_index);

  LOG_DEBUG("Creating index : %s", index_metadata->GetInfo().c_str());
}

void IndexTuner::BuildIndex(storage::DataTable* table,
                            std::shared_ptr<index::Index> index) {
  auto table_schema = table->GetSchema();
  auto index_tile_group_offset = index->GetIndexedTileGroupOff();
  auto table_tile_group_count = table->GetTileGroupCount();
  oid_t tile_groups_indexed = 0;

  auto index_schema = index->GetKeySchema();
  auto indexed_columns = index_schema->GetIndexedColumns();
  std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));

  while (index_tile_group_offset < table_tile_group_count &&
         (tile_groups_indexed < tile_groups_indexed_per_iteration)) {
    std::unique_ptr<storage::Tuple> tuple_ptr(
        new storage::Tuple(table_schema, true));

    auto tile_group = table->GetTileGroup(index_tile_group_offset);
    auto tile_group_id = tile_group->GetTileGroupId();
    oid_t active_tuple_count = tile_group->GetNextTupleSlot();

    for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
      // Setup container tuple
      ContainerTuple<storage::TileGroup> container_tuple(tile_group.get(),
                                                         tuple_id);

      // Set the location
      ItemPointer location(tile_group_id, tuple_id);

      // Set the key
      key->SetFromTuple(&container_tuple, indexed_columns, index->GetPool());

      // Insert in specific index
      // TODO: Allocate itempointer ?
      index->InsertEntry(key.get(), &location);
    }

    // Update indexed tile group offset (set of tgs indexed)
    index->IncrementIndexedTileGroupOffset();

    index_tile_group_offset++;
    tile_groups_indexed++;
  }

  tile_groups_indexed_ += tile_groups_indexed;
}

void IndexTuner::BuildIndices(storage::DataTable* table) {
  oid_t index_count = table->GetIndexCount();

  for (oid_t index_itr = 0; index_itr < index_count; index_itr++) {
    // Get index
    auto index = table->GetIndex(index_itr);
    if (index == nullptr) {
      continue;
    }

    // Build index
    // BuildIndex(table, index);
  }
}

double IndexTuner::ComputeWorkloadWriteRatio(
    const std::vector<brain::Sample>& samples) {
  double write_ratio = 0;

  double total_read_duration = 0;
  double total_write_duration = 0;

  // Go over all samples
  for (auto sample : samples) {
    if (sample.GetSampleType() == SampleType::ACCESS) {
      total_read_duration += sample.GetWeight();
    } else if (sample.GetSampleType() == SampleType::UPDATE) {
      total_write_duration += sample.GetWeight();
    } else {
      throw Exception("Unknown sample type : " +
                      std::to_string(static_cast<int>(sample.GetSampleType())));
    }
  }

  // Compute write ratio
  auto total_duration = total_read_duration + total_write_duration;
  PL_ASSERT(total_duration > 0);
  write_ratio = total_write_duration / (total_duration);

  // Compute exponential moving average
  if (average_write_ratio == INVALID_RATIO) {
    average_write_ratio = write_ratio;
  } else {
    // S_t = alpha * Y_t + (1 - alpha) * S_t-1
    average_write_ratio =
        write_ratio * alpha + (1 - alpha) * average_write_ratio;
  }

  LOG_TRACE("Average write Ratio : %.2lf", average_write_ratio);

  return average_write_ratio;
}

typedef std::pair<brain::Sample, double> sample_frequency_map_entry;

bool SampleFrequencyMapEntryComparator(sample_frequency_map_entry a,
                                       sample_frequency_map_entry b) {
  return a.second > b.second;
}

std::vector<sample_frequency_map_entry> GetFrequentSamples(
    const std::vector<brain::Sample>& samples) {
  std::unordered_map<brain::Sample, double> sample_frequency_map;
  double total_weight = 0;

  // Go over all samples
  for (auto sample : samples) {
    if (sample.GetSampleType() == SampleType::ACCESS) {
      // Update sample count
      sample_frequency_map[sample] += sample.GetWeight();
      total_weight += sample.GetWeight();
    } else if (sample.GetSampleType() == SampleType::UPDATE) {
      // Update sample count
      sample_frequency_map[sample] += sample.GetWeight();
      total_weight += sample.GetWeight();
    } else {
      throw Exception("Unknown sample type : " +
                      std::to_string(static_cast<int>(sample.GetSampleType())));
    }
  }

  PL_ASSERT(total_weight > 0);

  // Normalize
  std::unordered_map<brain::Sample, double>::iterator sample_frequency_map_itr;

  for (sample_frequency_map_itr = sample_frequency_map.begin();
       sample_frequency_map_itr != sample_frequency_map.end();
       ++sample_frequency_map_itr) {
    // Normalize sample's utility
    sample_frequency_map_itr->second /= total_weight;
  }

  std::vector<sample_frequency_map_entry> sample_frequency_entry_list;

  for (auto sample_frequency_map_entry : sample_frequency_map) {
    auto entry = std::make_pair(sample_frequency_map_entry.first,
                                sample_frequency_map_entry.second);
    sample_frequency_entry_list.push_back(entry);
  }

  std::sort(sample_frequency_entry_list.begin(),
            sample_frequency_entry_list.end(),
            SampleFrequencyMapEntryComparator);

  // Print sample frequency map
  for (auto sample_frequency_map_entry : sample_frequency_map) {
    LOG_TRACE("Sample: %s Utility %.1lf",
              sample_frequency_map_entry.first.GetInfo().c_str(),
              sample_frequency_map_entry.second);
  }

  return sample_frequency_entry_list;
}

std::vector<std::vector<double>> GetSuggestedIndices(
    const std::vector<sample_frequency_map_entry>& list) {
  // Find frequent samples
  size_t frequency_rank_threshold = 10;

  // Print top-k frequent samples for table
  std::vector<std::vector<double>> suggested_indices;
  auto list_size = list.size();

  for (size_t entry_itr = 0;
       (entry_itr < frequency_rank_threshold) && (entry_itr < list_size);
       entry_itr++) {
    auto& entry = list[entry_itr];
    auto& sample = entry.first;
    LOG_TRACE("%s Utility : %.2lf", sample.GetInfo().c_str(), entry.second);
    suggested_indices.push_back(sample.GetColumnsAccessed());
  }

  return suggested_indices;
}

double GetCurrentIndexUtility(
    std::set<oid_t> suggested_index_set,
    const std::vector<sample_frequency_map_entry>& list) {
  double current_index_utility = 0;
  auto list_size = list.size();

  for (size_t entry_itr = 0; entry_itr < list_size; entry_itr++) {
    auto& entry = list[entry_itr];
    auto& sample = entry.first;
    auto& columns = sample.GetColumnsAccessed();

    std::set<oid_t> columns_set(columns.begin(), columns.end());

    if (columns_set == suggested_index_set) {
      LOG_TRACE("Sample~Index Match : %s ", sample.GetInfo().c_str());
      current_index_utility = entry.second;
      break;
    }
  }

  return current_index_utility;
}

void IndexTuner::DropIndexes(storage::DataTable* table) {
  oid_t index_count = table->GetIndexCount();

  // Go over indices
  oid_t index_itr;
  for (index_itr = 0; index_itr < index_count; index_itr++) {
    auto index = table->GetIndex(index_itr);
    if (index == nullptr) {
      continue;
    }

    // auto average_index_utility = index_metadata->GetUtility();
    auto index_oid = index->GetOid();

    // Check if index utility below threshold and drop if needed
    // if (average_index_utility < index_utility_threshold) {
    LOG_DEBUG("Dropping index : %s", index->GetMetadata()->GetInfo().c_str());

    table->DropIndexWithOid(index_oid);

    // Drop one index at a time
    return;

    // Update index count
    index_count = table->GetIndexCount();
    // }
  }
}

void IndexTuner::AddIndexes(
    storage::DataTable* table,
    const std::vector<std::vector<double>>& suggested_indices) {
  oid_t valid_index_count = table->GetValidIndexCount();
  size_t constructed_index_itr = 0;

  // Check if we have constructed too many indexess
  if (valid_index_count > index_count_threshold) {
    LOG_TRACE("Constructed too many indexes");
    return;
  }

  for (auto suggested_index : suggested_indices) {
    std::set<oid_t> suggested_index_set(suggested_index.begin(),
                                        suggested_index.end());

    if (suggested_index_set.empty()) continue;

    // Go over all indices
    bool suggested_index_found = false;
    oid_t index_count = table->GetIndexCount();
    oid_t index_itr;
    for (index_itr = 0; index_itr < index_count; index_itr++) {
      // Check attributes
      auto index_attrs = table->GetIndexAttrs(index_itr);
      if (index_attrs != suggested_index_set) {
        continue;
      }

      // Exact match
      suggested_index_found = true;
      break;
    }

    // Did we find suggested index ?
    if (visibility_mode_ == false && suggested_index_found == false) {
      LOG_TRACE("Did not find suggested index.");

      // Add adhoc index with given utility
      AddIndex(table, suggested_index_set);
      constructed_index_itr++;
    }
    // Found suggested index, enable it
    else if (suggested_index_found == true) {
      LOG_TRACE("Found suggested index.");

      // Make it visible if it already isn't
      auto index = table->GetIndex(index_itr);
      auto index_metadata = index->GetMetadata();

      auto index_is_visible = index_metadata->GetVisibility();
      if (index_is_visible == false) {
        LOG_INFO("Enabling index : %s", index_metadata->GetName().c_str());
        index_metadata->SetVisibility(true);
      }
    }
  }
}

void UpdateIndexUtility(storage::DataTable* table,
                        const std::vector<sample_frequency_map_entry>& list) {
  oid_t index_count = table->GetIndexCount();

  for (oid_t index_itr = 0; index_itr < index_count; index_itr++) {
    // Get index
    auto index = table->GetIndex(index_itr);
    if (index == nullptr) {
      continue;
    }

    auto index_metadata = index->GetMetadata();
    auto index_key_attrs = index_metadata->GetKeyAttrs();

    std::set<oid_t> index_set(index_key_attrs.begin(), index_key_attrs.end());

    // Get current index utility
    auto current_index_utility = GetCurrentIndexUtility(index_set, list);

    auto average_index_utility = index_metadata->GetUtility();

    LOG_TRACE("Average index utility %5.2lf", average_index_utility);
    LOG_TRACE("Current index utility %5.2lf", current_index_utility);

    // alpha (weight for old samples)
    double alpha = 0.2;

    // Update index utility
    auto updated_average_index_utility =
        alpha * current_index_utility + (1 - alpha) * average_index_utility;

    index_metadata->SetUtility(updated_average_index_utility);

    LOG_TRACE("Updated index utility %5.2lf :: %s",
              updated_average_index_utility, index_metadata->GetInfo().c_str());
  }
}

void PrintIndexInformation(storage::DataTable* table) {
  oid_t index_count = table->GetIndexCount();
  auto table_tilegroup_count = table->GetTileGroupCount();
  LOG_INFO("Index count : %u", table->GetValidIndexCount());

  for (oid_t index_itr = 0; index_itr < index_count; index_itr++) {
    // Get index
    auto index = table->GetIndex(index_itr);
    if (index == nullptr) {
      continue;
    }

    auto indexed_tile_group_offset = index->GetIndexedTileGroupOff();

    // Get percentage completion
    double fraction = 0.0;
    if (table_tilegroup_count != 0) {
      fraction =
          (double)indexed_tile_group_offset / (double)table_tilegroup_count;
      fraction *= 100;
    }

    LOG_DEBUG("%s %.1f%%", index->GetMetadata()->GetInfo().c_str(), fraction);
  }
}

void IndexTuner::Analyze(storage::DataTable* table) {
  // Process all samples in table
  auto samples = table->GetIndexSamples();

  // Check write ratio
  auto average_write_ratio = ComputeWorkloadWriteRatio(samples);

  // Determine frequent samples
  auto sample_frequency_entry_list = GetFrequentSamples(samples);

  // Compute suggested indices
  auto suggested_indices = GetSuggestedIndices(sample_frequency_entry_list);

  // Check index storage footprint
  auto valid_index_count = table->GetValidIndexCount();

  ////////////////////////////////////////////////
  // Drop indexes if
  // a) constructed too many indexes
  // b) write intensive workloads
  ////////////////////////////////////////////////

  auto index_overflow = (valid_index_count > index_count_threshold);
  auto write_intensive_workload = (average_write_ratio > write_ratio_threshold);

  // Skip drop table time
  if (visibility_mode_ == false) {
    if (index_overflow == true || write_intensive_workload == true) {
      DropIndexes(table);
    }
  }

  // Add indexes if needed
  AddIndexes(table, suggested_indices);

  // Update index utility
  if (visibility_mode_ == false) {
    UpdateIndexUtility(table, sample_frequency_entry_list);
  }

  // Display index information
  PrintIndexInformation(table);
}

void IndexTuner::IndexTuneHelper(storage::DataTable* table) {
  // Process all samples in table
  auto samples = table->GetIndexSamples();
  auto sample_count = samples.size();

  // Check if we have sufficient number of samples for build
  if (sample_count >= analyze_sample_count_threshold) {
    // Add required indices
    Analyze(table);

    // Clear samples
    table->ClearIndexSamples();
  }

  // Build desired indices
  BuildIndices(table);
}

oid_t IndexTuner::GetIndexCount() const {
  oid_t index_count = 0;

  // Go over all tables
  for (auto table : tables) {
    oid_t table_index_count = table->GetValidIndexCount();

    // Update index count
    index_count += table_index_count;
  }

  return index_count;
}

void IndexTuner::Tune() {
  LOG_TRACE("Begin tuning");

  Timer<std::milli> pause_timer;
  pause_timer.Start();

  // Continue till signal is not false
  while (index_tuning_stop == false) {
    // Go over one table at a time
    for (auto table : tables) {
      // Update indices periodically
      IndexTuneHelper(table);

      LOG_INFO("TUNER PAUSE [%dms]", duration_of_pause);
      std::this_thread::sleep_for(std::chrono::milliseconds(duration_of_pause));
    }

    pause_timer.Stop();
    auto duration = pause_timer.GetDuration();

    // Sleep a bit if needed
    if (duration > duration_between_pauses) {
      std::this_thread::sleep_for(std::chrono::milliseconds(duration_of_pause));
      pause_timer.Reset();
      pause_timer.Start();
    }
  }
}

void IndexTuner::Stop() {
  // Stop tuning
  index_tuning_stop = true;

  // Stop thread
  index_tuner_thread.join();

  LOG_INFO("Stopped index tuner");
}

void IndexTuner::AddTable(storage::DataTable* table) {
  {
    std::lock_guard<std::mutex> lock(index_tuner_mutex);
    tables.push_back(table);
  }
}

void IndexTuner::ClearTables() {
  {
    std::lock_guard<std::mutex> lock(index_tuner_mutex);
    tables.clear();
  }
}

void IndexTuner::BootstrapTPCC(const std::string& path) {
  // Enable visibility mode
  SetVisibilityMode();

  auto tables_samples = brain::BrainUtil::LoadSamplesFile(path);

  // Build sample map
  std::string database_name = "default_database";

  auto catalog = catalog::Catalog::GetInstance();

  // Go over set of tables, and add samples
  for (auto table_samples : tables_samples) {
    // Get table name
    auto table_name = table_samples.first;
    auto samples = table_samples.second;

    // Locate table in catalog
    auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    auto table = catalog->GetTableWithName(database_name, table_name, txn);
    txn_manager.CommitTransaction(txn);
    PL_ASSERT(table != nullptr);
    for (auto& sample : samples) {
      table->RecordIndexSample(sample);
    }
    LOG_INFO("Added table to index tuner : %s", table_name.c_str());

    // Attach table to tuner
    AddTable(table);
  }
}

// Load statistics for Index Tuner from a file
void LoadStatsFromFile(const std::string& path) {
  LOG_INFO("LoadStatsFromFile Invoked");

  // Get index tuner
  auto& index_tuner = brain::IndexTuner::GetInstance();

  // Set duration between pauses
  auto duration = 30000;  // in ms
  index_tuner.SetDurationOfPause(duration);

  // Bootstrap
  index_tuner.BootstrapTPCC(path);

  return;
}

}  // namespace brain
}  // namespace peloton
