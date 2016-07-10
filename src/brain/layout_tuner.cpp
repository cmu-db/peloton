//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// layout_tuner.cpp
//
// Identification: src/brain/layout_tuner.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/layout_tuner.h"
#include "brain/clusterer.h"

#include "catalog/schema.h"
#include "common/logger.h"
#include "storage/data_table.h"

namespace peloton {
namespace brain {

LayoutTuner &LayoutTuner::GetInstance() {
  static LayoutTuner layout_tuner;
  return layout_tuner;
}

LayoutTuner::LayoutTuner(){
  // Nothing to do here !
}

LayoutTuner::~LayoutTuner(){
  // Nothing to do here !
}

void LayoutTuner::Start(){

  // Set signal
  layout_tuning_stop = false;

  // Launch thread
  layout_tuner_thread = std::thread(&brain::LayoutTuner::Tune, this);

}

void LayoutTuner::Tune(){


  // Continue till signal is not false
  while(layout_tuning_stop == false) {

    // Go over all tables
    for(auto table : tables) {

      // Transform
      auto tile_group_count = table->GetTileGroupCount();
      auto tile_group_offset = rand() % tile_group_count;

      table->TransformTileGroup(tile_group_offset, theta);

      // Update partitioning periodically
      UpdateDefaultPartition(table);

      // Sleep a bit
      std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
    }

  }

}

void LayoutTuner::UpdateDefaultPartition(storage::DataTable* table) {
  oid_t column_count = table->GetSchema()->GetColumnCount();

  // Set up clusterer
  brain::Clusterer clusterer(cluster_count,
                             column_count,
                             new_sample_weight);

  // Process all samples in table
  auto& samples = table->GetSamples();

  // Check if we have any samples
  if (samples.empty()) {
    return;
  }

  for (auto sample : samples) {
    clusterer.ProcessSample(sample);
  }

  // Clear all samples in table
  table->ClearSamples();

  // Desired number of tiles
  auto layout = clusterer.GetPartitioning(tile_count);

  // Update table layout
  table->SetDefaultLayout(layout);

}

void LayoutTuner::Stop(){

  // Stop tuning
  layout_tuning_stop = true;

  // Stop thread
  layout_tuner_thread.join();

}

void LayoutTuner::AddTable(storage::DataTable* table){
  {
    std::lock_guard<std::mutex> lock(layout_tuner_mutex);
    LOG_INFO("table : %p", table);

    tables.push_back(table);
  }
}

void LayoutTuner::ClearTables() {
  {
    std::lock_guard<std::mutex> lock(layout_tuner_mutex);
    tables.clear();
  }
}


}  // End brain namespace
}  // End peloton namespace
