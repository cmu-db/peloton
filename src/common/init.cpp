//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// init.h
//
// Identification: src/include/common/init.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/init.h"
#include "container/skip_list_map.h"

#include "libcds/cds/init.h"

#include <google/protobuf/stubs/common.h>

namespace peloton {

void PelotonInit::Initialize() {

  // Initialize CDS library
  cds::Initialize();

  // RCU

  // Create URCU general_buffered singleton
  SkipListMap<uint32_t, uint32_t>::RCUImpl::Construct();

}

void PelotonInit::Shutdown() {

  // Terminate CDS library
  cds::Terminate();

  // shutdown protocol buf library
  google::protobuf::ShutdownProtobufLibrary();

  // RCU

  // Destroy URCU general_buffered singleton
  SkipListMap<uint32_t, uint32_t>::RCUImpl::Destruct();

}

void PelotonInit::SetUpThread() {

  // Attach thread to cds
  cds::threading::Manager::attachThread();

}

void PelotonInit::TearDownThread() {

  // Detach thread from cds
  cds::threading::Manager::detachThread();

}

}  // End peloton namespace
