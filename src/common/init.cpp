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
#include "index/index_key.h"

#include "libcds/cds/init.h"

#include <google/protobuf/stubs/common.h>

namespace peloton {

void PelotonInit::Initialize() {

  // Initialize CDS library
  cds::Initialize();


  // Create URCU general_buffered singleton

  SkipListMap<index::GenericKey<4>, ItemPointer *, index::GenericComparator<4>>::RCUImpl::Construct();
  SkipListMap<index::GenericKey<8>, ItemPointer *, index::GenericComparator<8>>::RCUImpl::Construct();
  SkipListMap<index::GenericKey<16>, ItemPointer *, index::GenericComparator<16>>::RCUImpl::Construct();
  SkipListMap<index::GenericKey<64>, ItemPointer *, index::GenericComparator<64>>::RCUImpl::Construct();
  SkipListMap<index::GenericKey<256>, ItemPointer *, index::GenericComparator<256>>::RCUImpl::Construct();

  SkipListMap<index::TupleKey, ItemPointer *, index::TupleKeyComparator>::RCUImpl::Construct();

}

void PelotonInit::Shutdown() {

  // Terminate CDS library
  cds::Terminate();

  // shutdown protocol buf library
  google::protobuf::ShutdownProtobufLibrary();

  // Destroy URCU general_buffered singleton

  SkipListMap<index::GenericKey<4>, ItemPointer *, index::GenericComparator<4>>::RCUImpl::Destruct();
  SkipListMap<index::GenericKey<8>, ItemPointer *, index::GenericComparator<8>>::RCUImpl::Destruct();
  SkipListMap<index::GenericKey<16>, ItemPointer *, index::GenericComparator<16>>::RCUImpl::Destruct();
  SkipListMap<index::GenericKey<64>, ItemPointer *, index::GenericComparator<64>>::RCUImpl::Destruct();
  SkipListMap<index::GenericKey<256>, ItemPointer *, index::GenericComparator<256>>::RCUImpl::Destruct();

  SkipListMap<index::TupleKey, ItemPointer *, index::TupleKeyComparator>::RCUImpl::Destruct();

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
