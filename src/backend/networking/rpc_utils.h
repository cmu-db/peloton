//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_utils.h
//
// Identification: /peloton/src/backend/networking/rpc_utils.h
//
// Copyright (c) 2016, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_service.pb.h"

#include "postgres/include/c.h"
#include "postgres/include/access/transam.h"
#include "postgres/include/access/tupdesc.h"

namespace peloton {
namespace networking {

//===----------------------------------------------------------------------===//
//   Message Creation Functions
//===----------------------------------------------------------------------===//

/*
 * CreateTupleDescMsg is used when a node sends query plan
 */
void CreateTupleDescMsg(TupleDesc tuple_desc, TupleDescMsg& tuple_desc_msg);

/*
 * When executing the query plan, a node must parse the received msg and convert it
 * to Postgres's TupleDesc
 */
TupleDesc ParseTupleDescMsg(const TupleDescMsg& tuple_desc_msg);

} // namespace message
} // namespace peloton


