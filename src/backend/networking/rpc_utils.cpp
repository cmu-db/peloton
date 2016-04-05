//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_utils.cpp
//
// Identification: /peloton/src/backend/networking/rpc_utils.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "rpc_utils.h"
#include "backend/common/cast.h"

namespace peloton {
namespace networking {

//===----------------------------------------------------------------------===//
//   Message Creation Functions
//===----------------------------------------------------------------------===//
void CreateTupleDesc(TupleDesc tuple_desc, TupleDescMsg& tuple_desc_msg) {
    // Convert the basic value type
    tuple_desc_msg.set_natts(tuple_desc->natts);
    tuple_desc_msg.set_tdhasoid(tuple_desc->tdhasoid);
    tuple_desc_msg.set_tdrefcount(tuple_desc->tdrefcount);
    tuple_desc_msg.set_tdtypeid(tuple_desc->tdtypeid);
    tuple_desc_msg.set_tdtypmod(tuple_desc->tdtypmod);

    // Convert attrs. It is a message type
    // Here attalign is char in postgres, so we convert it to int32
    tuple_desc_msg.mutable_attrs()->set_attalign(
            assert_range_cast_same<int32, int8>(tuple_desc->attrs->attalign));
    tuple_desc_msg.mutable_attrs()->set_attbyval(tuple_desc->attrs->attbyval);
    tuple_desc_msg.mutable_attrs()->set_attcacheoff(
            tuple_desc->attrs->attcacheoff);
    tuple_desc_msg.mutable_attrs()->set_attcollation(
            tuple_desc->attrs->attcollation);
    tuple_desc_msg.mutable_attrs()->set_atthasdef(tuple_desc->attrs->atthasdef);
    tuple_desc_msg.mutable_attrs()->set_attinhcount(
            tuple_desc->attrs->attinhcount);
    tuple_desc_msg.mutable_attrs()->set_attisdropped(
            tuple_desc->attrs->attislocal);
    tuple_desc_msg.mutable_attrs()->set_attislocal(
            tuple_desc->attrs->attislocal);
    // Here attlen is int16 in postgres, so we convert it to int32
    tuple_desc_msg.mutable_attrs()->set_attlen(
            assert_range_cast_same<int32, int16>(tuple_desc->attrs->attlen));
    // Postgres defines NAMEDATALEN 64
    tuple_desc_msg.mutable_attrs()->set_attname(tuple_desc->attrs->attname.data,
            NAMEDATALEN);

    // Convert TupleConstr. It is a message type
    // Note the threee char* () is ended with \0 in postgres, such as appendStringInfoChar.
    // So they are safe to be set in protobuf (as bytes) with on "size" parameter
    tuple_desc_msg.mutable_constr()->set_adbin(
            tuple_desc->constr->defval->adbin);
    // AttrNumber(adnum) is int16 in Postgres, so we convert it to int32
    tuple_desc_msg.mutable_constr()->set_adnum(
            assert_range_cast_same<int32, int16>(
                    tuple_desc->constr->defval->adnum));
    tuple_desc_msg.mutable_constr()->set_ccbin(
            tuple_desc->constr->check->ccbin);
    tuple_desc_msg.mutable_constr()->set_ccname(
            tuple_desc->constr->check->ccname);
    tuple_desc_msg.mutable_constr()->set_ccnoinherit(
            tuple_desc->constr->check->ccnoinherit);
    tuple_desc_msg.mutable_constr()->set_ccvalid(
            tuple_desc->constr->check->ccvalid);
    tuple_desc_msg.mutable_constr()->set_has_not_null(
            tuple_desc->constr->has_not_null);
    // num_check is uint16 in Postgres, so we convert it to uint32
    tuple_desc_msg.mutable_constr()->set_num_check(
            assert_range_cast_same<uint32, uint16>(
                    tuple_desc->constr->num_check));
    // num_defval is uint16 in Postgres, so we convert it to uint32
    tuple_desc_msg.mutable_constr()->set_num_defval(
            assert_range_cast_same<uint32, uint16>(
                    tuple_desc->constr->num_defval));
}

} // namespace message
} // namespace peloton
