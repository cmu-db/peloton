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

/*
 * @brief Create TupleDesc Message from Postgres's TupleDesc
 * @param TupleDesc and TupleDescMsg&
 * @return TupleDescMsg is set on
 */
void CreateTupleDescMsg(TupleDesc tuple_desc, TupleDescMsg& tuple_desc_msg) {
    // Convert the basic value type
    tuple_desc_msg.set_natts(tuple_desc->natts);
    tuple_desc_msg.set_tdhasoid(tuple_desc->tdhasoid);
    tuple_desc_msg.set_tdrefcount(tuple_desc->tdrefcount);
    tuple_desc_msg.set_tdtypeid(tuple_desc->tdtypeid);
    tuple_desc_msg.set_tdtypmod(tuple_desc->tdtypmod);

    //===----------------------------------------------------------------------===//
    //    Convert attrs. It is message type and there might be multiple
    //    attrs according to tuple_desc->natts
    //===----------------------------------------------------------------------===//
    for (int it = 0; it < tuple_desc->natts; it++) {
        // Prepare adding a attrs
        FormAttributeMsg* attrs = tuple_desc_msg.add_attrs();
        // Here attalign is char in postgres, so we convert it to int32
        attrs->set_attalign(
                assert_range_cast_same<int32, int8>(
                        (*tuple_desc->attrs)->attalign));
        attrs->set_attbyval(
                (*tuple_desc->attrs)->attbyval);
        attrs->set_attcacheoff(
                (*tuple_desc->attrs)->attcacheoff);
        attrs->set_attcollation(
                (*tuple_desc->attrs)->attcollation);
        attrs->set_atthasdef(
                (*tuple_desc->attrs)->atthasdef);
        attrs->set_attinhcount(
                (*tuple_desc->attrs)->attinhcount);
        attrs->set_attisdropped(
                (*tuple_desc->attrs)->attislocal);
        attrs->set_attislocal(
                (*tuple_desc->attrs)->attislocal);
        // Here attlen is int16 in postgres, so we convert it to int32
        attrs->set_attlen(
                assert_range_cast_same<int32, int16>(
                        (*tuple_desc->attrs)->attlen));
        // Postgres defines NAMEDATALEN 64
        attrs->set_attname(
                (*tuple_desc->attrs)->attname.data, NAMEDATALEN);
        attrs->set_attndims(
                (*tuple_desc->attrs)->attndims);
        attrs->set_attnotnull(
                (*tuple_desc->attrs)->attnotnull);
        // It is int16 in postgres, so we convert it to int32
        attrs->set_attnum(
                assert_range_cast_same<int32, int16>(
                        (*tuple_desc->attrs)->attnum));
        attrs->set_attrelid(
                (*tuple_desc->attrs)->attrelid);
        attrs->set_attstattarget(
                (*tuple_desc->attrs)->attstattarget);
        // It is char in postgres, so we convert it to int32
        attrs->set_attstorage(
                assert_range_cast_same<int32, int8>(
                        (*tuple_desc->attrs)->attstorage));
        attrs->set_atttypid(
                (*tuple_desc->attrs)->atttypid);
        attrs->set_atttypmod(
                (*tuple_desc->attrs)->atttypmod);
    }

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

/*
 * @brief Parse TupleDesc Message and create Postgres's TupleDesc
 * @param  TupleDescMsg&
 * @return TupleDesc
 */
TupleDesc ParseTupleDescMsg(const TupleDescMsg& tuple_desc_msg) {

    // TODO: Using Postgres func to create, but when should we FreeTupleDesc?
    TupleDesc tuple_desc;

    //===----------------------------------------------------------------------===//
    //   Parse and create attrs structure
    //===----------------------------------------------------------------------===//
    // Get the number of attrs
    int attrs_count = tuple_desc_msg.natts();

    if (attrs_count > 0) {
        Form_pg_attribute attrs[attrs_count];
        Form_pg_attribute* ppattrs = attrs;

        assert(tuple_desc_msg.attrs_size() == attrs_count);

        for (int it = 0; it < attrs_count; it++) {
            // Here attalign is char in postgres, so we convert it
            attrs[it]->attalign = assert_range_cast_same<int8, int32>(
                    tuple_desc_msg.attrs(it).attalign());
            attrs[it]->attbyval = tuple_desc_msg.attrs(it).attbyval();
            attrs[it]->attcacheoff = tuple_desc_msg.attrs(it).attcacheoff();
            attrs[it]->attcollation = tuple_desc_msg.attrs(it).attcollation();
            attrs[it]->atthasdef = tuple_desc_msg.attrs(it).atthasdef();
            attrs[it]->attinhcount = tuple_desc_msg.attrs(it).attinhcount();
            attrs[it]->attisdropped = tuple_desc_msg.attrs(it).attisdropped();
            attrs[it]->attislocal = tuple_desc_msg.attrs(it).attislocal();
            // It is int16 in postgres, so we convert it
            attrs[it]->attlen = assert_range_cast_same<int16, int32>(
                    tuple_desc_msg.attrs(it).attlen());
            std::string attname = tuple_desc_msg.attrs(it).attname();
            attname.copy(attrs[it]->attname.data,NAMEDATALEN);
            attrs[it]->attndims = tuple_desc_msg.attrs(it).attndims();
            attrs[it]->attnotnull = tuple_desc_msg.attrs(it).attnotnull();
            // It is int16 in postgres, so we convert it
            attrs[it]->attnum = assert_range_cast_same<int16, int32>(
                    tuple_desc_msg.attrs(it).attnum());
            attrs[it]->attrelid = tuple_desc_msg.attrs(it).attrelid();
            attrs[it]->attstattarget = tuple_desc_msg.attrs(it).attstattarget();
            // It is char in postgres, so we convert it
            attrs[it]->attstorage = assert_range_cast_same<int8, int32>(
                    tuple_desc_msg.attrs(it).attstorage());
            attrs[it]->atttypid = tuple_desc_msg.attrs(it).atttypid();
            attrs[it]->atttypmod = tuple_desc_msg.attrs(it).atttypmod();

        }

        tuple_desc = CreateTupleDesc(attrs_count, false, ppattrs);
    } else {
        tuple_desc = CreateTemplateTupleDesc(attrs_count, false);
    }

    //===----------------------------------------------------------------------===//
    //    Set the basic value type
    //===----------------------------------------------------------------------===//
    tuple_desc->tdhasoid = tuple_desc_msg.tdhasoid();
    tuple_desc->tdrefcount = tuple_desc_msg.tdrefcount();
    tuple_desc->tdtypeid = tuple_desc_msg.tdtypeid();
    tuple_desc->tdtypmod = tuple_desc_msg.tdtypmod();

    //===----------------------------------------------------------------------===//
    //   Parse and create TupeDescCons structure
    //===----------------------------------------------------------------------===//
    // Note the three char* () is ended with \0 in postgres, such as appendStringInfoChar.
    // So they are safe to be set in protobuf (as bytes) with on "size" parameter
    // But when convert them back to char* we should set \0 at the end of char*
    AttrDefault* attrdef = (AttrDefault*)malloc(sizeof(AttrDefault));
    ConstrCheck* constrch = (ConstrCheck*)malloc(sizeof(ConstrCheck));
    TupleConstr* tuple_constr = (TupleConstr*)malloc(sizeof(TupleConstr));

    std::string adbinss = tuple_desc_msg.constr().adbin();
    std::string ccbinss = tuple_desc_msg.constr().ccbin();
    std::string ccnamess = tuple_desc_msg.constr().ccname();

    char* adbin = (char*)malloc( adbinss.size() + 1 );
    char* ccbin = (char*)malloc( ccbinss.size() + 1 );
    char* ccname = (char*)malloc( ccnamess.size() + 1 );

    adbinss.copy(adbin, adbinss.size());
    adbin[adbinss.size()] = '\0';

    ccbinss.copy(adbin, ccbinss.size());
    adbin[ccbinss.size()] = '\0';

    ccnamess.copy(adbin, ccnamess.size());
    adbin[ccnamess.size()] = '\0';

    // Set AttrDefault
    attrdef->adbin = adbin;
    // int16
    attrdef->adnum = tuple_desc_msg.constr().adnum();

    // Set ConstrCheck
    constrch->ccbin = ccbin;
    constrch->ccname = ccname;
    constrch->ccnoinherit = tuple_desc_msg.constr().ccnoinherit();
    constrch->ccvalid = tuple_desc_msg.constr().ccvalid();

    // Set TupleConstr
    tuple_constr->check = constrch;
    tuple_constr->defval = attrdef;
    tuple_constr->has_not_null = tuple_desc_msg.constr().has_not_null();
    // uint16
    tuple_constr->num_check = assert_range_cast_same<uint16, uint32>(
            tuple_desc_msg.constr().num_check());
    // uint16
    tuple_constr->num_defval = assert_range_cast_same<uint16, uint32>(
            tuple_desc_msg.constr().num_defval());

    // Set Constr
    tuple_desc->constr = tuple_constr;

    return tuple_desc;
}

} // namespace message
} // namespace peloton
