//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// logic_functions.h
//
// Identification: src/backend/expression/logic_functions.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/value.h"

namespace peloton {

/** implement the 2n/2n+1-argument DECODE function */
template<> inline Value Value::Call<FUNC_DECODE>(const std::vector<Value>& arguments) {
    int size = (int)arguments.size();
    assert(size>=3);
    int loopnum = ( size - 1 )/2;
    const Value& basEvaluate = arguments[0];
    for ( int i = 0; i < loopnum; i++ ) {
        const Value& condval = arguments[2*i+1];
        if ( condval.Compare(basEvaluate) == VALUE_COMPARE_EQUAL ) {
            return arguments[2*i+2];
        }
    }
    const bool hasDefault = ( size % 2 == 0 );
    if ( hasDefault ) {
        Value defaultResult = arguments[size-1];
        // See the comment above about the reason for un-inlining, here.
        if ( defaultResult.m_sourceInlined ) {
            defaultResult.AllocateObjectFromInlinedValue();
        }
        return defaultResult;
    }
    return GetNullValue();
}

}  // End peloton namespace

