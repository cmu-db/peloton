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

#include "common/NValue.hpp"

namespace voltdb {

/** implement the 2n/2n+1-argument DECODE function */
template<> inline NValue NValue::call<FUNC_DECODE>(const std::vector<NValue>& arguments) {
    int size = (int)arguments.size();
    assert(size>=3);
    int loopnum = ( size - 1 )/2;
    const NValue& baseval = arguments[0];
    for ( int i = 0; i < loopnum; i++ ) {
        const NValue& condval = arguments[2*i+1];
        if ( condval.compare(baseval) == VALUE_COMPARE_EQUAL ) {
            return arguments[2*i+2];
        }
    }
    const bool hasDefault = ( size % 2 == 0 );
    if ( hasDefault ) {
        NValue defaultResult = arguments[size-1];
        // See the comment above about the reason for un-inlining, here.
        if ( defaultResult.m_sourceInlined ) {
            defaultResult.allocateObjectFromInlinedValue();
        }
        return defaultResult;
    }
    return getNullValue();
}

}
