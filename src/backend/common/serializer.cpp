//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// serializer.cpp
//
// Identification: src/backend/common/serializer.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/serializer.h"

namespace peloton {

void FallbackSerializeOutput::Expand(size_t minimum_desired) {
    /*
     * Leave some space for message headers and such, almost 50 megabytes
     */
    size_t maxAllocationSize = ((1024 * 1024 *50) - (1024 * 32));
    if (fallbackBuffer_ != NULL || minimum_desired > maxAllocationSize) {
        if (fallbackBuffer_ != NULL) {
            char *temp = fallbackBuffer_;
            fallbackBuffer_ = NULL;
            delete []temp;
        }
        throw Exception(
            "Output from SQL stmt overflowed output/network buffer of 50mb (-32k for message headers). "
            "Try a \"limit\" clause or a stronger predicate.");
    }
    fallbackBuffer_ = new char[maxAllocationSize];
    //::memcpy(fallbackBuffer_, Data(), position_);
    //SetPosition(position_);
    Initialize(fallbackBuffer_, maxAllocationSize);
    //ExecutorContext::getExecutorContext()->getTopend()->fallbackToEEAllocatedBuffer(fallbackBuffer_, maxAllocationSize);
}

template<Endianess E>
std::string SerializeInput<E>::fullBufferStringRep() {
    std::stringstream message(std::stringstream::in
                              | std::stringstream::out);

    message << "length: " << end_ - current_ << " Data: ";

    for (const char* i = current_; i != end_; i++) {
        const uint8_t value = static_cast<uint8_t>(*i);
        message << std::setw( 2 ) << std::setfill( '0' ) << std::hex << std::uppercase << (int)value;
        message << " ";
    }
    return message.str();
}

}  // End peloton namespace
