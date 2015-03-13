/*-------------------------------------------------------------------------
 *
 * synch.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/synch.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

//===--------------------------------------------------------------------===//
// Synchronization utilities
//===--------------------------------------------------------------------===//

namespace nstore {

template <typename T>
inline bool atomic_cas(T* object, T old_value, T new_value) {
	return __sync_bool_compare_and_swap(object, old_value, new_value);
}

} // End nstore namespace
