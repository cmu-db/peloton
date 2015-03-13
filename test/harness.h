/*-------------------------------------------------------------------------
 *
 * multithreaded_tester.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/test/multithreaded_tester.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <vector>
#include <thread>
#include <functional>
#include <iostream>

#include "common/types.h"
#include "common/pretty_printer.h"

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Test Harness (common routines)
//===--------------------------------------------------------------------===//

#define MAX_THREADS 1024

// variadic function
template<typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args&&... args){

	std::vector<std::thread> thread_group;

	// Launch a group of threads
	for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
		thread_group.push_back(std::thread(args...));
	}

	//Join the threads with the main thread
	for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
		thread_group[thread_itr].join();
	}

}

uint64_t GetThreadId() {
	std::hash<std::thread::id> hash_fn;

	uint64_t id = hash_fn(std::this_thread::get_id());
	id = id % MAX_THREADS;

	return id;
}

std::atomic<txn_id_t> txn_id_counter(INVALID_TXN_ID);
std::atomic<cid_t> cid_counter(INVALID_CID);

txn_id_t GetTransactionId() {
	return ++txn_id_counter;
}

cid_t GetCommitId() {
	return ++cid_counter;
}

} // End test namespace
} // End nstore namespace



