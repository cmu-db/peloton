#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <ctime>

#include "tbb/tbb.h"
#include "tbb/flow_graph.h"

using namespace tbb::flow::interface6;

int size = 10000000;
int chunk_size = 100000;
int *data = new int[size];

class table_iterator_task {
 public:

  table_iterator_task(int l) :
    num_tilegroups(l),
    next_tilegroup(0) {
  }

  bool operator()( int &v ) {
    if ( next_tilegroup < num_tilegroups ) {
      v = next_tilegroup++;
      return true;
    }
    else {
      return false;
    }
  }

 private:
  const int num_tilegroups;
  int next_tilegroup;
};

int predicate() {
  int sum = 0;
  for(auto ii = 0 ; ii < 1000 ; ii++)
    sum += ii;
  return sum;
}

class seq_scanner_task {
 public:
  std::vector<int> operator()(const int &v) const {
    std::vector<int> matching;

    int offset = v * chunk_size;
    int end = offset + chunk_size;

    for(auto ii = offset; ii < end ; ii++)
      if(data[ii] % 5 == 0 && predicate())
        matching.push_back(ii);

    return matching;
  }
};

class summer_task {
 public:
  int operator()(const std::vector<int>& matching) const {

    long local_sum = 0;
    for(auto ii : matching)
      local_sum += data[ii];

    return local_sum;
  }
};

long long sum = 0;

class aggregator_task {
 public:
  int operator()(const int& local_sum) const {
    sum += local_sum;
    return sum;
  }
};

void query(int n) {

  std::cout << "Query :: " << n << "\n";

  std::chrono::time_point<std::chrono::system_clock> start, end;
  std::chrono::duration<double> elapsed_seconds;

  int num_chunks = size/chunk_size;

  for(auto ii = 0; ii < size; ii++)
    data[ii] = rand()%10;

  // PARALLEL
  start = std::chrono::system_clock::now();

  graph g;

  function_node< int, int > aggregator( g, 1, aggregator_task() );
  function_node< std::vector<int>, int > summer( g, tbb::flow::unlimited, summer_task());
  function_node< int, std::vector<int> > seq_scanner( g, tbb::flow::unlimited, seq_scanner_task());

  source_node< int > table_iterator( g, table_iterator_task(num_chunks), false );

  make_edge(table_iterator, seq_scanner);
  make_edge(seq_scanner, summer);
  make_edge(summer, aggregator);

  table_iterator.activate();

  g.wait_for_all();

  std::cout << "Parallel Sum is    : " << sum << "\n";

  end = std::chrono::system_clock::now();
  elapsed_seconds = end-start;
  std::cout << "elapsed time: " << elapsed_seconds.count() << "s\n";

  // SEQUENTIAL
  start = std::chrono::system_clock::now();

  sum = 0;

  for(auto ii = 0; ii < size; ii++)
    if(data[ii] % 5 == 0 && predicate())
      sum *= data[ii];

  std::cout << "Sequential Sum is  : " << sum << "\n";

  end = std::chrono::system_clock::now();
  elapsed_seconds = end-start;
  std::cout << "elapsed time: " << elapsed_seconds.count() << "s\n";

}

int main() {

  // Run queries in parallel
  tbb::parallel_for<int>( 1, 3, 1, query);

  delete[] data;
}
