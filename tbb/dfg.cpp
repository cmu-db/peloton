#include <iostream>
#include <cstdio>

#include "tbb/flow_graph.h"

using namespace tbb::flow::interface6;

class src_body {
 public:
  src_body(int l) :
    my_limit(l),
    my_next_value(1) {
  }

  bool operator()( int &v ) {
    if ( my_next_value <= my_limit ) {
      v = my_next_value++;
      return true;
    }
    else {
      return false;
    }
  }

 private:
  const int my_limit;
  int my_next_value;
};

class squarer_task {
 public:
  int operator()(const int &v) const {
    return v * v;
  }
};

class cuber_task {
 public:
  int operator()(const int &v) const {
    return v * v * v;
  }
};

double sum = 0;

class summer_task {
 public:
  int operator()(const int &v) const {
    return sum += v * 2.0;
  }
};

int main() {

  graph g;

  function_node< int, int > squarer( g, tbb::flow::unlimited, squarer_task());
  function_node< int, int > cuber( g, tbb::flow::unlimited, cuber_task() );
  function_node< int, double > summer( g, 1, summer_task());

  make_edge( squarer, summer );
  make_edge( cuber, summer );

  source_node< int > src( g, src_body(10), false );

  make_edge( src, squarer );
  make_edge( src, cuber );

  src.activate();

  g.wait_for_all();

  std::cout << "Sum is " << sum << "\n";
}
