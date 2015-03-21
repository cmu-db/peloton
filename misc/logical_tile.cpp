//===--------------------------------------------------------------------===//
// Test Logical Tiles
//===--------------------------------------------------------------------===//

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cassert>
#include <iostream>
#include <chrono>
#include <numeric>

using namespace std;

#define TILE_WIDTH_1 4
#define TILE_WIDTH_2 8

#define TILE_LENGTH_1 128
#define TILE_LENGTH_2 256

#define JOIN_ATTR_1 0
#define JOIN_ATTR_2 0

#define THRESHOLD_1 48
#define THRESHOLD_2 64

struct tile_t {
  int rows;
  int cols;
  int result_rows;

  bool* bitmap;

  tile_t* source;
  tile_t* other;

  int** data;
};

tile_t *allocate_tile(int rows, int cols) {
  int row_itr, col_itr;
  tile_t *tile = new tile_t;

  tile->data = new int*[rows];
  for(row_itr = 0; row_itr < rows ; row_itr++)
    tile->data[row_itr] = new int[cols];

  tile->rows = rows;
  tile->cols = cols;

  return tile;
}

void init_tile(tile_t* tile) {
  int row_itr, col_itr;

  for(row_itr = 0; row_itr < tile->rows ; row_itr++) {
    for(col_itr = 0; col_itr < tile->cols ; col_itr++) {
      tile->data[row_itr][col_itr] = row_itr + col_itr ;
    }
  }

}

void print_tile(tile_t* tile, int verbose = 0) {
  int row_itr, col_itr;

  printf("---------------------\n");

  if(verbose) {
    for(row_itr = 0; row_itr < tile->rows ; row_itr++) {
      for(col_itr = 0; col_itr < tile->cols ; col_itr++) {
        printf("%d ", tile->data[row_itr][col_itr]);
      }
      printf("\n");
    }
  }

  printf("rows : %d \n", tile->rows);
  printf("cols : %d \n", tile->cols);

  printf("---------------------\n");
}

tile_t* physical_predicate(tile_t* source, int attr, int threshold) {
  assert(attr < source->cols);
  assert(threshold < source->rows);
  int row_itr, col_itr;

  tile_t *predicate_tile;
  predicate_tile = allocate_tile(threshold, source->rows);

  for(row_itr = 0; row_itr < source->rows ; row_itr++) {
    if(source->data[row_itr][attr] < threshold)
      memcpy(predicate_tile->data[row_itr], source->data[row_itr],  sizeof(int) * source->cols);
  }

  predicate_tile->rows = threshold;
  predicate_tile->cols = source->cols;

  return predicate_tile;
}

tile_t* logical_predicate(tile_t* source, int attr, int threshold) {
  assert(attr < source->cols);
  assert(threshold < source->rows);
  int row_itr, col_itr;

  tile_t *predicate_tile = new tile_t;
  predicate_tile->bitmap = new bool[source->rows](); // set to 0s

  for(row_itr = 0; row_itr < source->rows ; row_itr++) {
    if(source->data[row_itr][attr] < threshold)
      predicate_tile->bitmap[row_itr] = true;
  }

  predicate_tile->rows = threshold;
  predicate_tile->cols = source->cols;

  predicate_tile->source = source;

  return predicate_tile;
}

tile_t* physical_join(tile_t* source1, tile_t* source2, int attr1, int attr2) {
  assert(attr1 < source1->cols);
  assert(attr2 < source2->cols);

  int row_itr1, row_itr2;
  int join_itr = 0;

  tile_t *join_tile;
  join_tile = allocate_tile(source1->rows * source2->rows, source1->cols + source2->cols);

  printf("rows1 : %d \n", source1->rows);
  printf("rows2 : %d \n", source2->rows);

  for(row_itr1 = 0; row_itr1 < source1->rows  ; row_itr1++) {
    for(row_itr2 = 0; row_itr2 < source2->rows  ; row_itr2++) {

      if(source1->data[row_itr1][attr1] ==source2->data[row_itr2][attr2]) {
        memcpy(join_tile->data[join_itr], source1->data[row_itr1], sizeof(int) * source1->cols);
        memcpy(join_tile->data[join_itr] + source1->cols, source2->data[row_itr2], sizeof(int) * source2->cols);

        join_itr++;
      }

    }
  }

  join_tile->rows = source1->rows * source2->rows;
  join_tile->result_rows = join_itr - 1;
  join_tile->cols = source1->cols + source2->cols;

  return join_tile;
}

tile_t* logical_join(tile_t* source1, tile_t* source2, int attr1, int attr2) {
  assert(attr1 < source1->cols);
  assert(attr2 < source2->cols);

  int row_itr1, row_itr2;
  int join_itr = 0;

  tile_t *join_tile = new tile_t;
  join_tile->bitmap = new bool[source1->rows * source2->rows];

  for(row_itr1 = 0; row_itr1 < source1->rows  ; row_itr1++) {
    if(source1->bitmap[row_itr1] == true) {

      for(row_itr2 = 0; row_itr2 < source2->rows  ; row_itr2++) {
        if(source2->bitmap[row_itr2] == true) {

          if(source1->source->data[row_itr1][attr1] ==source2->source->data[row_itr2][attr2]) {
            join_tile->bitmap[row_itr1 * source2->rows + row_itr2] = true;
            join_itr++;
          }

        }
      } // INNER

    }
  } // OUTER

  join_tile->rows = source1->rows * source2->rows;
  join_tile->result_rows = join_itr - 1;
  join_tile->cols = source1->cols + source2->cols;

  join_tile->source = source1->source;
  join_tile->other = source2->source;

  printf("join itr :: %d \n", join_itr - 1);

  return join_tile;
}

void release_tile(tile_t* tile) {
  int row_itr, col_itr;

  for(row_itr = 0; row_itr < tile->rows ; row_itr++)
    delete[] tile->data[row_itr];

  delete[] tile->data;
  delete tile;
}

void release_logical_tile(tile_t* tile) {

  delete[] tile->bitmap;
  delete tile;
}

tile_t *materialize_join_tile(tile_t* tile) {

  int bitmap_itr;
  int join_itr = 0;

  for(bitmap_itr = 0; bitmap_itr < tile->source->rows * tile->other->rows ; bitmap_itr++) {
    if(tile->bitmap[bitmap_itr] == true)
      join_itr++;
  }

  tile_t *join_tile;
  join_tile = allocate_tile(tile->result_rows, tile->source->cols + tile->other->cols);

  int row_itr1, row_itr2;
  int join_cnt = join_itr - 1;
  join_itr = 0;

  for(bitmap_itr = 0; bitmap_itr < tile->source->rows * tile->other->rows ; bitmap_itr++) {

      if(tile->bitmap[bitmap_itr] == true) {

        //row_itr1 = bitmap_itr / tile->other->rows;
        //row_itr2 = bitmap_itr % tile->other->rows;
        //memcpy(join_tile->data[join_itr], tile->source->data[row_itr1], sizeof(int) * tile->source->cols);
        //memcpy(join_tile->data[join_itr] + tile->source->cols, tile->other->data[row_itr2], sizeof(int) * tile->other->cols);

        join_itr++;
      }
  }

  printf("join itr :: %d \n", join_itr - 1);


  join_tile->rows = join_itr - 1;
  join_tile->result_rows = join_itr - 1;
  join_tile->cols = tile->source->cols + tile->other->cols;

  return NULL;
  //return join_tile;
}

void do_physical_join() {
  tile_t *physical_tile_1;
  tile_t *physical_tile_2;
  int row_itr, col_itr;

  physical_tile_1 = allocate_tile(TILE_LENGTH_1, TILE_WIDTH_1);
  physical_tile_2 = allocate_tile(TILE_LENGTH_2, TILE_WIDTH_2);

  init_tile(physical_tile_1);
  init_tile(physical_tile_2);

  // PREDICATE

  tile_t *predicate_tile_1;
  tile_t *predicate_tile_2;

  predicate_tile_1 = physical_predicate(physical_tile_1, JOIN_ATTR_1, THRESHOLD_1);
  predicate_tile_2 = physical_predicate(physical_tile_2, JOIN_ATTR_2, THRESHOLD_2);

  // JOIN

  tile_t *join_tile;
  join_tile = physical_join(predicate_tile_1, predicate_tile_2, JOIN_ATTR_1, JOIN_ATTR_2);

  release_tile(physical_tile_1);
  release_tile(physical_tile_2);

  release_tile(predicate_tile_1);
  release_tile(predicate_tile_2);

  release_tile(join_tile);
}

void do_logical_join() {
  tile_t *physical_tile_1;
  tile_t *physical_tile_2;
  int row_itr, col_itr;

  physical_tile_1 = allocate_tile(TILE_LENGTH_1, TILE_WIDTH_1);
  physical_tile_2 = allocate_tile(TILE_LENGTH_2, TILE_WIDTH_2);

  init_tile(physical_tile_1);
  init_tile(physical_tile_2);

  // PREDICATE

  tile_t *predicate_tile_1;
  tile_t *predicate_tile_2;

  predicate_tile_1 = logical_predicate(physical_tile_1, JOIN_ATTR_1, THRESHOLD_1);
  predicate_tile_2 = logical_predicate(physical_tile_2, JOIN_ATTR_2, THRESHOLD_2);

  // JOIN

  tile_t *join_tile;
  join_tile = logical_join(predicate_tile_1, predicate_tile_2, JOIN_ATTR_1, JOIN_ATTR_2);

  /*
  tile_t *materialize_tile;
  materialize_tile = materialize_join_tile(join_tile);
  */

  release_tile(physical_tile_1);
  release_tile(physical_tile_2);

  release_logical_tile(predicate_tile_1);
  release_logical_tile(predicate_tile_2);

  release_logical_tile(join_tile);

  //release_tile(materialize_tile);
}

int main() {

  auto t1 =  std::chrono::high_resolution_clock::now();
  int itr;

  for(itr = 0 ; itr < 1000 ; itr++)
    do_logical_join();

  auto t2 =  std::chrono::high_resolution_clock::now();

  std::cout<<"Duration :: "<< (std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count())/itr <<" us"<< std::endl;

  return 0;
}
