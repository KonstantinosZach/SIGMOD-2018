#pragma once

#include "data_structures.h"
#include "scheduler.h"
#include "building.h"

Histogram create_psum(Histogram);

void delete_psum(Histogram);

Histogram create_hist(int n);

void update_hist(Histogram hist, int hash_value);

void delete_hist(Histogram hist);

int hash(int n, uint64_t value);

Relation re_ordered(Tuple* row, int row_size, Histogram psum, int n);

Relation create_relation(int size);

void delete_relation(Relation relation);

Pass split(Tuple* row, int row_size, int n_start, int n_end, boolean* case_c);

Pass create_pass(Relation relation, Histogram psum);

void delete_pass(Pass pass);

Partition create_partition(Pass pass1, Pass pass2);

void delete_partition(Partition partition);

Partition partitioning(uint64_t* row, int row_size, boolean DEBUG);

Tuple* create_id_array(uint64_t* row, int row_size);

void delete_id_array(Tuple* array, int row_size);