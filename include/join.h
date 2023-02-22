#pragma once

#include "probing.h"
#include "init.h"
#include "scheduler.h"

Join join_hash_function(uint64_t** R, int r_rowsize, int r_colsize, int r_col, uint64_t** S, int s_rowsize, int s_colsize, int s_col);

void join_hash_different_relation(MappedRelation mapR, int r_col_num, uint64_t* R_values,int Rfilter_size, MappedRelation mapS, int s_col_num, uint64_t* S_values, int Sfilter_size, int* final_rows, int*** id_array);

Join create_join(int** final, uint64_t** R, int r_colsize, uint64_t** S,  int s_colsize, int final_rows);

void delete_join(Join joined);