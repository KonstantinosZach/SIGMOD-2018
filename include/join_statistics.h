#pragma once
#include "data_structures.h"
#include "operations.h"

void find_join_order(int* array_value, int n_array, PredicatesQueue queue, int num_joins, PredicatesQueue ordered_queue, List list_array, IntermediateResultsFilter* filters);


JoinStats* create_cost_array(int n_array);

void delete_cost_array(JoinStats* cost, int n_array);

JoinStats calclulate_join_stats(JoinStats* cost, int s, int r, JoinStats current, int i, List list_array, int* array_value, int scol, int rcol);

Connected** create_connected_relations(int n_array);

void delete_connected_relations(Connected** connected_relations, int n_array);

Connected** init_connected_relations(PredicatesQueue queue, int num_joins, Connected** connected_relations);

void copy_joinStats(JoinStats target, JoinStats source);

void delete_joinStats(JoinStats target);

int* resize_R(int i, int* R, int* r_size, int n_array, JoinStats* cost, int in_list);

JoinStats* init_cost(JoinStats* cost, int n_array, List list_array, int* array_value, IntermediateResultsFilter* filters);

void delete_stats_array(Stats* stats, int cols);