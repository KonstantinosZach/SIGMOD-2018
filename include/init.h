#pragma once

#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>

#include "operations.h"
#include "predicatesQueue.h"
#include "data_structures.h"
#include "filter_statistics.h"
#include "join_statistics.h"

#define FOLDER "../../workloads/small/"
// #define FOLDER "../../workloads/public/"

int init();

int read_workload(List relation_list);

void parse_predicates(PredicatesQueue pq, char* token);

void parse_relations_id(char* token, int* relation_num, int** array_value);

void parse_line(PredicatesQueue pq, List viewslist, int** array_value, int* relation_num, int** views, char* line);

IntermediateResultsFilter intermediateResultsFilter_create(int* array, int size, int cols, boolean create_stats);

void intermediateResultsFilter_delete(IntermediateResultsFilter intermediateResults, boolean delete_stats);

MappedRelation create_mappedRelation(uint64_t size, int fd);

void delete_MappedRelation(MappedRelation mappedRelation);

int get_col(MappedRelation map, int col, size_t* start, size_t* end);

void traverse_predicates(List list_mapping, List viewsList, int* array_value, int n_array, PredicatesQueue queue);

JoinIntermidiate joinIntermidiate_create(int cols, int rows, int number_of_relations, int** id_array);

void joinIntermidiate_delete(JoinIntermidiate joinIntermidiate);

JoinIntermidiateArray joinIntermidiateArray_create(int size_array, int size_pos);

void joinIntermidiateArray_delete(JoinIntermidiateArray joinArray);