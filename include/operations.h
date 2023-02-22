#pragma once

#include "data_structures.h"
#include "init.h"
#include "predicatesQueue.h"
#include "join.h"
#include "filter_statistics.h"

/*create id_array -> needs to be free'd later*/
void apply_filter(List list_array, int* array_value, IntermediateResultsFilter* intermediate_results, PredicateNode node);

void apply_join(List list_array, int* array_value, IntermediateResultsFilter* filters, JoinIntermidiateArray joinArray, PredicateNode node);

void apply_sum(IntermediateResultsFilter* filters, List list_array, int* array_value, JoinIntermidiateArray joinArray, Node node, uint64_t* sum);

MappedRelation get_map(List list_array, int* array_value, int number_of_array);

void split_part(char* part, int* number_of_array, int* number_of_column);