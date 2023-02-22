#pragma once
#include "init.h"

void update_filter_statistics(List list_mapping, int* array_value, IntermediateResultsFilter* filters, PredicateNode node);

void calculate_statistics(MappedRelation mappedRelation);

void initialize_statistics(IntermediateResultsFilter filter, MappedRelation map);
