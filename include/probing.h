#pragma once

#include "building.h"
#include "partition.h"

Match create_match();

void delete_match(Match match);

List probing(Build build, int* final_rows);

int** final_id_array(List list, int size);

void delete_final_id_array(int** id_array, int size);
