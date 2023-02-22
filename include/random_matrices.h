#pragma once

#include<unistd.h>
#include <time.h>

#include "join.h"

int** read_random_array(char* filename, int* rowsize, int* colsize, int row_digits, int col_digits, int values_digits);

int** create_random_array(char* file_name, int* final_rowsize, int* final_colsize, int row_digits, int col_digits, int values_digits);

void delete_random_array(int** array, int rowsize);

void write_result_in_file(Join result, int max_value_digits);
