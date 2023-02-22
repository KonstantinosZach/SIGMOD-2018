#include "random_matrices.h"

int** read_random_array(char* filename, int* rowsize, int* colsize, int row_digits, int col_digits, int values_digits){
    FILE* fp = fopen(filename, "r");

    char* buffer = malloc(sizeof(char) * (row_digits + 2));
    *rowsize = atoi(fgets(buffer, (row_digits + 2), fp));
    free(buffer);

    buffer = malloc(sizeof(char) * (col_digits + 2));
    *colsize = atoi(fgets(buffer, (col_digits + 2), fp));

    //read new line
    fgets(buffer, 5, fp);
    free(buffer);

    int** array = malloc(sizeof(int*) * (*rowsize));
    for(int i=0; i<*rowsize; i++)
        array[i] = malloc(sizeof(int) * (*colsize));

    int r_counter = 0;
    int c_counter = 0;

    buffer = malloc(sizeof(char) * (values_digits + 2));
    while(fgets(buffer, (values_digits + 2), fp) != NULL){

        if(strcmp(buffer, "\n") == 0){
            r_counter++;
            c_counter = 0;
        }
        else{
            array[r_counter][c_counter] = (int)atof(buffer);
            c_counter++;
        }
    }

    // print array
    // for(int i=0; i<*rowsize; i++){
    //     for(int j=0; j<*colsize; j++){
    //         printf("%d ", array[i][j]);
    //     }
    //     printf("\n");
    // }
    // printf("\n");

    fclose(fp);
    free(buffer); 

    return array;
}

int** create_random_array(char* file_name, int* final_rowsize, int* final_colsize, int row_digits, int col_digits, int values_digits){
    srand(time(NULL));

    FILE* fp = fopen(file_name, "w+");
    
    int rowsize = 1 + rand()%((int)pow(10, row_digits) - 2);
    int colsize = 1 + rand()%((int)pow(10, col_digits) - 2);

    char row_write_size [4] = {'%', ((row_digits + 1) + '0'), 'd', '\0'};
    fprintf(fp, row_write_size, rowsize);

    char col_write_size [4] = {'%', ((col_digits + 1) + '0'), 'd', '\0'};
    fprintf(fp, col_write_size, colsize);

    fprintf(fp, "%c", '\n');

    char values_write_size [4] = {'%', ((values_digits + 1) + '0'), 'd', '\0'};
    for(int i=0; i<rowsize;i++){
       for(int j=0; j<colsize;j++){
           fprintf(fp, values_write_size, 1 + rand()%((int)pow(10, values_digits) - 2));
       }
       fprintf(fp, "%c", '\n');
    }

    fclose(fp);

    return read_random_array(file_name, final_rowsize, final_colsize, row_digits, col_digits, values_digits);
}

void delete_random_array(int** array, int rowsize){
    for(int i=0; i<rowsize; i++)
        free(array[i]);
    free(array);
}

void write_result_in_file(Join result, int max_value_digits){
    char value_write_size [4] = {'%', ((max_value_digits + 1) + '0'), 'd', '\0'};

    FILE* fp = fopen("output/result.txt", "w+");

    for(int i=0; i<result->row_size; i++){
        for(int j=0; j<result->col_size; j++){
            fprintf(fp, value_write_size, result->array[i][j]);
        }
        fprintf(fp, "%c", '\n');
    }
    fclose(fp);    
}
