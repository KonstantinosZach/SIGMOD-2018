#include "acutest.h"
#include "join.h"

void join_base_case(void){

    //sample case from exercise

    uint64_t** R = malloc(sizeof(uint64_t*)*4);
    for(int i=0; i<4; i++){
        R[i] = malloc(sizeof(uint64_t)*3);  
    }
    R[0][0] = 1; R[0][1] = 2; R[0][2] = 9;
    R[1][0] = 2; R[1][1] = 3; R[1][2] = 3;
    R[2][0] = 3; R[2][1] = 3; R[2][2] = 4;
    R[3][0] = 4; R[3][1] = 2; R[3][2] = 1;

    uint64_t** S = malloc(sizeof(uint64_t*)*3);
    for(int i=0; i<3; i++){
        S[i] = malloc(sizeof(uint64_t)*2);  
    }
    S[0][0] = 1; S[0][1] = 3;
    S[1][0] = 1; S[1][1] = 6; 
    S[2][0] = 3; S[2][1] = 2; 

    int r_col = 0;
    int s_col = 0;

    Join result = join_hash_function(R, 4, 3, r_col, S, 3, 2, s_col);

    TEST_CHECK(result->row_size == 3);
    TEST_CHECK(result->col_size == 5);
    TEST_CHECK(result->array[0][0] == 1);
    TEST_CHECK(result->array[0][1] == 2);
    TEST_CHECK(result->array[0][2] == 9);
    TEST_CHECK(result->array[0][3] == 1);
    TEST_CHECK(result->array[0][4] == 3);
    TEST_CHECK(result->array[1][0] == 1);
    TEST_CHECK(result->array[1][1] == 2);
    TEST_CHECK(result->array[1][2] == 9);
    TEST_CHECK(result->array[1][3] == 1);
    TEST_CHECK(result->array[1][4] == 6);
    TEST_CHECK(result->array[2][0] == 3);
    TEST_CHECK(result->array[2][1] == 3);
    TEST_CHECK(result->array[2][2] == 4);
    TEST_CHECK(result->array[2][3] == 3);
    TEST_CHECK(result->array[2][4] == 2);

    /*delete*/
    delete_join(result);
    for(int i=0; i<3; i++)
        free(S[i]);
    free(S);
    for(int i=0; i<4; i++)
        free(R[i]);
    free(R);
}

void join_extreme_case1(void){

    /* exteme case 1: same values in row*/
    uint64_t** R = malloc(sizeof(uint64_t*)*5);
    for(int i=0; i<5; i++){
        R[i] = malloc(sizeof(uint64_t)*1); 
    }
    R[0][0] = 1;
    R[1][0] = 1;
    R[2][0] = 1;
    R[3][0] = 1;
    R[4][0] = 1;

    uint64_t** S = malloc(sizeof(uint64_t*)*4);
    for(int i=0; i<4; i++){
        S[i] = malloc(sizeof(uint64_t)*1);  
    }

    S[0][0] = 1;
    S[1][0] = 1;
    S[2][0] = 1;
    S[3][0] = 1;

    int r_col = 0;
    int s_col = 0;

    Join result = join_hash_function(R, 5, 1, r_col, S, 4, 1, s_col);

    TEST_CHECK(result->row_size == 20);
    TEST_CHECK(result->col_size == 2);
    for(int i=0; i<result->row_size; i++){
        for(int j=0; j<result->col_size; j++){
            TEST_CHECK(result->array[i][j] == 1);
        }
    }

    /*delete*/
    for(int i=0; i<5; i++)
        free(R[i]);
    free(R);

    for(int i=0; i<4; i++)
        free(S[i]);
    free(S);

    delete_join(result);

}

void join_extreme_case2(void){

    /* exteme case 2: no match between the 2 matrices*/
    uint64_t** R = malloc(sizeof(uint64_t*)*5);
    for(int i=0; i<5; i++){
        R[i] = malloc(sizeof(uint64_t)*1); 
    }
    R[0][0] = 1;
    R[1][0] = 2;
    R[2][0] = 3;
    R[3][0] = 4;
    R[4][0] = 5;

    uint64_t** S = malloc(sizeof(uint64_t*)*4);
    for(int i=0; i<4; i++){
        S[i] = malloc(sizeof(uint64_t)*1);  
    }

    S[0][0] = 6;
    S[1][0] = 7;
    S[2][0] = 8;
    S[3][0] = 9;

    int r_col = 0;
    int s_col = 0;

    Join result = join_hash_function(R, 5, 1, r_col, S, 4, 1, s_col);

    TEST_CHECK(result->row_size == 0);
    TEST_CHECK(result->col_size == 0);
    
    /*delete*/
    for(int i=0; i<5; i++)
        free(R[i]);
    free(R);

    for(int i=0; i<4; i++)
        free(S[i]);
    free(S);

    delete_join(result);

}

// Λίστα με όλα τα tests προς εκτέλεση
TEST_LIST = {
    { "join_base_case", join_base_case },
    { "join_extreme_case1", join_extreme_case1 },
    { "join_extreme_case2", join_extreme_case2 },

	{ NULL, NULL } // τερματίζουμε τη λίστα με NULL
};