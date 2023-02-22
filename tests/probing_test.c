#include "acutest.h"
#include "probing.h"
#include "building.h"
#include "partition.h"

/* In case a values are not hashed so we expect first partition to be [1,2] and second [2,3] */
void probing_case_a_test(void){
    init_scheduler();
    uint64_t row1[] = {1,2};
    int row_size1 = 2;
    Partition partition1 = partitioning(row1, row_size1, true);
    
    uint64_t row2[] = {2,3};
    int row_size2 = 2;
    Partition partition2 = partitioning(row2, row_size2, true);
    
    /* 
        We build hash table with partition1[(0,1),(1,2)]
        1. Create hash table with neighborhood h=1 and size=partition->relation->nbits (which is N_LOWER_BOUND = 2)
        2. Insert Tuple(0,1) -> 1 hashes in 3 so we expect hash table to be hash_table[0, 0, 0, (0,1)]
        3. Insert Tuple(1,2) -> 2 hashes in 0 so hash_table is hash_table[(1,2), 0, 0, (0,1)] 
    */
    Build build = build_hash_table(partition1, partition2, true);
    
    /*
        Now we check every tuple from partition2[(0.2),(1,3)] and see if it is matched with any tuple in hash_table
        1. Check for Tuple(0,2) -> value 2 exists in hash_table so we have a match. Final_rows is increased (1)
        2. Check for Tuple(1,3) -> value 3 doesnt exist in hash_table.
        3. Final array shoyld have the row id's of the values that matched so it should be [1,0]
    */
    int final_rows = 0;
    List list = probing(build, &final_rows);

    TEST_CHECK(final_rows == 1);

    int** final = final_id_array(list, final_rows);

    TEST_CHECK(final[0][0] == 1);
    TEST_CHECK(final[0][1] == 0);

    delete_final_id_array(final, final_rows);
    list->destroy_value(list);
    delete_build(build);
    delete_partition(partition1);
    delete_partition(partition2);
    destroy_scheduler();
}

void probing_case_b_test(void){
    init_scheduler();
    uint64_t row1[] = {2,3,1,0,22,33,11,10,32,42};
    int row_size1 = 10;

    uint64_t row2[] = {2,3,1,0,22,33,11,10,32,42};
    int row_size2 = 10;

    Partition partition1 = partitioning(row1, row_size1, true);
    Partition partition2 = partitioning(row2, row_size2, true);
    /* 
        Partitions are the same [3 8 2 5 0 1 4 7 9 6] so we expect final_rows to be 10 and
                                [0 0 1 33 2 3 22 10 42 11]
        every row in final array to be filled with 2 cols of same value
    */
    Build build = build_hash_table(partition1, partition2, true);

    int final_rows = 0;
    List list = probing(build, &final_rows);
    
    TEST_CHECK(final_rows == 10);
    
    int** final = final_id_array(list, final_rows);
    
    for(int i=0; i<final_rows; i++)
        TEST_CHECK(final[i][0] == final[i][1]);

    delete_final_id_array(final, final_rows);   
    list->destroy_value(list);
    delete_build(build);
    delete_partition(partition1);
    delete_partition(partition2);
    destroy_scheduler();
}

void probing_case_c_test(void){
    init_scheduler();
    uint64_t row1[] = {2,3,1,0,22,33,11,10,0,42};
    int row_size1 = 10;

    uint64_t row2[] = {0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8};
    int row_size2 = 11;

    Partition partition1 = partitioning(row1, row_size1, true);   /* partition1[0 0 1 33 2 3 22 10 42 11] */
    Partition partition2 = partitioning(row2, row_size2, true);   /* partition2[0 0 0 1 2 3 4 5 6 7 8 ] */
    
    /* We create hash_table with partition2 (that is on case c) */
    Build build = build_hash_table(partition2, partition1, true);

    /* search from partition1 everything that matched
        1.3 -> 2.0
        1.3 -> 2.1
        1.3 -> 2.2
        1.8 -> 2.0
        1.8 -> 2.1
        1.8 -> 2.2
        1.2 -> 2.3
        1.0 -> 2.4
        1.1 -> 2.5
     */
    int final_rows = 0;
    List list = probing(build, &final_rows);

    int** final = final_id_array(list, final_rows);

    TEST_CHECK(final[0][0] == 0);TEST_CHECK(final[0][1] == 3);
    TEST_CHECK(final[1][0] == 1);TEST_CHECK(final[1][1] == 3);
    TEST_CHECK(final[2][0] == 2);TEST_CHECK(final[2][1] == 3);
    TEST_CHECK(final[3][0] == 0);TEST_CHECK(final[3][1] == 8);
    TEST_CHECK(final[4][0] == 1);TEST_CHECK(final[4][1] == 8);
    TEST_CHECK(final[5][0] == 2);TEST_CHECK(final[5][1] == 8);
    TEST_CHECK(final[6][0] == 3);TEST_CHECK(final[6][1] == 2);
    TEST_CHECK(final[7][0] == 4);TEST_CHECK(final[7][1] == 0);
    TEST_CHECK(final[8][0] == 5);TEST_CHECK(final[8][1] == 1);

    delete_final_id_array(final, final_rows);
    list->destroy_value(list);
    delete_build(build);
    delete_partition(partition1);
    delete_partition(partition2);
    destroy_scheduler();
}

// Λίστα με όλα τα tests προς εκτέλεση
TEST_LIST = {
    { "probing_case_a_test", probing_case_a_test },
    { "probing_case_b_test", probing_case_b_test },
    { "probing_case_c_test", probing_case_c_test },

	{ NULL, NULL } // τερματίζουμε τη λίστα με NULL
};