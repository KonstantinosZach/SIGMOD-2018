#include "acutest.h"
#include "partition.h"

void test_create_hist(void){
    Histogram hist = create_hist(2);
    
    /* check that histogram size is 2^n */
    TEST_CHECK(hist->size == 4);
    
    /* check for pseudovalues */
    for(int i=0; i<hist->size; i++){
        TEST_CHECK(hist->array[i]->id == -1);
    }

    /* delete hist */
    delete_hist(hist);
}

void test_update_hist(void){
    Histogram hist = create_hist(2);
    
    update_hist(hist, 0);
    TEST_CHECK(hist->array[0]->id == 0);
    TEST_CHECK(hist->array[0]->value == 1);

    update_hist(hist, 1);
    TEST_CHECK(hist->array[1]->id == 1);
    TEST_CHECK(hist->array[1]->value == 1);

    update_hist(hist, 2);
    TEST_CHECK(hist->array[2]->id == 2);
    TEST_CHECK(hist->array[2]->value == 1);

    update_hist(hist, 3);
    TEST_CHECK(hist->array[3]->id == 3);
    TEST_CHECK(hist->array[3]->value == 1);

    update_hist(hist, 3);
    TEST_CHECK(hist->array[3]->id == 3);
    TEST_CHECK(hist->array[3]->value == 2);

    update_hist(hist, 2);
    TEST_CHECK(hist->array[2]->id == 2);
    TEST_CHECK(hist->array[2]->value == 2);

    /* delete hist */
    delete_hist(hist);
}

void psum_test(void){

    /* create a histogram */
    Histogram hist = malloc(sizeof(struct histogram));
    hist->size = 4;
    hist->array = malloc(sizeof(Tuple*) * 4);
    for(int i = 0; i<4; i++){
        *((hist->array)+i) = malloc(sizeof(struct tuple));
        hist->array[i]->id = i;
    }
    hist->array[0]->value = 3;
    hist->array[1]->value = 2;
    hist->array[2]->value = 2;
    hist->array[3]->value = 0;

    Histogram psum = create_psum(hist);
    TEST_CHECK(psum->size == 4);
    TEST_CHECK(psum->array[0]->id == 0);
    TEST_CHECK(psum->array[1]->id == 1);
    TEST_CHECK(psum->array[2]->id == 2);
    TEST_CHECK(psum->array[3]->id = 3);
    TEST_CHECK(psum->array[0]->value == 0);
    TEST_CHECK(psum->array[1]->value == 3);
    TEST_CHECK(psum->array[2]->value == 5);
    TEST_CHECK(psum->array[3]->value == 7);

    delete_hist(psum);
    delete_hist(hist);

}

void test_hash(void){
    TEST_CHECK(hash(2, 0) == 0);
    TEST_CHECK(hash(2, 5) == 1);
    TEST_CHECK(hash(3, 56) == 0);
	TEST_CHECK(hash(4, 23) == 7);
    TEST_CHECK(hash(5, 45) == 13);
    TEST_CHECK(hash(7, 16) == 16);
    TEST_CHECK(hash(8, 975) == 207);
}

void re_ordered_test(void){
    init_scheduler();
    // n = 2 L2 >= 2
    // create histogram
    uint64_t row[] = {0,1,2,3,4,5,6,8};
    int row_size = 8;

    Histogram hist = create_hist(2);

    for(int i=0; i<3; i++)
        update_hist(hist, 0);

    for(int i=0; i<2; i++)
        update_hist(hist, 1);

    for(int i=0; i<2; i++)
        update_hist(hist, 2);

    update_hist(hist, 3);
    
    //create psum
    Histogram psum = create_psum(hist);

    //test the re_ordered
    Tuple* array = create_id_array(row, row_size);
    Relation relation = re_ordered(array, row_size, psum, 2);
    delete_id_array(array, row_size);

    TEST_CHECK(relation->array[0]->value == 0);
    TEST_CHECK(relation->array[0]->id == 0);

    TEST_CHECK(relation->array[1]->value == 4);
    TEST_CHECK(relation->array[1]->id == 4);

    TEST_CHECK(relation->array[2]->value == 8);
    TEST_CHECK(relation->array[2]->id == 7);

    TEST_CHECK(relation->array[3]->value == 1);
    TEST_CHECK(relation->array[3]->id == 1);

    TEST_CHECK(relation->array[4]->value == 5);
    TEST_CHECK(relation->array[4]->id == 5);

    TEST_CHECK(relation->array[5]->value == 2);
    TEST_CHECK(relation->array[5]->id == 2);

    TEST_CHECK(relation->array[6]->value == 6);
    TEST_CHECK(relation->array[6]->id == 6);

    TEST_CHECK(relation->array[7]->value == 3);
    TEST_CHECK(relation->array[7]->id == 3);

    TEST_CHECK(relation->num_tuples == 8);

    destroy_scheduler();
    delete_hist(hist);
    delete_psum(psum);
    delete_relation(relation);
}

void partition_test_case_a(void){
    //n = 4 and l2 >= 4
    // uint64_t row[] = {98,81,3,36,69,53,101,22,77,93,14,94,95,87,24,88,89,90,75,92,48,17};
    // int row_size = 22;

    /* CASE A */

    uint64_t row[] = {0,1};
    int row_size = 2;

    Partition partition = partitioning(row, row_size, true);
    
    TEST_CHECK(partition->basic_pass->psum == NULL);
    
    for(int i=0; i<row_size; i++){
        TEST_CHECK(partition->basic_pass->relation->array[i]->value == row[i]);
        TEST_CHECK(partition->basic_pass->relation->array[i]->id == i);
    }

    delete_partition(partition);
}

void partition_test_case_b(void){
    /* For n upper bound = 4 (creates 16 buckets) */
    init_scheduler();
    uint64_t row[] = {2,3,1,0,22,33,11,10,32,42};
    int row_size = 10;

    /***
     * L2:2 N_LowerBound:2, N_UpperBound:4
     * 
     * hashes in 0  -> 0,32
     * hashes in 1  -> 1,33
     * hashes in 2  -> 2
     * hashes in 3  -> 3
     * hashes in 6  -> 22
     * hashes in 10 -> 10,42
     * hashes in 11 -> 11
     * 
     * Άρα ο τελικός πίνακας θα πρέπει να είναι ο R = [0,32,1,33,2,3,22,10,42,11] 
    ***/

    Partition partition = partitioning(row, row_size, true);

    //todo check why its not null
    //TEST_CHECK(partition->second_pass == NULL);

    TEST_CHECK(partition->basic_pass->relation->array[0]->value == 0);
    TEST_CHECK(partition->basic_pass->relation->array[0]->id == 3);

    TEST_CHECK(partition->basic_pass->relation->array[1]->value == 32);
    TEST_CHECK(partition->basic_pass->relation->array[1]->id == 8);

    TEST_CHECK(partition->basic_pass->relation->array[2]->value == 1);
    TEST_CHECK(partition->basic_pass->relation->array[2]->id == 2);

    TEST_CHECK(partition->basic_pass->relation->array[3]->value == 33);
    TEST_CHECK(partition->basic_pass->relation->array[3]->id == 5);

    TEST_CHECK(partition->basic_pass->relation->array[4]->value == 2);
    TEST_CHECK(partition->basic_pass->relation->array[4]->id == 0);

    TEST_CHECK(partition->basic_pass->relation->array[5]->value == 3);
    TEST_CHECK(partition->basic_pass->relation->array[5]->id == 1);

    TEST_CHECK(partition->basic_pass->relation->array[6]->value == 22);
    TEST_CHECK(partition->basic_pass->relation->array[6]->id == 4);

    TEST_CHECK(partition->basic_pass->relation->array[7]->value == 10);
    TEST_CHECK(partition->basic_pass->relation->array[7]->id == 7);

    TEST_CHECK(partition->basic_pass->relation->array[8]->value == 42);
    TEST_CHECK(partition->basic_pass->relation->array[8]->id == 9);

    TEST_CHECK(partition->basic_pass->relation->array[9]->value == 11);
    TEST_CHECK(partition->basic_pass->relation->array[9]->id == 6);

    destroy_scheduler();
    delete_partition(partition);   
}

void partition_test_case_c(void){
    uint64_t row[] = {0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8};
    int row_size = 16;

    // L2 = 2, N_LOWER_BOUND 2, N_UPPER_BOUND 4, N_SECOND_UPPER_BOUND = 8
    init_scheduler();
    Partition partition = partitioning(row, row_size, true);

    TEST_CHECK(partition->second_pass != NULL);
    TEST_CHECK(partition->basic_pass->psum->size == 16);
    TEST_CHECK(partition->second_pass->psum->size == 256);
    TEST_CHECK(partition->basic_pass->relation->n_bits == 4);
    TEST_CHECK(partition->second_pass->relation->n_bits == 8);

    int pos = partition->basic_pass->psum->array[15]->value;
    TEST_CHECK(partition->second_pass->relation->num_tuples == pos);

    pos = partition->basic_pass->psum->array[14]->value;
    TEST_CHECK(partition->second_pass->relation->num_tuples == pos);
    destroy_scheduler();
    delete_partition(partition);   
}

// Λίστα με όλα τα tests προς εκτέλεση
TEST_LIST = {
    { "test_create_hist", test_create_hist },
    { "test_update_hist", test_update_hist },
    { "psum_test", psum_test },
	{ "test_hash", test_hash },
    { "re_ordered_test", re_ordered_test },
    { "partition_test_case_a", partition_test_case_a },
    { "partition_test_case_b", partition_test_case_b },
    { "partition_test_case_c", partition_test_case_c },

	{ NULL, NULL } // τερματίζουμε τη λίστα με NULL
};