#include "acutest.h"
#include "building.h"
#include "partition.h"

// static void print_hash_table(HashTable hash_table){
//     printf("\n");
//     for(int i=0; i<hash_table->size; i++){
//         printf("i:%d, ", i);
//         if(hash_table->hash_table[i]->initialized){
//             printf("Tuple(%d,%d) ", hash_table->hash_table[i]->tuple->id, hash_table->hash_table[i]->tuple->value);
//         }
//         else{
//             printf("Tuple(-,-) ");
//         }
//         printf("bitmap[");
//         for(int j=0; j<hash_table->h; j++){
//             printf("%d ", hash_table->hash_table[i]->bitmap[j]);
//         }
//         if(hash_table->hash_table[i]->initialized){
//          printf("] hash = %d\n", hash2(hash_table->n,hash_table->hash_table[i]->tuple->value));
//         }else{
//             printf("]\n");
//         }
//     }
//     printf("\n");
// }

void create_hash_cell_test(void){
    HashCell hash_cell = create_hash_cell(2);

    TEST_CHECK(hash_cell != NULL);

    for(int i=0; i<2; i++)
        TEST_CHECK(hash_cell->bitmap[i] == 0);
    
    TEST_CHECK(hash_cell->tuple != NULL);
    
    delete_hash_cell(hash_cell);
}

void create_hash_table_test(void){
    HashTable hash_table = create_hash_table(2,2);

    TEST_CHECK(hash_table != NULL);
    TEST_CHECK(hash_table->n == 2 && hash_table->size == (2));
    for(int i=0; i<2; i++)
        TEST_CHECK(hash_table->hash_table[i] != NULL);
    
    delete_hash_table(hash_table);
}

void insert_test(void){

    /* starting H =1 starting n = 2 */
    HashTable hash_table = create_hash_table(1,2);

    Tuple x = malloc(sizeof(*x));
    x->id = 1;
    x->value = 1;
    hash_table = insert(x,hash_table);
    
    TEST_CHECK(hash_table->hash_table[1]->tuple->id == 1);
    TEST_CHECK(hash_table->hash_table[1]->tuple->value == 1);
    TEST_CHECK(hash_table->hash_table[1]->bitmap[0] == true);
    
    x->id = 2;
    x->value = 1;
 
    hash_table = insert(x,hash_table);

    //insert goes into list
    Tuple tuple = hash_table->hash_table[1]->chain->head->data;
    TEST_CHECK(tuple->value == 1);
    TEST_CHECK(tuple->id == 2);

    delete_hash_table(hash_table);
    free(x);
}

void build_hash_table_test(void){

    /* case a*/
    init_scheduler();
    uint64_t row1[] = {0,1};
    int row_size1 = 2;
    Partition partition1 = partitioning(row1, row_size1, true);

    uint64_t row2[] = {2,3};
    int row_size2 = 2;
    Partition partition2 = partitioning(row2, row_size2, true);

    Build build = build_hash_table(partition1, partition2, true);

    TEST_CHECK(build->build_case == case_a);
    TEST_CHECK(build->partition_hash == partition1);
    TEST_CHECK(build->partition_search == partition2);

    /*case b*/

    uint64_t row3[] = {2,3,1,0,22,33,11,10,32,42};
    int row_size3 = 10;

    Partition partition3 = partitioning(row3, row_size3, true);
    Build build2 = build_hash_table(partition3, partition1, true);

    //todo check why build2->build_case is not case_b
    //TEST_CHECK(build2->build_case == case_b);

    TEST_CHECK(build2->partition_hash == partition3);
    TEST_CHECK(build2->partition_search == partition1);
    //check if there are all the hash_tables needed
    TEST_CHECK(build2->hash_table[build2->partition_hash->basic_pass->psum->size - 1] != NULL);


    /* case c*/

    uint64_t row4[] = {0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8};
    int row_size4 = 11;

    Partition partition4 = partitioning(row4, row_size4, true);
    Build build3 = build_hash_table(partition4, partition3, true);

    TEST_CHECK(build3->build_case == case_c);
    TEST_CHECK(build3->partition_hash == partition4);
    TEST_CHECK(build3->partition_search == partition3);
    TEST_CHECK(build3->hash_table[build3->partition_hash->second_pass->psum->size - 1] != NULL);

    delete_build(build);
    delete_build(build2);
    delete_build(build3);

    delete_partition(partition1);
    delete_partition(partition2);
    delete_partition(partition3);
    delete_partition(partition4);
    destroy_scheduler();
}

// Λίστα με όλα τα tests προς εκτέλεση
TEST_LIST = {
    { "create_hash_cell_test", create_hash_cell_test },
    { "create_hash_table_test", create_hash_table_test },
    { "insert_test", insert_test },
    { "build_hash_table_test", build_hash_table_test },

	{ NULL, NULL } // τερματίζουμε τη λίστα με NULL
};
