#include "acutest.h"
#include "init.h"
#include "string.h"


void parse_predicates_test(void){

    //create Predicates Queue
    PredicatesQueue pq = predicates_queue_create(delete_pq, print_pq);

    char* test_token = "0.2=1.0&0.1=2.0&0.2>3499";

    /*
     Queue is like this:

        0.2 > 3499

        0.2 = 1.0

        0.1 = 2.0
    */
    
    parse_predicates(pq, test_token);

    TEST_CHECK(pq->size == 3);
    Node current = pq->head;
    PredicateNode node = (PredicateNode)current->data;

    TEST_CHECK(strcmp(node->left_part, "0.2") == 0);
    TEST_CHECK(node->op == '>');
    TEST_CHECK(strcmp(node->right_part, "3499") == 0);

    current = current->next;
    node = (PredicateNode)current->data;

    TEST_CHECK(strcmp(node->left_part, "0.2") == 0);
    TEST_CHECK(node->op == '=');
    TEST_CHECK(strcmp(node->right_part, "1.0") == 0);

    current = current->next;
    node = (PredicateNode)current->data;

    TEST_CHECK(strcmp(node->left_part, "0.1") == 0);
    TEST_CHECK(node->op == '=');
    TEST_CHECK(strcmp(node->right_part, "2.0") == 0);

    pq->destroy_value(pq);
    return;

}

void parse_relations_id_test(void){

    char* test_token = "3 0 1";
    int relation_num = 0;
    int* array_value;

    parse_relations_id(test_token, &relation_num, &array_value);

    TEST_CHECK(relation_num == 3);
    TEST_CHECK(array_value[0] == 3);
    TEST_CHECK(array_value[1] == 0);
    TEST_CHECK(array_value[2] == 1);

    free(array_value);
    return;
}

void intermediateResultsFilter_test(void){

    int size = 10;
    int cols = 3;
    int* array = malloc(sizeof(int)* size);

    for(int i=0; i<size; i++)
        array[i] = i;
    
    IntermediateResultsFilter intermediate_results = intermediateResultsFilter_create(array, size, cols, true);

    TEST_CHECK(intermediate_results->filter_array != NULL);
    TEST_CHECK(intermediate_results->size_filter_array == 10);

    for(uint64_t i=0; i<intermediate_results->size_filter_array; i++)
        TEST_CHECK(array[i] == i);
    
    intermediateResultsFilter_delete(intermediate_results, true);
}

void create_MappedRelation_test(){
    char* filename = "../workloads/small/r0";
    
    int fd = open(filename, O_RDONLY);
    if(fd == -1){
        printf("open failed \n");
        return;
    }

    struct stat sb;
    if(fstat(fd, &sb) == -1)
        return;
    
    MappedRelation map = create_mappedRelation((uint64_t)sb.st_size, fd);
    
    TEST_CHECK(map->size == sb.st_size);
    TEST_CHECK(map->cols == 3);
    TEST_CHECK(map->rows == 1561);

    size_t start, end;
    uint64_t* data = (uint64_t*)map->addr;

    get_col(map, 0, &start, &end);  // first element of first col (1)
    TEST_CHECK(data[start] == 1);   // check the first element of col 
    TEST_CHECK(data[end - (uint64_t)1] == 4690);   // check the last element of col 
    
    get_col(map, 1, &start, &end);  // first element of second col (8463)
    TEST_CHECK(data[start] == 8463);
    TEST_CHECK(data[end - (uint64_t)1] == 5930);

    get_col(map, 2, &start, &end);  // first element of third col (582)
    TEST_CHECK(data[start] == 582);
    TEST_CHECK(data[end - (uint64_t)1] == 4307);
    
    delete_MappedRelation(map);
    
    close(fd);
}

void joinIntermidiateArray_test(void){
    int relation_num = 3;
    JoinIntermidiateArray joinArray = joinIntermidiateArray_create(relation_num/2, relation_num);

    TEST_CHECK(joinArray != NULL);
    TEST_CHECK(joinArray->size_array == 1);
    TEST_CHECK(joinArray->size_pos == 3);
    TEST_CHECK(joinArray->in_array == 0);
    
    for(int i=0; i< relation_num; i++){
        TEST_CHECK(joinArray->pos[i] == -1);
    }

    for(int i=0; i< relation_num/2; i++){
        TEST_CHECK(joinArray->array[i] == NULL);
    }

    joinIntermidiateArray_delete(joinArray);
}

TEST_LIST = {
    { "create_MappedRelation_test", create_MappedRelation_test },
    { "intermediateResultsFilter_test", intermediateResultsFilter_test},
    { "parse_relations_id_test", parse_relations_id_test},
    { "parse_predicates_test", parse_predicates_test},
    { "joinIntermidiateArray_test" , joinIntermidiateArray_test},
	{ NULL, NULL } // τερματίζουμε τη λίστα με NULL
};
