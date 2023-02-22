#include "acutest.h"
#include "filter_statistics.h"

void delete_list_with_maps(Pointer list){
    Node current_node = ((List)list)->head;
    Node next_node;
    
    while(current_node != NULL){
        MappedRelation map = (MappedRelation)current_node->data;
        delete_MappedRelation(map);

        next_node = current_node->next;
        free(current_node);
        current_node = next_node;
    }

    free(list);
    return;
}

void calculate_statistics_test(void){

    int fd = open("../input/workloads/small/r0", O_RDONLY);

    struct stat sb;
    fstat(fd, &sb);

    MappedRelation map = create_mappedRelation((uint64_t)sb.st_size, fd);

    TEST_CHECK(map->statistics[0] != NULL);
    TEST_CHECK(map->statistics[0]->f == 1561);
    TEST_CHECK(map->statistics[0]->l == 1);
    TEST_CHECK(map->statistics[0]->u == 4690);
    TEST_CHECK(map->statistics[0]->d == 1561);

    TEST_CHECK(map->statistics[1] != NULL);
    TEST_CHECK(map->statistics[1]->f == 1561);
    TEST_CHECK(map->statistics[1]->l == 4403);
    TEST_CHECK(map->statistics[1]->u == 10262);
    TEST_CHECK(map->statistics[1]->d == 1365);

    TEST_CHECK(map->statistics[2] != NULL);
    TEST_CHECK(map->statistics[2]->f == 1561);
    TEST_CHECK(map->statistics[2]->l == 197);
    TEST_CHECK(map->statistics[2]->u == 8632);
    TEST_CHECK(map->statistics[2]->d == 1431);

    delete_MappedRelation(map);
    close(fd);
}

void update_filter_statistics_test(void){
    int fd = open("../input/workloads/small/r0", O_RDONLY);

    struct stat sb;
    fstat(fd, &sb);

    MappedRelation map = create_mappedRelation((uint64_t)sb.st_size, fd);

    List list = list_create(delete_list_with_maps, NULL);
    int* array_value = calloc(sizeof(int), 1);
    IntermediateResultsFilter filters[1];
    filters[0] = NULL;
    add(map, list);

    PredicateNode node = predicate_node_create("0.0" , '>' , "500");
    apply_filter(list, array_value, filters, node);
    update_filter_statistics(list, array_value, filters, node);

    TEST_CHECK(filters[0]->stats[0]->u == 4690);
    TEST_CHECK(filters[0]->stats[0]->l == 500);
    TEST_CHECK(filters[0]->stats[0]->d == 1394);
    TEST_CHECK(filters[0]->stats[0]->f == 1394);
    free(node);

    node = predicate_node_create("0.0" , '<' , "1000");
    apply_filter(list, array_value, filters, node);
    update_filter_statistics(list, array_value, filters, node);

    //when value does not exist
    TEST_CHECK(filters[0]->stats[0]->u == 1000);
    TEST_CHECK(filters[0]->stats[0]->l == 500);
    TEST_CHECK(filters[0]->stats[0]->d == 166);
    TEST_CHECK(filters[0]->stats[0]->f == 166);
    free(node);

    node = predicate_node_create("0.0" , '=' , "635");
    apply_filter(list, array_value, filters, node);
    update_filter_statistics(list, array_value, filters, node);

    //when value exists
    TEST_CHECK(filters[0]->stats[0]->u == 635);
    TEST_CHECK(filters[0]->stats[0]->l == 635);
    TEST_CHECK(filters[0]->stats[0]->d == 1);
    TEST_CHECK(filters[0]->stats[0]->f == 1);
    free(node);

    node = predicate_node_create("0.0" , '=' , "500");
    apply_filter(list, array_value, filters, node);
    update_filter_statistics(list, array_value, filters, node);

    //when value does not exist
    TEST_CHECK(filters[0]->stats[0]->u == 500);
    TEST_CHECK(filters[0]->stats[0]->l == 500);
    TEST_CHECK(filters[0]->stats[0]->d == 0);
    TEST_CHECK(filters[0]->stats[0]->f == 0);
    TEST_CHECK(filters[0]->stats[1]->d == 0);
    TEST_CHECK(filters[0]->stats[1]->d == 0);
    free(node);

    free(array_value);
    intermediateResultsFilter_delete(filters[0], true);
    list->destroy_value(list);
    close(fd);
}

void update_filter_statistics_same_array_test(void){

    int fd = open("../input/workloads/small/r3", O_RDONLY);

    struct stat sb;
    fstat(fd, &sb);

    MappedRelation map = create_mappedRelation((uint64_t)sb.st_size, fd);
    List list = list_create(delete_list_with_maps, NULL);
    add(map, list);

    IntermediateResultsFilter filters[1];
    filters[0] = NULL;

    int* array_value = calloc(sizeof(int), 1);
    PredicateNode node = predicate_node_create("0.2" , '=' , "0.3");

    apply_filter(list, array_value, filters, node);
    update_filter_statistics(list, array_value, filters, node);

    TEST_CHECK(filters[0]->stats[2]->u == 4690);
    TEST_CHECK(filters[0]->stats[2]->l == 2624);
    TEST_CHECK(filters[0]->stats[2]->d == 10);
    TEST_CHECK(filters[0]->stats[2]->f == 11);

    TEST_CHECK(filters[0]->stats[3]->u == 4690);
    TEST_CHECK(filters[0]->stats[3]->l == 2624);
    TEST_CHECK(filters[0]->stats[3]->d == 10);
    TEST_CHECK(filters[0]->stats[3]->f == 11);

    TEST_CHECK(filters[0]->stats[0]->u == 69465);
    TEST_CHECK(filters[0]->stats[0]->l == 5);
    TEST_CHECK(filters[0]->stats[0]->d == 11);
    TEST_CHECK(filters[0]->stats[0]->f == 11);

    TEST_CHECK(filters[0]->stats[1]->u == 11410);
    TEST_CHECK(filters[0]->stats[1]->l == 4);
    TEST_CHECK(filters[0]->stats[1]->d == 10);
    TEST_CHECK(filters[0]->stats[1]->f == 11);

    free(node);
    free(array_value);
    intermediateResultsFilter_delete(filters[0], true);
    list->destroy_value(list);
    close(fd);
}

TEST_LIST = {
    { "update_filter_statistics_test", update_filter_statistics_test },
    { "calculate_statistics_test", calculate_statistics_test},
    { "update_filter_statistics_same_array_test", update_filter_statistics_same_array_test},

	{ NULL, NULL }
};

