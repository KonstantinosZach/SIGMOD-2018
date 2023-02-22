#include "acutest.h"
#include "operations.h"

void delete_list(Pointer list){
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

static void delete_viewslist(Pointer list){
    Node current_node = ((List)list)->head;
    Node next_node;
    
    while(current_node != NULL){
        free(current_node->data);
        next_node = current_node->next;
        free(current_node);
        current_node = next_node;
    }

    free(list);
    return;
}

static void add_map(char* r, List list){
    int fd = open(r, O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    MappedRelation map = create_mappedRelation((uint64_t)sb.st_size, fd);
    add(map, list);
    close(fd);
}

void split_test(void){
    PredicateNode node = predicate_node_create("0.2" , '>' , "3499");

    int number_of_array, col;
    split_part(node->left_part, &number_of_array, &col);
    TEST_CHECK(number_of_array == 0);
    TEST_CHECK(col == 2);

    free(node);
}

void apply_sum_test(void){
    init_scheduler();

    List relation_list = list_create(delete_list, NULL);
    add_map("../input/workloads/small/r0", relation_list);
    add_map("../input/workloads/small/r1", relation_list);
    add_map("../input/workloads/small/r2", relation_list);
    add_map("../input/workloads/small/r3", relation_list);
    
    PredicatesQueue pq = predicates_queue_create(delete_pq, print_pq);
    List viewslist = list_create(delete_viewslist, NULL);
    
    int* array_value;
    int relation_num = 0;
    int* views;
    char* predicate = malloc(40 * sizeof(char));
    strcpy(predicate, "3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1");
    parse_line(pq, viewslist, &array_value, &relation_num, &views, predicate);
    
    PredicateNode node2;
    IntermediateResultsFilter* filters= malloc(sizeof(IntermediateResultsFilter)*relation_num);
    
    for(int i=0; i<relation_num; i++)
        filters[i] = NULL;
    JoinIntermidiateArray joinArray = joinIntermidiateArray_create(relation_num/2, relation_num);
    
    while((node2 = predicates_queue_pop(pq)) != NULL){
        if(node2->filter){
            apply_filter(relation_list, array_value, filters, node2);
            
            update_filter_statistics(relation_list, array_value, filters, node2);
        }
        else{
            apply_join(relation_list, array_value, filters, joinArray, node2);
        }
        predicate_node_delete(node2);
    }
    
    //here we traverse the list of views to take the sums
    Node current_node = viewslist->head;
    Node next_node;
    int i=0;
    while(current_node != NULL){
        uint64_t sum;
        apply_sum(filters, relation_list, array_value, joinArray, current_node, &sum);
        next_node = current_node->next;
        current_node = next_node;
        if(current_node != NULL)
            fprintf(stdout, " ");
        if(i==0)
            TEST_CHECK(sum == 26468015);
        else
            TEST_CHECK(sum == 32533054);
        i++;
    }
    
    relation_list->destroy_value(relation_list);
    pq->destroy_value(pq);
    viewslist->destroy_value(viewslist);
    
    //delete intermidate results
    for(int i=0; i<relation_num; i++){
        if(filters[i] != NULL){
            intermediateResultsFilter_delete(filters[i], true);
        }
    }
    free(filters);
    
    joinIntermidiateArray_delete(joinArray);
    free(array_value);
    free(predicate);
    destroy_scheduler();
}

void apply_filter_test(void){
    List list = list_create(delete_list, NULL);
    add_map("../input/workloads/small/r0", list);
    add_map("../input/workloads/small/r1", list);
    add_map("../input/workloads/small/r2", list);
    add_map("../input/workloads/small/r3", list);

    PredicatesQueue pq = predicates_queue_create(delete_pq, print_pq);
    List viewslist = list_create(delete_viewslist, NULL);

    int* array_value;
    int relation_num = 0;
    int* views;
    char line[4096] = "3 0 1|0.2=1.0&0.1=2.0&0.2>4689|1.2 0.1";

    parse_line(pq, viewslist, &array_value, &relation_num, &views, line);
    PredicateNode node;

    IntermediateResultsFilter* filters= malloc(sizeof(IntermediateResultsFilter)*relation_num);
    for(int i=0; i<relation_num; i++){
        filters[i] = NULL;
    }

    while((node = predicates_queue_pop(pq)) != NULL){
        if(node->filter){
            apply_filter(list, array_value, filters, node);
        }
        predicate_node_delete(node);
    }

    TEST_CHECK(filters[0]->cols == 4);
    TEST_CHECK(filters[0]->filtered == true);
    TEST_CHECK(filters[0]->size_filter_array == 14);
    
    TEST_CHECK(filters[0]->filter_array[0] == 290);
    TEST_CHECK(filters[0]->filter_array[1] == 532);
    TEST_CHECK(filters[0]->filter_array[2] == 696);
    TEST_CHECK(filters[0]->filter_array[3] == 783);
    TEST_CHECK(filters[0]->filter_array[4] == 1994);
    TEST_CHECK(filters[0]->filter_array[5] == 2166);
    TEST_CHECK(filters[0]->filter_array[6] == 4162);
    TEST_CHECK(filters[0]->filter_array[7] == 9236);
    TEST_CHECK(filters[0]->filter_array[8] == 10018);
    TEST_CHECK(filters[0]->filter_array[9] == 13238);
    TEST_CHECK(filters[0]->filter_array[10] == 18376);
    TEST_CHECK(filters[0]->filter_array[11] == 21236);
    TEST_CHECK(filters[0]->filter_array[12] == 21556);
    TEST_CHECK(filters[0]->filter_array[13] == 22760);

    for(int i=0; i<relation_num; i++){
        if(filters[i] != NULL){
            intermediateResultsFilter_delete(filters[i], true);
        }
    }
    free(filters);
    viewslist->destroy_value(viewslist);
    list->destroy_value(list);
    pq->destroy_value(pq);
    free(array_value);
}

void apply_join_test(void){
    init_scheduler();
    
    List relation_list = list_create(delete_list, NULL);
    add_map("../input/workloads/small/r0", relation_list);
    add_map("../input/workloads/small/r1", relation_list);
    add_map("../input/workloads/small/r2", relation_list);
    add_map("../input/workloads/small/r3", relation_list);
    
    PredicatesQueue pq = predicates_queue_create(delete_pq, print_pq);
    List viewslist = list_create(delete_viewslist, NULL);
    
    int* array_value;
    int relation_num = 0;
    int* views;
    
    char* predicate = malloc(40 * sizeof(char));
    strcpy(predicate, "3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1");
    parse_line(pq, viewslist, &array_value, &relation_num, &views, predicate);
    
    PredicateNode node2;
    
    IntermediateResultsFilter* filters= malloc(sizeof(IntermediateResultsFilter)*relation_num);
    for(int i=0; i<relation_num; i++)
        filters[i] = NULL;
    JoinIntermidiateArray joinArray = joinIntermidiateArray_create(relation_num/2, relation_num);
    
    TEST_CHECK(joinArray->in_array == 0);
    TEST_CHECK(joinArray->array[0] == NULL);

    int counter_join = 0;
    while((node2 = predicates_queue_pop(pq)) != NULL){
        if(node2->filter){
            apply_filter(relation_list, array_value, filters, node2);
            
            update_filter_statistics(relation_list, array_value, filters, node2);
        }
        else{
            apply_join(relation_list, array_value, filters, joinArray, node2);
            
            /* we have 3 relations */
            TEST_CHECK(joinArray->size_pos == 3);
            TEST_CHECK(joinArray->array[0]->number_of_relations == 3);
            /* size is number of relations / 2 */
            TEST_CHECK(joinArray->size_array == 1);
            
            if(counter_join == 0){
                /* intermediates in array */
                TEST_CHECK(joinArray->in_array == 1);
                /* intermediate array */
                TEST_CHECK(joinArray->array[0] != NULL);
                /* arrays 0 and 1 are in the first intermediate */
                TEST_CHECK(joinArray->pos[0] == 0);
                TEST_CHECK(joinArray->pos[1] == 0);
                /* array 2 is not yet assigned to an intermediate */
                TEST_CHECK(joinArray->pos[2] == -1);
                /* intermediate now holds 2 columns one for each of the arrays joined (0,1) */
                TEST_CHECK(joinArray->array[0]->cols_id_array == 2);
            }
            else{
                /* intermediates in array */
                TEST_CHECK(joinArray->in_array == 1);
                /* intermediate array */
                TEST_CHECK(joinArray->array[0] != NULL);
                /* array 2 is now joined with 0 and one in the first intermediate */
                TEST_CHECK(joinArray->pos[0] == 0);
                TEST_CHECK(joinArray->pos[1] == 0);
                TEST_CHECK(joinArray->pos[2] == 0);
                /* intermediate now holds 3 columns one for each of the arrays joined ((0,1),3) */
                TEST_CHECK(joinArray->array[0]->cols_id_array == 3);
            }
            
            counter_join++;
        }
        predicate_node_delete(node2);
    }
    
    relation_list->destroy_value(relation_list);
    pq->destroy_value(pq);
    viewslist->destroy_value(viewslist);
    //delete intermidate results
    for(int i=0; i<relation_num; i++){
        if(filters[i] != NULL){
            intermediateResultsFilter_delete(filters[i], true);
        }
    }
    free(filters);
    joinIntermidiateArray_delete(joinArray);
    free(array_value);
    free(predicate);
    destroy_scheduler();
}

TEST_LIST = {
    { "split_test", split_test },
    { "apply_filter_test", apply_filter_test },
    { "apply_sum_test", apply_sum_test},
    { "apply_join_test", apply_join_test},
    
	{ NULL, NULL } // τερματίζουμε τη λίστα με NULL
};