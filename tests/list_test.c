#include "acutest.h"
#include "data_structures.h"

void list_create_test(void){
    List list = list_create(free, NULL);

    TEST_CHECK(list != NULL);
    TEST_CHECK(list->destroy_value != NULL);
    TEST_CHECK(list->print_value == NULL);
    TEST_CHECK(list->size == 0);
    TEST_CHECK(list->head == NULL && list->last == NULL);

    list->destroy_value(list);
}

void delete_list(Pointer list){
    Node current_node = ((List)list)->head;
    Node next_node;
    
    while(current_node != NULL){
        free((Node)current_node->data);

        next_node = current_node->next;
        free(current_node);
        current_node = next_node;
    }

    free(list);
}

void add_test(void){
    List list = list_create(delete_list, NULL);
    
    int* n = malloc(sizeof(int));
    *n = 1;
    add(n, list);

    int* n2 = malloc(sizeof(int));
    *n2 = 2;
    add(n2, list);

    int* n3 = malloc(sizeof(int));
    *n3 = 3;
    add(n3, list);

    //traverse list and check that the values are in-order
    TEST_CHECK(*(int*)list->head->data == 1);
    TEST_CHECK(*(int*)list->head->next->data == 2);
    TEST_CHECK(*(int*)list->head->next->next->data == 3);

    //checks for leaks
    list->destroy_value(list);
}

void pop_test(void){
    List list = list_create(delete_list, NULL);
    
    for(int i=1; i<4; i++){
        int* n = malloc(sizeof(int));
        *n = i;
        add(n, list);
    }

    for(int i=1; i<4; i++){
        Node node = pop(list);
        TEST_CHECK(*(int*)node->data == i);
        free(node->data);
        free(node);
    }

    //checks for leaks
    list->destroy_value(list);
}

// Λίστα με όλα τα tests προς εκτέλεση
TEST_LIST = {
    { "list_create_test", list_create_test },
    { "add_test", add_test},
    { "pop_test" , pop_test},
	{ NULL, NULL } // τερματίζουμε τη λίστα με NULL
};