#include "acutest.h"
#include "predicatesQueue.h"

void delete_predicate_queue(Pointer queue){

    Node current_node = ((List)queue)->head;
    Node next_node;
    
    while(current_node != NULL){
        PredicateNode node = (PredicateNode)current_node->data;
        predicate_node_delete(node);

        next_node = current_node->next;
        free(current_node);
        current_node = next_node;
    }

    free(queue);
}

void predicate_queue_create_test(void){

    PredicatesQueue queue = predicates_queue_create(delete_predicate_queue, NULL);
    TEST_CHECK(queue != NULL);
    TEST_CHECK(queue->size == 0);
    TEST_CHECK(predicates_queue_pop(queue) == NULL);
    queue->destroy_value(queue);
}

void predicate_queue_add_test(void){

    PredicatesQueue queue = predicates_queue_create(delete_predicate_queue, NULL);
    PredicateNode node;
    
    char* right_part = malloc(sizeof(char) * 10);
    strcpy(right_part, "0.0");
    char* left_part = malloc(sizeof(char) * 10);
    strcpy(left_part, "0.1");
    node = predicate_node_create(right_part, '=', left_part);
    predicates_queue_add(node, queue);

    char* right_part2 = malloc(sizeof(char) * 10);
    strcpy(right_part2, "1.0");
    char* left_part2 = malloc(sizeof(char) * 10);
    strcpy(left_part2, "1.1");
    node = predicate_node_create(right_part2, '<', left_part2);
    predicates_queue_add(node, queue);

    char* right_part3 = malloc(sizeof(char) * 10);
    strcpy(right_part3, "2.0");
    char* left_part3 = malloc(sizeof(char) * 10);
    strcpy(left_part3, "2.1");
    node = predicate_node_create(right_part3, '=', left_part3);
    predicates_queue_add(node, queue);

    char* right_part4 = malloc(sizeof(char) * 10);
    strcpy(right_part4, "3.0");
    char* left_part4 = malloc(sizeof(char) * 10);
    strcpy(left_part4, "3.1");
    node = predicate_node_create(right_part4, '>', left_part4);
    predicates_queue_add(node, queue);

    char* right_part5 = malloc(sizeof(char) * 10);
    strcpy(right_part5, "1.0");
    char* left_part5 = malloc(sizeof(char) * 10);
    strcpy(left_part5, "2.1");
    node = predicate_node_create(right_part5, '=', left_part5);
    predicates_queue_add(node, queue);

    /*
        After these inserts the queue should be like this
        [
            3.0 > 3.1
            2.0 = 2.1
            1.0 < 1.1
            0.0 = 0.1
            1.0 = 2.1
        ]
    */
    node = predicates_queue_pop(queue);
    TEST_CHECK(strcmp(node->left_part, "3.0") == 0);
    TEST_CHECK(strcmp(node->right_part, "3.1") == 0);
    //when we pop a node we must delete it manually after
    predicate_node_delete(node);

    node = predicates_queue_pop(queue);
    TEST_CHECK(strcmp(node->left_part, "2.0") == 0);
    TEST_CHECK(strcmp(node->right_part, "2.1") == 0);
    predicate_node_delete(node);

    node = predicates_queue_pop(queue);
    TEST_CHECK(strcmp(node->left_part, "1.0") == 0);
    TEST_CHECK(strcmp(node->right_part, "1.1") == 0);
    predicate_node_delete(node);

    node = predicates_queue_pop(queue);
    TEST_CHECK(strcmp(node->left_part, "0.0") == 0);
    TEST_CHECK(strcmp(node->right_part, "0.1") == 0);
    predicate_node_delete(node);

    node = predicates_queue_pop(queue);
    TEST_CHECK(strcmp(node->left_part, "1.0") == 0);
    TEST_CHECK(strcmp(node->right_part, "2.1") == 0);
    predicate_node_delete(node);

    //no elements left in the queue
    TEST_CHECK(queue->size == 0);
    node = predicates_queue_pop(queue);
    TEST_CHECK(node == NULL);

    //we add one element in an empty queue
    char* right_part6 = malloc(sizeof(char) * 10);
    strcpy(right_part6, "4.0");
    char* left_part6 = malloc(sizeof(char) * 10);
    strcpy(left_part6, "4.1");
    node = predicate_node_create(right_part6, '=', left_part6);
    predicates_queue_add(node, queue);

    //at this point the queue has only one element, we call the destroy function to delete the queue
    TEST_CHECK(queue->size == 1);
    queue->destroy_value(queue);
}

// Λίστα με όλα τα tests προς εκτέλεση
TEST_LIST = {
    { "predicate_queue_create_test", predicate_queue_create_test },
    { "predicate_queue_add_test", predicate_queue_add_test },
	{ NULL, NULL } // τερματίζουμε τη λίστα με NULL
};
