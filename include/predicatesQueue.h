#pragma once

#include "data_structures.h"

typedef List PredicatesQueue;

struct predicateNode{
    char* left_part;
    char op;
    char* right_part;
    boolean filter;
};

typedef struct predicateNode* PredicateNode;

PredicateNode predicate_node_create(char* left_part, char op, char* right_part);

void predicate_node_delete(PredicateNode node);

PredicatesQueue predicates_queue_create(DestroyFunc destroy_value, PrintFunc print_value);

void predicates_queue_add(PredicateNode node, PredicatesQueue queue);

PredicateNode predicates_queue_pop(PredicatesQueue queue);

void delete_pq(Pointer pq);

void print_pq(Pointer pq);