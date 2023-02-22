#pragma once

#include <stdint.h> /*uint64_t*/
#include <math.h>   /*pow(), log2()*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h> /*memcpy*/

#define boolean int
#define true 1
#define false 0
#define MAX 1844674407370955161

struct tuple
{
    int id;
    uint64_t value;
};

typedef struct tuple* Tuple;

struct histogram
{
    Tuple* array;
    int size;
};

typedef struct histogram* Histogram;

struct relation
{
    Tuple* array;
    int num_tuples;
    int n_bits;  //bits
};

typedef struct relation* Relation;

struct pass
{
    Relation relation;
    Histogram psum;
};

typedef struct pass* Pass;

struct partition
{
    Pass basic_pass;
    Pass second_pass;
};

typedef struct partition* Partition;

typedef void* Pointer;

typedef void (*DestroyFunc)(Pointer value);
typedef void (*PrintFunc)(Pointer value);

typedef struct node* Node;

struct node{
    Node next;
    Pointer data;
};

struct list{
    Node head;
    Node last;
    int size;
    DestroyFunc destroy_value;
    PrintFunc print_value;
};

typedef struct list* List;

List list_create(DestroyFunc destroy_value, PrintFunc print_value);

void add(Pointer data, List list);

Node pop(List list);

struct hashCell{
    boolean* bitmap;
    Tuple tuple;    // first tuple 
    List chain;     // list for same values as tuple
    boolean initialized;
};

typedef struct hashCell* HashCell;

struct hashTable{
    int n;      //n_bits from corresponding partition
    int size;   //size of hash_tabke
    int h;      //size of neighboor
    HashCell* hash_table;
};

typedef struct hashTable* HashTable;

enum cases{case_a, case_b, case_c};

struct build{
    Partition partition_hash;
    Partition partition_search;
    HashTable* hash_table;
    enum cases build_case;
    enum cases search_case;
};

typedef struct build* Build;

struct match{
    List hits;
    Tuple source;
};

typedef struct match* Match;

struct join{
    int** array;
    int row_size;
    int col_size;
};

typedef struct join* Join;

//Part 2

/* Statistics */
struct stats{
    /* lowest value of col */
    uint64_t l;
    /* highest value of col */
    uint64_t u;
    /* number of data */
    uint64_t f;
    /* number of distinct values */
    uint64_t d;
};

typedef struct stats* Stats;

struct arrayStats{
    /* array of which we keep the stats */
    int array;
    int cols;
    /* stats of array */
    Stats* stats;
};

typedef struct arrayStats* ArrayStats;

typedef struct joinNode* JoinNode;

struct connected{
    boolean is_connected;
    boolean added;         /*if node added in ordered_queue but has others in list that haven't*/
    int left_col;
    int right_col;
    char* join_left;
    char* join_right;
    int has_list;       /*if number of joins on the same arrays is > 1 has the number of joins left in list*/
    List list;
};

typedef struct connected* Connected;

struct joinStats{
    /* minimum cost of subset */
    uint64_t cost;
    int size_of_subset;
    /* array of relations in join size of subset - S in example */ 
    int* S; 
    /* array of statistics of arrays in subset for every relation we have its statistics(all cols) */
    ArrayStats* array_stats; 
};

typedef struct joinStats* JoinStats;

struct mappedRelation{
    /* number of rows */
    uint64_t rows;
    /* number of cols */
    uint64_t cols;
    uint64_t size;
    /* address of first element */
    char* addr;
    /* a struct array of col size */
    Stats* statistics;
};

typedef struct mappedRelation* MappedRelation;

struct intermediateResultsFilter{
    int* filter_array;                   //filter
    int size_filter_array;              //size of filters
    Stats* stats;                       //array of stats where each cell represents a column.
    int cols;
    boolean filtered;                   //true if array is already filtered once
};

typedef struct intermediateResultsFilter* IntermediateResultsFilter;

struct joinIntermidiate{
    int cols_id_array;           //number of relations inside Intermidiate
    int rows_id_array;           //the number of the ids
    int* pos;                    //the position in the id_array of each relation (where 0 relation is pos[0])
    int number_of_relations;     //numbers of relations genarally
    int** id_array;              //array with the row ids of the relations
};

typedef struct joinIntermidiate* JoinIntermidiate;

struct joinIntermidiateArray{
    int size_array;             //where size is always number of relations / 2
    int size_pos;               //number of relations
    int* pos;                   //the position in the array of indermidiates for each relation
    int in_array;               // number of intermediates in array
    JoinIntermidiate* array;     //the array that holds all the indermidiates
};

typedef struct joinIntermidiateArray* JoinIntermidiateArray;