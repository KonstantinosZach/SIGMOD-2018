#include "acutest.h"
#include "join_statistics.h"

static void delete_list(Pointer list){
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

void create_delete_cost_array_test(void){
	
	/* create cost array of size 2 */
	JoinStats* test_cost_array = NULL; 
	test_cost_array = create_cost_array(2);

	TEST_CHECK(test_cost_array != NULL);
	for(int i=0 ; i<2; i++){
		TEST_CHECK(test_cost_array[i] != NULL);
		TEST_CHECK(test_cost_array[i]->cost == MAX);
		TEST_CHECK(test_cost_array[i]->S == NULL);
		TEST_CHECK(test_cost_array[i]->array_stats == NULL);
	}
	
	delete_cost_array(test_cost_array, 2);
}

void create_connected_relations_test(void){

	Connected** test_array = NULL;
	test_array = create_connected_relations(2);		/* create 2 x 2 array */

	for(int i=0; i<2; i++){
		for(int j=0; j<2; j++){
			TEST_CHECK(test_array[i][j] != NULL);
			TEST_CHECK(sizeof(*test_array[i][j]) == sizeof(struct connected));
			TEST_CHECK(test_array[i][j]->has_list == 0);
			TEST_CHECK(test_array[i][j]->added == false);
			TEST_CHECK(test_array[i][j]->is_connected == false);
			TEST_CHECK(test_array[i][j]->left_col == -1);
			TEST_CHECK(test_array[i][j]->right_col == -1);
		}
	}

	delete_connected_relations(test_array, 2);
}

void init_connected_relations_test(void){
	Connected** test_array = create_connected_relations(2);
	
	PredicatesQueue queue = predicates_queue_create(delete_pq, NULL);
	char* left = malloc(sizeof(char) * 4);
	strcpy(left, "0.1");
	char* right = malloc(sizeof(char) * 4);
	strcpy(right, "1.1");
	PredicateNode node = predicate_node_create(left, '=', right);
	predicates_queue_add(node, queue);
	test_array = init_connected_relations(queue, 1, test_array);

	TEST_CHECK(test_array[1][0]->is_connected == true);
	TEST_CHECK(test_array[0][1]->is_connected == true);
	TEST_CHECK(test_array[0][0]->is_connected == false);
	TEST_CHECK(test_array[1][1]->is_connected == false);

	TEST_CHECK(test_array[1][0]->list != NULL);
	TEST_CHECK(test_array[0][1]->list != NULL);

	delete_connected_relations(test_array, 2);
	queue->destroy_value(queue);
}

void copy_joinStats_test(void){
	JoinStats source = malloc(sizeof(struct joinStats));
	JoinStats target = malloc(sizeof(struct joinStats));

	source->array_stats = malloc(sizeof(ArrayStats));
	source->array_stats[0] = malloc(sizeof(struct arrayStats));
	source->array_stats[0]->stats = malloc(sizeof(Stats));
	source->array_stats[0]->stats[0] = malloc(sizeof(struct stats));
	source->array_stats[0]->stats[0]->d = 1;
	source->array_stats[0]->stats[0]->l = 2;
	source->array_stats[0]->stats[0]->u = 3;
	source->array_stats[0]->stats[0]->f = 4;
	source->array_stats[0]->array = 0;
	source->array_stats[0]->cols = 1;

	source->S = malloc(sizeof(int));
	source->S[0] = 0;
	source->cost = 10;
	source->size_of_subset = 1;

	copy_joinStats(target, source);

	TEST_CHECK(target->S[0] == 0);
	TEST_CHECK(target->cost == 10);
	TEST_CHECK(target->size_of_subset == 1);
	TEST_CHECK(target->array_stats[0]->array == 0);
	TEST_CHECK(target->array_stats[0]->cols == 1);
	TEST_CHECK(target->array_stats[0]->stats[0]->d  == 1);
	TEST_CHECK(target->array_stats[0]->stats[0]->l  == 2);
	TEST_CHECK(target->array_stats[0]->stats[0]->u  == 3);
	TEST_CHECK(target->array_stats[0]->stats[0]->f  == 4);

	delete_joinStats(target);
	delete_joinStats(source);
}

void resize_R_test(void){
	
	JoinStats source = malloc(sizeof(struct joinStats));

	source->array_stats = malloc(sizeof(ArrayStats));
	source->array_stats[0] = malloc(sizeof(struct arrayStats));
	source->array_stats[0]->stats = malloc(sizeof(Stats));
	source->array_stats[0]->stats[0] = malloc(sizeof(struct stats));
	source->array_stats[0]->stats[0]->d = 1;
	source->array_stats[0]->stats[0]->l = 2;
	source->array_stats[0]->stats[0]->u = 3;
	source->array_stats[0]->stats[0]->f = 4;
	source->array_stats[0]->array = 0;
	source->array_stats[0]->cols = 1;

	source->S = malloc(sizeof(int)*2);
	source->S[0] = 0;
	source->S[1] = 1;
	source->cost = 10;
	source->size_of_subset = 2;

	JoinStats* cost = malloc(sizeof(JoinStats)*2);
	cost[1] = source;
	cost[1]->size_of_subset = 2;

	int* R = malloc(sizeof(int)*3);
	for(int i=0; i<3; i++){
		R[i] = i;
	}

	int r_size = 3;

	R = resize_R(1, R, &r_size, 3, cost, 0);

	TEST_CHECK(r_size == 1);
	TEST_CHECK(R[0] == 2);
	
	free(R);
	free(source->S);
	free(source->array_stats[0]->stats[0]);
	free(source->array_stats[0]->stats);
	free(source->array_stats[0]);
	free(source->array_stats);
	free(source);
	free(cost);
}

void calculate_join_stats_test(void){
	int fd = open("../input/workloads/small/r0", O_RDONLY);
	int cols[2];
	MappedRelation maps[2];

    struct stat sb;
    fstat(fd, &sb);

    maps[0] = create_mappedRelation((uint64_t)sb.st_size, fd);
	cols[0] = maps[0]->cols;

	fd = open("../input/workloads/small/r1", O_RDONLY);
    fstat(fd, &sb);

    maps[1] = create_mappedRelation((uint64_t)sb.st_size, fd);
	cols[1]= maps[1]->cols;
	
	int* array_value = malloc(sizeof(int) * 2);
	array_value[0] = 0;
	array_value[1] = 1;

	JoinStats* cost = create_cost_array(1);
	cost[0]->S = malloc(sizeof(int) * 2);
	cost[0]->S[0] = 0;
	cost[0]->S[1] = 1;

	cost[0]->cost = MAX;
	cost[0]->array_stats = malloc(sizeof(ArrayStats) * 2);
	for(int i=0; i<2; i++){
		cost[0]->array_stats[i]= malloc(sizeof(struct arrayStats)*2);
		cost[0]->array_stats[i]->array = i;
		cost[0]->array_stats[i]->cols = cols[i];
		cost[0]->array_stats[i]->stats = malloc(sizeof(Stats)* cols[i]);
		for(int j = 0; j<cols[i]; j++){
			cost[0]->array_stats[i]->stats[j] = malloc(sizeof(struct stats));
			cost[0]->array_stats[i]->stats[j]->d = maps[i]->statistics[j]->d;
			cost[0]->array_stats[i]->stats[j]->f = maps[i]->statistics[j]->f;
			cost[0]->array_stats[i]->stats[j]->u = maps[i]->statistics[j]->u;
			cost[0]->array_stats[i]->stats[j]->l = maps[i]->statistics[j]->l;
		}
	}

	List mappings = list_create(delete_list, NULL);
	add(maps[0], mappings);
	add(maps[1], mappings);

	JoinStats join_stats = malloc(sizeof(struct joinStats));
	join_stats = calclulate_join_stats(cost, 0, 1, join_stats,1,mappings, array_value, 0,0);

	TEST_CHECK(join_stats->cost	== 506);

	free(cost[0]->S);
	for(int i=0; i<2; i++){
		delete_stats_array(cost[0]->array_stats[i]->stats, cols[i]);
		free(cost[0]->array_stats[i]);
	}
	free(cost[0]->array_stats);
	free(cost[0]);
	delete_joinStats(join_stats);

	free(cost);
	free(array_value);
	mappings->destroy_value(mappings);
}


TEST_LIST = {
	{ "create_delete_cost_array_test", create_delete_cost_array_test },
	{ "create_connecter_relations_test", create_connected_relations_test },
	{ "init_connected_relations_test", init_connected_relations_test },
	{ "copy_joinStats_test", copy_joinStats_test },
	{ "resize_R_test", resize_R_test},
	{ "calculate_join_stats_test", calculate_join_stats_test},
	{ NULL, NULL } // τερματίζουμε τη λίστα με NULL
};