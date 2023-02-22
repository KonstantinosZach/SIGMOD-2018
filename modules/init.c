#include "init.h"

/* first col is 0 */
int get_col(MappedRelation map, int col, size_t* start, size_t* end){
    if((col > map->cols) || (col < 0)){
        printf("wrong number of col !!!\n");
        return -1;
    }
    size_t size_of_col = (map->size-2*sizeof(uint64_t))/ sizeof(uint64_t); /* size of table */
    size_of_col /= map->cols;   /* size of col */
    *start = size_of_col * col;
    *end = *start + size_of_col;

    return 0;
}

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

static void print_list(Pointer list){
    Node current_node = ((List)list)->head;
    Node next_node;
    
    while(current_node != NULL){
        MappedRelation map = (MappedRelation)current_node->data;
        
        size_t size = (map->size - 2*sizeof(uint64_t))/sizeof(uint64_t);
        uint64_t* data = (uint64_t*)map->addr;
        for (size_t i = 0; i < size; ++i)
            printf("data[%ld]->%ld \n", i, data[i]);

        next_node = current_node->next;
        current_node = next_node;
    }

    return;
}

void delete_MappedRelation(MappedRelation mappedRelation){
    char* to_unmap = mappedRelation->addr - sizeof(uint64_t)*2;
    munmap(to_unmap, mappedRelation->size);
    for(int i=0; i<mappedRelation->cols; i++)
        free(mappedRelation->statistics[i]);
    free(mappedRelation->statistics);
    free(mappedRelation);
    
    return;
}

MappedRelation create_mappedRelation(uint64_t size, int fd){
    char* addr = (char*)mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0u);
    MappedRelation mappedRelation = malloc(sizeof(*mappedRelation));
    
    mappedRelation->rows = *(uint64_t*)(addr);
    mappedRelation->cols = (uint64_t)(addr[sizeof(mappedRelation->rows)]); 
    mappedRelation->addr = addr + sizeof(uint64_t)*2; 
    mappedRelation->size = size;
    
    mappedRelation->statistics = malloc(sizeof(Stats)*mappedRelation->cols);
    calculate_statistics(mappedRelation);

    return mappedRelation;
}

int init(){ 
    char line[4096];
    List list = list_create(delete_list, print_list);

    fgets(line, 4096, stdin);
    while(strcmp(line, "Done\n") != 0){
        char* relation = malloc(sizeof(char) * (strlen(FOLDER) + strlen(line)));
        strcpy(relation, FOLDER);
        strncat(relation, line, strlen(line) - 1);

        int fd = open(relation, O_RDONLY);

        struct stat sb;
        if(fstat(fd, &sb) == -1)
            return -1;
        
        MappedRelation map = create_mappedRelation((uint64_t)sb.st_size, fd);
        add(map, list);
        
        close(fd);
        free(relation);

        fgets(line, 4096, stdin);
    }

    if(read_workload(list) == -1)
        exit(-1);
    
    delete_list(list);
    return 0;
}

IntermediateResultsFilter intermediateResultsFilter_create(int* array, int size, int cols, boolean create_stats){
    IntermediateResultsFilter intermediateResults = malloc(sizeof(*intermediateResults));
    intermediateResults->size_filter_array = size;
    intermediateResults->filter_array = array;
    intermediateResults->cols = cols;

    if(create_stats){
        intermediateResults->filtered = false;
        intermediateResults->stats = malloc(sizeof(Stats) * cols);
        for(int i=0; i<cols; i++){
            intermediateResults->stats[i] = malloc(sizeof(struct stats));
        }
    }
    
    return intermediateResults;
}

void intermediateResultsFilter_delete(IntermediateResultsFilter intermediateResults ,boolean del){
    if(del){
        for(int i=0; i<intermediateResults->cols; i++){
            free(intermediateResults->stats[i]);
        }
        free(intermediateResults->stats);
    }
    free(intermediateResults->filter_array);
    free(intermediateResults);
    return;
}

JoinIntermidiate joinIntermidiate_create(int cols, int rows, int number_of_relations, int** id_array){
    JoinIntermidiate joinIntermidiate = malloc(sizeof(*joinIntermidiate));

    joinIntermidiate->cols_id_array = cols;
    joinIntermidiate->rows_id_array = rows;
    joinIntermidiate->number_of_relations = number_of_relations;

    joinIntermidiate->pos = malloc(sizeof(int) * number_of_relations);

    for(int i=0; i<number_of_relations; i++)
        joinIntermidiate->pos[i] = -1;
    
    joinIntermidiate->id_array = id_array;

    return joinIntermidiate;
}

void joinIntermidiate_delete(JoinIntermidiate joinIntermidiate){
    for(int i=0; i<joinIntermidiate->rows_id_array; i++)
        free(joinIntermidiate->id_array[i]);   
    free(joinIntermidiate->id_array);
    free(joinIntermidiate->pos);
    free(joinIntermidiate);

    return;
}

JoinIntermidiateArray joinIntermidiateArray_create(int size_array, int size_pos){
    JoinIntermidiateArray joinArray = malloc(sizeof(*joinArray));

    joinArray->size_array = size_array;
    joinArray->size_pos = size_pos;
    joinArray->in_array = 0;
    joinArray->pos = malloc(sizeof(int) * size_pos);

    for(int i=0; i<size_pos; i++)
        joinArray->pos[i] = -1;

    joinArray->array = malloc(sizeof(*joinArray->array) * size_array);
    for(int i=0; i<size_array; i++)
        joinArray->array[i] = NULL;

    return joinArray;  
}

void joinIntermidiateArray_delete(JoinIntermidiateArray joinArray){
    free(joinArray->pos);
    for(int i=0; i<joinArray->size_array; i++)
        if(joinArray->array[i] != NULL)
            joinIntermidiate_delete(joinArray->array[i]);
    free(joinArray->array);

    free(joinArray);

    return;
}

void parse_predicates(PredicatesQueue pq, char* token){

    char* str1;
    char* predicates = malloc(strlen(token)+1);
    strcpy(predicates, token);
    char* token2 = strtok_r(predicates, "&", &str1);
    while(token2 != NULL){
        char* pos;
        char* str_predicate;
        char* left;
        char* predicate = malloc(strlen(token2)+1);
        strcpy(predicate, token2);
        char op;
        if((pos = strchr(token2, '=')) != NULL){
            op = '=';
            left = strtok_r(predicate, "=", &str_predicate);
        }
        else if((pos = strchr(token2, '<')) != NULL){
            op = '<';
            left = strtok_r(predicate, "<", &str_predicate);                            
        }
        else{
            pos = strchr(token2, '>');
            op = '>';
            left = strtok_r(predicate, ">", &str_predicate);
        }
        char* leftp = malloc(strlen(left)+1);
        strcpy(leftp, left);
        char* rightp = malloc(strlen(pos)+1);
        strcpy(rightp, pos+1);

        PredicateNode node = predicate_node_create(leftp, op, rightp);

        predicates_queue_add(node, pq);

        token2 = strtok_r(NULL, "&", &str1);
        free(predicate);
    }
    free(predicates);
    return;
}

void parse_relations_id(char* token, int* relation_num, int** array_value){
    int i=0;
    /* find number of occurences of ' ' in string */
    while(token[i] != '\0'){
        if(token[i] == ' ')
            (*relation_num)++;
        i++;
    }
    char* str;
    char* str_arrays = malloc(strlen(token)+1);
    (*relation_num)++;          /*total number of arrays*/
    *array_value = malloc(sizeof(int)*((*relation_num)));   /* 2 empty chars = 3 values */
    strcpy(str_arrays, token);
    char* token1 = strtok_r(str_arrays, " ", &str);
    int k=0;
    while(token1 != NULL){
        (*array_value)[k] = atoi(token1);
        k++;
        token1 = strtok_r(NULL, " ", &str);
    }
    /* save arrays on list */
    free(str_arrays);
    
}

void parse_line(PredicatesQueue pq, List viewslist, int** array_value, int* relation_num, int** views, char* line){
    int count = 0;  /* indicates part of workload line (splitted on "|") */
    char* token;
    /* first strtok on "|" */
    token = strtok(line, "|");
    while(token != NULL){
        /* first token represents the arrays */
        if(count == 0){
            parse_relations_id(token, relation_num, array_value);
        }
        else if(count == 1){
            parse_predicates(pq, token);
        }
        else{
            char* str2;
            char* views = malloc(strlen(token)+1);
            strcpy(views, token);
            char* token3 = strtok_r(views, " ", &str2);
            while(token3 != NULL){
                char* temp = malloc(strlen(token3)+1);
                strcpy(temp, token3);

                add(temp, viewslist);

                //char* dpos = strchr(temp, '.');
                //int col = atoi(dpos+1);
                //char* str4;
                //char* str_array = strtok_r(temp, ".", &str4);
                //int array = atoi(str_array);
                //printf("array -> %d and col %d \n", array, col);


                //free(temp);
                token3 = strtok_r(NULL, " ", &str2);
            }

            free(views);
        } 
        count++;
        token = strtok(NULL, "|");
    }
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

int read_workload(List relation_list){
    char line[4096];

    /* for every line */
    while(fgets(line, 4096, stdin) != NULL){
        //helps writing on console the input
        if(strcmp(line, "F\n") == 0){
            fflush(stdout);
            continue;
        }
        
        /* what to pass on function later */
        PredicatesQueue pq = predicates_queue_create(delete_pq, print_pq);
        List viewslist = list_create(delete_viewslist, NULL);

        int* array_value;
        int relation_num = 0;
        int* views;

        parse_line(pq, viewslist, &array_value, &relation_num, &views, line);
        traverse_predicates(relation_list, viewslist, array_value, relation_num, pq);
        printf("\n");

        pq->destroy_value(pq);
        free(array_value);
    }

    return 0;
}

void print_intermidiate(IntermediateResultsFilter* intermediate_results_array, int n_array){
    for(int i=0; i<n_array; i++){
        if(intermediate_results_array[i] != NULL){
            int* ar = intermediate_results_array[i]->filter_array;
            for(int j=0; j<intermediate_results_array[i]->size_filter_array; j++){
                printf("%d ", ar[j]);
            }
            printf("\n");
        }
    }
}

//traverse a single line of predicates
void traverse_predicates(List list_mapping, List viewsList, int* array_value, int n_array, PredicatesQueue queue){
    PredicateNode node;

    //filter intermediate result init
    IntermediateResultsFilter* filters= malloc(sizeof(IntermediateResultsFilter)*n_array);
    for(int i=0; i<n_array; i++){
        filters[i] = NULL;
    }

    JoinIntermidiateArray joinArray = joinIntermidiateArray_create(n_array/2, n_array);
    while((node = predicates_queue_pop(queue)) != NULL){
        if(node->filter){
            apply_filter(list_mapping, array_value, filters, node);
            
            update_filter_statistics(list_mapping, array_value, filters, node);
        }
        else{
          break;
           // fprintf(stderr, "%s = %s\n",node->left_part, node->right_part);
           // apply_join(list_mapping, array_value, filters, joinArray, node);
        }
        predicate_node_delete(node);
    }
    
    /* calculate joins */
    /* ADD POPPED NODE */
    predicates_queue_add(node, queue);
    int num_joins = queue->size;
    PredicatesQueue ordered_queue = predicates_queue_create(delete_pq, print_pq);
    find_join_order(array_value, n_array, queue, num_joins, ordered_queue, list_mapping, filters);
    while((node = predicates_queue_pop(ordered_queue)) != NULL){
       // fprintf(stderr, "%s = %s\n",node->left_part, node->right_part);
        apply_join(list_mapping, array_value, filters, joinArray, node);
        predicate_node_delete(node);
    }

    ordered_queue->destroy_value(ordered_queue);

    //here we traverse the list of views to take the sums
    Node current_node = viewsList->head;
    Node next_node;
    while(current_node != NULL){
        uint64_t sum;
        apply_sum(filters, list_mapping, array_value, joinArray, current_node, &sum);
        next_node = current_node->next;
        current_node = next_node;

        if(current_node != NULL)
            fprintf(stdout, " ");
    }
    //delete intermidate results
    for(int i=0; i<n_array; i++){
        if(filters[i] != NULL){
            intermediateResultsFilter_delete(filters[i], true);
        }
    }
    free(filters);

    viewsList->destroy_value(viewsList);
    joinIntermidiateArray_delete(joinArray);
    return;
}

/*
static void delete_inside_list(Pointer list){
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

void update_join_list(PredicateNode node, List list_joined){
    int left_array, right_array;
    int left_col, right_col; 
    split_part(node->left_part, &left_array, &left_col);
    split_part(node->right_part, &right_array, &right_col);

    char* left_part = malloc(strlen(node->left_part)+1);
    strcpy(left_part, node->left_part);
    char* right_part = malloc(strlen(node->right_part)+1);
    strcpy(right_part, node->right_part);
    
    // first case 
    if(list_joined->size == 0){
        //add both arrays
        List inside_list = list_create(delete_inside_list,NULL);
        add(left_part, inside_list);  // 1.0
        add(right_part, inside_list); // 0.1
        add(inside_list, list_joined);
    }
    else{
        Node current_node = list_joined->head;
        while(current_node != NULL){
            List inside_list = (List)current_node->data;
            boolean create_new = true;
            for(Node i=inside_list->head; i!=NULL; i=i->next){
                //fprintf(stderr, "next -> %p\n", );
                int data_array, data_col;
                split_part(i->data, &data_array, &data_col);    
                
                // if array exists in list the it is connected to a join so we add on same list
                if(data_array == left_array){
                    add(right_part, inside_list);
                    free(left_part);
                    create_new = false;
                    break;
                }
                else if(data_array == right_array){
                    add(left_part, inside_list);
                    free(right_part);
                    create_new = false;
                    break;
                }
            }
            
            // if we havent found array in list then we create a new list
            if(create_new){
                List inside_list = list_create(delete_inside_list,NULL);
                add(left_part, inside_list);
                add(right_part, inside_list);
                add(inside_list, list_joined);
                break;
            }
            current_node = current_node->next;
        }
    }
}

*/
