#include "probing.h"

Match create_match(Tuple source, List hits){
    Match match = malloc(sizeof(*match));
    match->hits = hits;
    match->source = source;

    return match;
}

void delete_match(Match match){
    match->hits->destroy_value(match->hits);
    free(match);
}

static void delete_list(Pointer list){

    Node current_node = ((List)list)->head;
    Node next_node;

    while(current_node != NULL){
        Match match = (Match)current_node->data;
        delete_match(match);

        next_node = current_node->next;
        free(current_node);
        current_node = next_node;
    }

    free(list);
}

static void delete_threadslist(Pointer list){
    
    Node current_node = ((List)list)->head;
    Node next_node;

    while(current_node != NULL){
        next_node = current_node->next;
        free(current_node);
        current_node = next_node;
    }

    free(list);
}

// void print_list(Pointer list){

//     Node current_node = ((List)list)->head;
//     Node next_node;
    
//     while(current_node != NULL){
//         Match match = (Match)current_node->data;

//         printf("\nSource id:%d value:%ld\n", match->source->id, match->source->value);
//         for(int i=0; i<match->num; i++){
//             printf("id: %d value: %ld\n", match->hits[i]->id, match->hits[i]->value);
//         }

//         next_node = current_node->next;
//         current_node = next_node;
//     }        
// }

//adds on list the matches from find
void search_tuple(Tuple tuple, Build build, List list, int* final_rows){
    //get n from the other partition
    int n;
    int hash_value;
    List hits;
    Match match;
    //case a we have only one hash table
    if(build->build_case == case_a){
        hash_value = 0;
    }
    else if(build->build_case == case_b){
        n = build->partition_hash->basic_pass->relation->n_bits;
        hash_value = hash(n, tuple->value);
    } 
    else{
        n = build->partition_hash->second_pass->relation->n_bits;
        hash_value = hash(n, tuple->value);
    }
    hits = find(build->hash_table[hash_value], tuple->value);
    *final_rows += hits->size;
    if(hits->size > 0){
        match = create_match(tuple, hits);
        add(match, list);      
    }
    else{
        hits->destroy_value(hits);
    }
}

struct join_args{
    int start;
    int end;
    int* final_rows;
    Pass pass;
    Build build;
    List list;
};
typedef struct join_args* Join_args;

Join_args join_args_create(int start, int end, Pass pass, Build build){
    Join_args args = malloc(sizeof(*args));
    args->list = list_create(delete_threadslist, NULL);

    args->final_rows = malloc(sizeof(int));
    *args->final_rows = 0;

    args->start = start;
    args->end = end;
    args->pass = pass;
    args->build = build;

    return args;
}

void join_args_delete(Join_args args){
    args->list->destroy_value(args->list);
    free(args->final_rows);
    free(args);
}

int join_task(Pointer value){
    Join_args args = (Pointer)value;

    for(int i=args->start; i<args->end; i++){
        Tuple tuple = args->pass->relation->array[i];
        search_tuple(tuple, args->build, args->list, args->final_rows);
    }

    return 0;
}

List probing(Build build, int* final_rows){
    Pass pass;
    if(build->search_case == case_b || build->search_case == case_a)
        pass =  build->partition_search->basic_pass;
    else
        pass =  build->partition_search->second_pass;

    //we can do the probing in parallel
    if(build->search_case != case_a){
        int psum_size = pass->psum->size;
        
        //we will create 2^n tasks so we will need psum_size args
        Join_args* args_arr = malloc(sizeof(Join_args*) * psum_size);

        for(int psum_pos=0; psum_pos<psum_size; psum_pos++){
            int start = pass->psum->array[psum_pos]->value;
            int end;

            //start and end for each bucket
            if(psum_pos == psum_size-1)
                end = pass->relation->num_tuples;
            else
                end = pass->psum->array[psum_pos+1]->value;

            args_arr[psum_pos] = join_args_create(start, end, pass, build);
            serve_task(join_task, (Pointer)args_arr[psum_pos]);
        }
        //wait for all the threads to do the probing
        barrier();

        List list = list_create(delete_list, NULL);
        *final_rows = 0;
        
        //for each thread we iterate the list with the matches and we make the final list
        for(int psum_pos=0; psum_pos<psum_size; psum_pos++){
            for(Node node=args_arr[psum_pos]->list->head; node != NULL; node = node->next)
                add(node->data, list);
            *final_rows += *args_arr[psum_pos]->final_rows;
            join_args_delete(args_arr[psum_pos]);
        }
        free(args_arr);
        return list;    
    }
    //case a does not need parralism
    else{
        List list = list_create(delete_list, NULL); /* list of lists */
        Tuple tuple;
        *final_rows = 0;

        for(int i=0; i < pass->relation->num_tuples; i++){
            tuple = pass->relation->array[i];
            search_tuple(tuple, build, list, final_rows);
        }
        return list;
    }
}


/*returs an alloced array of size [size][2]*/
int** final_id_array(List list, int size){
    int** final_array = malloc(sizeof(int*) * size);
    
    Node current_node = ((List)list)->head;
    Node next_node;
    int counter = 0;

    while(current_node != NULL){
        Match match = (Match)current_node->data;

        Node current_node_2 = match->hits->head;
       
        while(current_node_2 != NULL){
            Tuple tuple = (Tuple)current_node_2->data;

            final_array[counter] = malloc(sizeof(int) * 2);
            final_array[counter][1] = match->source->id;
            final_array[counter][0] = tuple->id;

            counter++;

            current_node_2 = current_node_2->next;
        }
            
        next_node = current_node->next;
        current_node = next_node;
    } 
    return final_array;
}

void delete_final_id_array(int** id_array, int size){
    for(int i=0; i<size; i++)
        free(id_array[i]);
    free(id_array);
}
