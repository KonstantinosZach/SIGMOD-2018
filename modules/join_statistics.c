#include"join_statistics.h"

void connected_node_delete(Connected node){
    free(node->join_left);
    free(node->join_right);

    free(node);

    return;
}

void delete_connected_list(Pointer list){

    Node current_node = ((List)list)->head;
    Node next_node;

    Connected con_node;
    while(current_node != NULL){
        con_node = current_node->data;
        connected_node_delete(con_node);
        next_node = current_node->next;

        free(current_node);
        current_node = next_node;
    } 
    free(list);

    return;
}

/* Initialize cost array that holds optimal cost of joins for each subset size*/
JoinStats* create_cost_array(int size){
    JoinStats* cost = malloc(sizeof(JoinStats) * size);
    for(int i=0; i<size; i++){
        cost[i] = malloc(sizeof(struct joinStats));
        cost[i]->cost = MAX;
        /* here we keep all the arrays that contribute in the join */
        /* for the first iteration its all the arrays */
        cost[i]->S = NULL;
        cost[i]->array_stats = NULL;
    }
    return cost;
}

void delete_stats_array(Stats* stats, int cols){
    for(int i=0; i<cols; i++){
        free(stats[i]);
    }
    free(stats);
    return;
}

void delete_cost_array(JoinStats* cost, int size){
    for(int i=0; i<size; i++){
        if(cost[i]->array_stats != NULL){
            for(int j=0; j<cost[i]->size_of_subset; j++){
                if(cost[i]->array_stats[j] != NULL){
                    delete_stats_array(cost[i]->array_stats[j]->stats, cost[i]->array_stats[j]->cols); 
                    free(cost[i]->array_stats[j]);
                }
            }
        free(cost[i]->array_stats);
        }
        
        if(cost[i]->S != NULL)
            free(cost[i]->S);

        free(cost[i]);
    }
    free(cost);
    return;
}

/* Initialize cost[0] which is every array from query with its corresponding statistics */
JoinStats* init_cost(JoinStats* cost, int n_array, List list_array, int* array_value, IntermediateResultsFilter* filters){
    MappedRelation map;
    int cols;

    /* only in first iteration S (subsets of size 1) is every array */
    cost[0]->size_of_subset = n_array;
    cost[0]->S = malloc(sizeof(int) * n_array);
    for(int i=0; i<n_array; i++){
        cost[0]->S[i] = i;          /* array_value[i] */
    }

    /* store stats of each array */
    cost[0]->array_stats = malloc(sizeof(ArrayStats)*n_array);
    for(int i=0; i<n_array; i++){
        cost[0]->array_stats[i] = malloc(sizeof(struct arrayStats));

        /* get map of each array */
        map = get_map(list_array, array_value, i);
        cols = map->cols;
        cost[0]->array_stats[i]->array = i;
        cost[0]->array_stats[i]->stats = malloc(sizeof(Stats) * cols);
        cost[0]->array_stats[i]->cols = cols;
        /* check if this array has filtered or map stats and copy stats */
        if(filters[i] != NULL){
            for(int j=0; j<cols; j++){
                cost[0]->array_stats[i]->stats[j] = malloc(sizeof(struct stats));
                cost[0]->array_stats[i]->stats[j]->d = filters[i]->stats[j]->d;
                cost[0]->array_stats[i]->stats[j]->f = filters[i]->stats[j]->f;
                cost[0]->array_stats[i]->stats[j]->u = filters[i]->stats[j]->u;
                cost[0]->array_stats[i]->stats[j]->l = filters[i]->stats[j]->l;
            }
        }
        else{
            for(int j=0; j<cols; j++){
                cost[0]->array_stats[i]->stats[j] = malloc(sizeof(struct stats));
                cost[0]->array_stats[i]->stats[j]->d = map->statistics[j]->d;
                cost[0]->array_stats[i]->stats[j]->f = map->statistics[j]->f;
                cost[0]->array_stats[i]->stats[j]->u = map->statistics[j]->u;
                cost[0]->array_stats[i]->stats[j]->l = map->statistics[j]->l;
            }
        }
    }
    return cost;
}

/* Initializes Connected 2D array to store which arrays are connected via join*/
Connected** create_connected_relations(int n_array){
    Connected** connected_relations = malloc(sizeof(Connected*)*n_array);
    for(int i=0; i<n_array; i++){
        connected_relations[i] = malloc(n_array * sizeof(Connected));
        for(int j=0; j<n_array; j++){
            connected_relations[i][j] = malloc(sizeof(struct connected));
            connected_relations[i][j]->has_list = 0;
            connected_relations[i][j]->is_connected = false;
            connected_relations[i][j]->added = false;
            connected_relations[i][j]->left_col = -1;
            connected_relations[i][j]->right_col = -1;
        }
    }
    return connected_relations;
}

void delete_connected_relations(Connected** connected_relations, int n_array){
    for(int i=0; i<n_array; i++){
        for(int j=0; j<n_array; j++){
            if(connected_relations[i][j]->is_connected){
                free(connected_relations[i][j]->join_left);
                free(connected_relations[i][j]->join_right);                
                connected_relations[i][j]->list->destroy_value(connected_relations[i][j]->list);
            }
            free(connected_relations[i][j]);
        }
        free(connected_relations[i]);
    }
    free(connected_relations);
    return;
}

Connected** init_connected_relations(PredicatesQueue queue, int num_joins, Connected** connected_relations){
    PredicateNode join_predicate;
    int left_array, right_array, left_col, right_col;
    for(int i=0; i<num_joins; i++){
        /* Get the join out of queue */
        join_predicate = predicates_queue_pop(queue);

        /* Get left array of join predicate */
        split_part(join_predicate->left_part, &left_array, &left_col);
        split_part(join_predicate->right_part, &right_array, &right_col);
        
        /* 
            1.0=2.3 
            left_array = 1, left_col = 0
            right_array = 2, right_col = 3
        */
        if(connected_relations[left_array][right_array]->is_connected == false){
            //fprintf(stderr, "list created!\n");
            connected_relations[left_array][right_array]->list = list_create(delete_connected_list, NULL);
            connected_relations[left_array][right_array]->is_connected = true;
            connected_relations[left_array][right_array]->added = false;

            connected_relations[left_array][right_array]->left_col = left_col;
            connected_relations[left_array][right_array]->right_col = right_col;
            
            connected_relations[left_array][right_array]->join_left = malloc(sizeof(char)*strlen(join_predicate->left_part) +1);
            strcpy(connected_relations[left_array][right_array]->join_left, join_predicate->left_part);
            connected_relations[left_array][right_array]->join_right = malloc(sizeof(char)*strlen(join_predicate->right_part)+1);
            strcpy(connected_relations[left_array][right_array]->join_right, join_predicate->right_part);
        }else{
            connected_relations[left_array][right_array]->has_list++;           //list size remaining to be checked.
            //there is more than 1 join on same arrays.
            Connected new = malloc(sizeof(struct connected));

            new->join_left = malloc(sizeof(char)*strlen(join_predicate->left_part) +1);
            strcpy(new->join_left, join_predicate->left_part);
            new->join_right = malloc(sizeof(char) * strlen(join_predicate->right_part)+1);
            strcpy(new->join_right, join_predicate->right_part);
            new->has_list = 0;    
            new->is_connected = true;         
            new->added = false;         // this value shows that list node has not been checked and added yet.
            new->list = NULL;
            new->left_col = left_col;
            new->right_col = right_col;

            add(new, connected_relations[left_array][right_array]->list);
        }
        if(connected_relations[right_array][left_array]->is_connected == false){
            connected_relations[right_array][left_array]->list = list_create(delete_connected_list, NULL);
            connected_relations[right_array][left_array]->is_connected = true;
            connected_relations[left_array][right_array]->added = false;
            connected_relations[right_array][left_array]->left_col = right_col;
            connected_relations[right_array][left_array]->right_col = left_col;
            connected_relations[right_array][left_array]->join_left = malloc(sizeof(char)*strlen(join_predicate->left_part) +1);
            strcpy(connected_relations[right_array][left_array]->join_left, join_predicate->left_part);
            connected_relations[right_array][left_array]->join_right = malloc(sizeof(char)*strlen(join_predicate->right_part)+1);
            strcpy(connected_relations[right_array][left_array]->join_right, join_predicate->right_part);
        }else{
            connected_relations[right_array][left_array]->has_list++;           //list size remaining to be checked.
            //there is more than 1 join on same arrays.
            Connected new = malloc(sizeof(struct connected));

            new->join_left = malloc(sizeof(char)*strlen(join_predicate->left_part) +1);
            strcpy(new->join_left, join_predicate->left_part);
            new->join_right = malloc(sizeof(char) * strlen(join_predicate->right_part)+1);
            strcpy(new->join_right, join_predicate->right_part);
            new->has_list = 0;    
            new->is_connected = true;         
            new->added = false;         // this value shows that list node has not been checked and added yet.
            new->list = NULL;
            new->left_col = right_col;
            new->right_col = left_col;

            add(new, connected_relations[right_array][left_array]->list);
        }
        predicate_node_delete(join_predicate);
    }
    return connected_relations;
}

void copy_joinStats(JoinStats target, JoinStats source){
    target->S = malloc(sizeof(int) * source->size_of_subset);
    target->size_of_subset = source->size_of_subset;
    target->array_stats = malloc(sizeof(ArrayStats)* source->size_of_subset);
    target->cost = source->cost;
    for(int j=0; j<source->size_of_subset; j++){
        target->S[j] = source->S[j];
        target->array_stats[j] = malloc(sizeof(struct arrayStats));
        target->array_stats[j]->cols = source->array_stats[j]->cols;
        target->array_stats[j]->array = source->array_stats[j]->array;
        target->array_stats[j]->stats = malloc(sizeof(Stats)*source->array_stats[j]->cols);
        for(int k=0; k<source->array_stats[j]->cols; k++){
            target->array_stats[j]->stats[k] = malloc(sizeof(struct stats));
            target->array_stats[j]->stats[k]->d = source->array_stats[j]->stats[k]->d;
            target->array_stats[j]->stats[k]->l = source->array_stats[j]->stats[k]->l;
            target->array_stats[j]->stats[k]->u = source->array_stats[j]->stats[k]->u;
            target->array_stats[j]->stats[k]->f = source->array_stats[j]->stats[k]->f;
        }
    }
    return;
}

void delete_joinStats(JoinStats target){
    for(int k=0; k<target->size_of_subset; k++){
        delete_stats_array(target->array_stats[k]->stats, target->array_stats[k]->cols);
        free(target->array_stats[k]);
    }
    free(target->S);
    free(target->array_stats);
    free(target);
}

int* resize_R(int i, int* R, int* r_size, int n_array, JoinStats* cost, int in_list){
    
    int count=0;
    boolean inS = false;
    if(in_list == 0){
        if(i == 1){
            R = realloc(R, sizeof(int)*n_array - 2);
            *r_size = n_array - 2;
        }
        else{
            R = realloc(R, sizeof(int)*n_array - i);
            *r_size = *r_size - 1;
        }
        for(int j=0; j<n_array; j++){
            for(int k=0; k<cost[i]->size_of_subset; k++){
                if(cost[i]->S[k] == j){
                    inS = true;
                }
            }
            if(!inS){
                R[count] = j;
                count++;
            }
            inS = false;
        }
    }
    return R;
}

void find_join_order(int* array_value, int n_array, PredicatesQueue queue, int num_joins, PredicatesQueue ordered_queue, List list_array, IntermediateResultsFilter* filters){
    JoinStats* cost = create_cost_array(num_joins+1);
    /* Initialize cost[0] which is every array from query with its corresponding statistics */
    cost = init_cost(cost, n_array, list_array, array_value, filters);

    Connected** connected_relations = create_connected_relations(n_array);
    connected_relations = init_connected_relations(queue, num_joins, connected_relations);
    /* JOIN ENUMERATION ALGORITHM */
    int* R = malloc(sizeof(int)* n_array);
    int r_size = n_array;
    for(int i=0; i<n_array; i++)
        R[i] = i;
    int pos_in_list = 0;
    for(int i=1; i<=num_joins; i++){
        JoinStats min = malloc(sizeof(struct joinStats));
        min->array_stats = NULL;
        min->cost = MAX;
        min->S = NULL;
        min->size_of_subset = i+1;
        PredicateNode pn = NULL;
        int in_list = 0;
        for(int s=0; s<cost[i-1]->size_of_subset; s++){
            /* check for every singular relation */
            for(int r=0; r<r_size; r++){
                /* check if S = R or if S and R are connected in a join together */
                if(i==1 && cost[i-1]->S[s] > R[r])
                    continue;
                if(cost[i-1]->S[s] == R[r])
                    continue;
                if(!connected_relations[cost[i-1]->S[s]][R[r]]->is_connected)
                    continue;
                int times = 1;
                if(connected_relations[cost[i-1]->S[s]][R[r]]->has_list > 0){
                    times += connected_relations[cost[i-1]->S[s]][R[r]]->list->size;
                }
                for(int t=0; t<times; t++){
                    JoinStats current = malloc(sizeof(struct joinStats));            
                    /** Calculate Stats of join between S and R **/
                    int scol, rcol;
                    char* left, *right;
                    if(t==0){
                        in_list = false;
                        if(connected_relations[cost[i-1]->S[s]][R[r]]->added){
                            free(current);
                            continue;
                        }
                        scol = connected_relations[cost[i-1]->S[s]][R[r]]->left_col;
                        rcol = connected_relations[cost[i-1]->S[s]][R[r]]->right_col;
                        left = malloc(sizeof(char)* strlen(connected_relations[cost[i-1]->S[s]][R[r]]->join_left) + 1);
                        strcpy(left, connected_relations[cost[i-1]->S[s]][R[r]]->join_left);
                        right = malloc(sizeof(char)*strlen(connected_relations[cost[i-1]->S[s]][R[r]]->join_right)+1);
                        strcpy(right, connected_relations[cost[i-1]->S[s]][R[r]]->join_right);
                    }else{
                        in_list = t;
                        Node node = connected_relations[cost[i-1]->S[s]][R[r]]->list->head;
                        for(int node_number=0; node_number<t-1; node_number++){
                            node = node->next;
                        }
                        Connected con = (Connected)node->data;
                        if(con->added == true){          //if node has already been added.
                            free(current);
                            continue;
                        }
                        scol = con->left_col;
                        rcol = con->right_col;
                        left = malloc(sizeof(char)* strlen(connected_relations[cost[i-1]->S[s]][R[r]]->join_left)+1);
                        strcpy(left, con->join_left);
                        right = malloc(sizeof(char)*strlen(connected_relations[cost[i-1]->S[s]][R[r]]->join_right)+1);
                        strcpy(right, con->join_right);
                    }
                    current = calclulate_join_stats(cost, cost[i-1]->S[s], R[r], current, i, list_array, array_value, scol, rcol);    /* need to malloc current->array_stats */
                    if(current->cost < min->cost){
                        if(in_list){
                            pos_in_list = t;
                        }else{
                            pos_in_list = 0;
                        }
                        if(min->cost != MAX){
                            delete_joinStats(min);
                            min = malloc(sizeof(struct joinStats));
                        }
                        if(pn != NULL)
                            predicate_node_delete(pn);
                        min->size_of_subset = current->size_of_subset;
                        copy_joinStats(min, current);
                        
                        pn = predicate_node_create(left, '=', right);
                    }else{
                        free(left);
                        free(right);
                    }
                    delete_joinStats(current);
               }
            }
        }

        copy_joinStats(cost[i], min);
        delete_joinStats(min);
        char* left, *right;

        int la, ra, lc, rc;
        split_part(pn->left_part, &la, &lc);
        split_part(pn->right_part, &ra, &rc);

        if(pos_in_list != 0 ){
            //the node was added from a connected node->list.
            Node node1 = connected_relations[la][ra]->list->head;
            Node node2 = connected_relations[ra][la]->list->head;
            for(int node_number=0; node_number<pos_in_list-1; node_number++){
                node1 = node1->next;
                node2 = node2->next;
            }
            Connected add1 = (Connected)node1->data;
            Connected add2 = (Connected)node2->data;
            left = malloc(sizeof(char)* strlen(pn->left_part)+1);
            strcpy(left, add1->join_left);
            right = malloc(sizeof(char)*strlen(pn->right_part)+1);
            strcpy(right, add1->join_right);
            add1->added = true;              //used to see if node is been added so we dont add it again.
            add2->added = true;
            connected_relations[la][ra]->has_list--;
            connected_relations[ra][la]->has_list--;
        }else{
            left = malloc(sizeof(char)* strlen(pn->left_part)+1);
            strcpy(left, pn->left_part);
            right = malloc(sizeof(char)*strlen(pn->right_part)+1);
            strcpy(right, pn->right_part);
        }
        predicate_node_delete(pn);
        PredicateNode temp = predicate_node_create(left, '=', right);
        predicates_queue_add(temp, ordered_queue);

        if(pos_in_list == 0){
            //added item that is not in list.
            if(connected_relations[ra][la]->has_list == 0){
                //list is empty so we remove arrays from R.
                R = resize_R(i, R, &r_size, n_array, cost,pos_in_list);
            }else{
                //list is not yet empty so we do not remove arrays from R
            }
            /* make added == true so that we wont check same node again */
            connected_relations[ra][la]->added = true;
            connected_relations[la][ra]->added = true;

        }else{
            //added item from a list.
            if(connected_relations[ra][la]->added){
                //base node is already added.
                if(connected_relations[ra][la]->has_list == 0){
                    //list is now empty so we remove arrays
                    R = resize_R(i, R, &r_size, n_array, cost, pos_in_list);
                }
            }
        }

    }
    free(R);
    delete_cost_array(cost, num_joins+1);
    delete_connected_relations(connected_relations, n_array);
}

/* calculates new f after min and max change (like a filter) */
uint64_t calculate_f(MappedRelation map, int col, uint64_t max, uint64_t min){
    uint64_t f = 0; 
    size_t start, end;
    uint64_t* data = (uint64_t*)map->addr;
    get_col(map, col, &start, &end);
    for(size_t j=start; j<end; j++){
        if(data[j] >= min && data[j] <= max)
            f++;
    }
    return f;
}

/* Υπολογίζει τις νέες τετράδες στατιστικών */
void calculate_new_stats(uint64_t* min, uint64_t* max, uint64_t *f_S, uint64_t *f_R, Stats stats_s, Stats stats_r, MappedRelation mapS, MappedRelation mapR, int scol, int rcol){
    if(stats_s->u < stats_r->u)
        *max = stats_s->u;
    else
        *max = stats_r->u;
    if(stats_s->l < stats_r->l)
        *min = stats_r->l;
    else
        *min = stats_s->l; 

    /* Filter arrays */
    *f_S = calculate_f(mapS, scol, *max, *min);
    *f_R = calculate_f(mapR, rcol, *max, *min);
}

ArrayStats* malloc_array_stats(int size_of_subset, int* S, List list_array, int* array_value){
    ArrayStats* array_stats = malloc(sizeof(ArrayStats) * (size_of_subset));
    MappedRelation map;
    for(int j=0; j<size_of_subset; j++){
        array_stats[j] = malloc(sizeof(struct arrayStats));
        map = get_map(list_array, array_value, S[j]);
        array_stats[j]->cols = map->cols;
        array_stats[j]->array = S[j];
        array_stats[j]->stats = malloc(sizeof(Stats)* map->cols);
        for(int k=0; k<map->cols; k++){
            array_stats[j]->stats[k] = malloc(sizeof(struct stats));
        }
    }
    return array_stats;
}


JoinStats calclulate_join_stats(JoinStats* cost, int s, int r, JoinStats current, int i, List list_array, int* array_value, int scol, int rcol){
    int s_pos, r_pos;
    
    Stats *s_stats;
    Stats *r_stats = NULL;
    boolean r_in_S = false; /* true if R is already in S (join with R and S has happened before) */

    if(i==1){
        /* Πρώτη επανάληψη ο πίνακας S έχει όλους τους πίνακες και εμείς θέλουμε να κάνουμε join αυτόν στη θέση s */
        s_stats = cost[0]->array_stats[s]->stats;
        r_stats = cost[0]->array_stats[r]->stats;
        s_pos = 0;          //η θέση του S στον cost[i]->S πίνακα.
        r_pos = 1;          // η θέση του R στον cost[i]->S πίνακα.
    }
    else{
        /* get s stats */
        for(int j=0; j<cost[i-1]->size_of_subset; j++){
            if(cost[i-1]->S[j] == s){
                s_stats = cost[i-1]->array_stats[j]->stats;
                s_pos = j;     //η θέση του S στον cost[i]->S πίνακα.
                break;
            }
        }
        /* get r stats if they exist in S*/
        for(int j=0; j<cost[i-1]->size_of_subset; j++){
            if(cost[i-1]->S[j] == r){
                r_stats = cost[i-1]->array_stats[j]->stats;
                r_pos = j;      //update r pos in S array.
                r_in_S = true;
                break;
            }
        }
        if(!r_in_S){
            //αν δεν υπαρχουν τα στατιστικά του r στον πίνακα S τα παίρνουμε από τα αρχικά του cost[0].
            r_stats = cost[0]->array_stats[r]->stats;
            r_pos = cost[i-1]->size_of_subset;        //R will be in the last position of cost[i]->S array.
        }
    }

    uint64_t max, min, f_s, f_r;
    MappedRelation mapS = get_map(list_array, array_value, s);
    MappedRelation mapR = get_map(list_array, array_value, r);
    
    /*υπολογισμός νέων τετράδων στατιστικών των αντίστοιχων στηλών για σχέση S και σχέση R που γίνονται join */
    calculate_new_stats(&min, &max, &f_s, &f_r, s_stats[scol], r_stats[rcol], mapS, mapR, scol, rcol);

    /* now calculate cost of join */
    uint64_t f = (float)((float)f_s*(float)f_r) /(float)(max-min+1);
    current->cost = f;

    /* update stats of arrays that are in new S */
    if(i != 1){
        int r_old_pos;
        if(r_in_S){
            /* if r is already in previous S we dont increase subset size and we dont add R in S again*/
            current->size_of_subset = cost[i-1]->size_of_subset;
            /* copy S array from cost[i-1] */
            current->S = malloc(sizeof(int) * current->size_of_subset);
            for(int j=0; j<current->size_of_subset; j++){
                current->S[j] = cost[i-1]->S[j];
            }
            r_old_pos = r_pos;
        }else{
            r_old_pos = r;
            /* R is not in S so we add it in last position of new S*/
            current->size_of_subset = cost[i-1]->size_of_subset+1;
            /* copy S array from cost[i-1] */
            current->S = malloc(sizeof(int) * current->size_of_subset);
            for(int j=0; j<current->size_of_subset-1; j++){
                current->S[j] = cost[i-1]->S[j];
            }
            current->S[current->size_of_subset-1] = r;      //add r to S.
        }
        /* allocate memory for new array_stats */
        current->array_stats = malloc_array_stats(current->size_of_subset, current->S, list_array, array_value);
    
        /* calculate new stats for every array in S */
        
        /* for arrays other than S and R */
        for(int j=0; j<current->size_of_subset; j++){
            if(current->S[j] != s && current->S[j] != r){
                /* copy stats and change only f */
                for(int k=0; k<current->array_stats[j]->cols; k++){
                    current->array_stats[j]->stats[k]->u = cost[i-1]->array_stats[j]->stats[k]->u;
                    current->array_stats[j]->stats[k]->l = cost[i-1]->array_stats[j]->stats[k]->l;
                    current->array_stats[j]->stats[k]->f = f;
                    current->array_stats[j]->stats[k]->d = cost[i-1]->array_stats[j]->stats[k]->d;
                }
            }
        }

        int cost_pos;
        if(r_in_S)
            cost_pos = i-1;
        else
            cost_pos = 0;

        /* stats for S */
        for(int j=0; j<current->array_stats[s_pos]->cols; j++){
            if(j == scol){
                /*column that was used in join*/
                current->array_stats[s_pos]->stats[scol]->u = max;
                current->array_stats[s_pos]->stats[scol]->l = min;
                current->array_stats[s_pos]->stats[scol]->d = cost[i-1]->array_stats[s_pos]->stats[scol]->d * cost[cost_pos]->array_stats[r_old_pos]->stats[rcol]->d / (max-min+1);
                current->array_stats[s_pos]->stats[scol]->f = f;
            }else{
                /* column NOT used in join*/
                int old_d = cost[i-1]->array_stats[s_pos]->stats[scol]->d;
                int new_d = cost[i-1]->array_stats[s_pos]->stats[scol]->d * cost[cost_pos]->array_stats[r_old_pos]->stats[rcol]->d / (max-min+1);
                current->array_stats[s_pos]->stats[j]->u = cost[i-1]->array_stats[s_pos]->stats[j]->u;
                current->array_stats[s_pos]->stats[j]->l = cost[i-1]->array_stats[s_pos]->stats[j]->l;
                current->array_stats[s_pos]->stats[j]->d = cost[i-1]->array_stats[s_pos]->stats[j]->d * (1 - pow(1 - (float)new_d/(float)old_d, (float)cost[i-1]->array_stats[s_pos]->stats[j]->f/(float)cost[i-1]->array_stats[s_pos]->stats[j]->d));
                current->array_stats[s_pos]->stats[j]->f = f;
            }
        }
        
        /* stats for R */
        for(int j=0; j<current->array_stats[r_pos]->cols; j++){
            if(j == rcol){
                /* column that was used in join */
                current->array_stats[r_pos]->stats[rcol]->u = max;
                current->array_stats[r_pos]->stats[rcol]->l = min;
                current->array_stats[r_pos]->stats[rcol]->d = current->array_stats[s_pos]->stats[scol]->d;
                current->array_stats[r_pos]->stats[rcol]->f = f;
            }else{
                /* column NOT used in join */
                int old_d = cost[cost_pos]->array_stats[r_old_pos]->stats[rcol]->d;
                int new_d = current->array_stats[s_pos]->stats[scol]->d;
                current->array_stats[r_pos]->stats[j]->u = cost[cost_pos]->array_stats[r_old_pos]->stats[j]->u;
                current->array_stats[r_pos]->stats[j]->l = cost[cost_pos]->array_stats[r_old_pos]->stats[j]->l;
                current->array_stats[r_pos]->stats[j]->d = cost[cost_pos]->array_stats[r_old_pos]->stats[j]->d * (1 - pow(1 - (float)new_d/(float)old_d, (float)cost[cost_pos]->array_stats[r_old_pos]->stats[j]->f/(float)cost[cost_pos]->array_stats[r_old_pos]->stats[j]->d));
                current->array_stats[r_pos]->stats[j]->f = f; 
            }
        }
    }else{
        /* i == 1 and current size is 2 with only S and R */
        current->S = malloc(sizeof(int)*2);
        current->size_of_subset = 2;
        current->S[0] = s;
        current->S[1] = r;
        current->array_stats = malloc_array_stats(current->size_of_subset, current->S, list_array, array_value);
        /*copy stats for S*/
        for(int j=0; j<current->array_stats[0]->cols; j++){
            if(j == scol){
                /*column that was used in join*/
                current->array_stats[0]->stats[scol]->u = max;
                current->array_stats[0]->stats[scol]->l = min;
                current->array_stats[0]->stats[scol]->d = (float)((float)s_stats[scol]->d * (float) r_stats[rcol]->d )/(float)(max-min+1);
                current->array_stats[0]->stats[scol]->f = f;
            }else{
                /* column NOT used in join*/
                int old_d = s_stats[scol]->d;
                int new_d = (float)((float)s_stats[scol]->d * (float) r_stats[rcol]->d)/(float)(max-min+1);
                current->array_stats[0]->stats[j]->u = s_stats[j]->u;
                current->array_stats[0]->stats[j]->l =s_stats[j]->l;
                current->array_stats[0]->stats[j]->d = (float)s_stats[j]->d * (float)((float)1 - pow((float)1 - (float)new_d/(float)old_d, (float)s_stats[j]->f/(float)s_stats[j]->d));
                current->array_stats[0]->stats[j]->f = f;
            }
        }
        /* stats for R */
        for(int j=0; j<current->array_stats[1]->cols; j++){
            if(j == rcol){
                /* column that was used in join */
                current->array_stats[1]->stats[rcol]->u = max;
                current->array_stats[1]->stats[rcol]->l = min;
                current->array_stats[1]->stats[rcol]->d = current->array_stats[0]->stats[scol]->d;
                current->array_stats[1]->stats[rcol]->f = f;
            }else{
                /* column NOT used in join */
                int old_d = r_stats[rcol]->d;
                int new_d = (float)((float)r_stats[rcol]->d * (float)r_stats[rcol]->d) /(float) (max-min+1);
                current->array_stats[1]->stats[j]->u =r_stats[j]->u;
                current->array_stats[1]->stats[j]->l = r_stats[j]->l;
                current->array_stats[1]->stats[j]->d = r_stats[j]->d * (1 - pow(1 - (float)new_d/(float)old_d, (float)r_stats[j]->f/(float)r_stats[j]->d));
                current->array_stats[1]->stats[j]->f = f; 
            }
        }
    }
    return current;
}
