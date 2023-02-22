#include "operations.h"

//return an id array when the column doesnt belong in the intermidate results
int* filter(MappedRelation map, PredicateNode node, int column, int* count){

    int* id_array = (int*)malloc(sizeof(uint64_t)* map->rows);
    int row_id = 0;
    size_t start, end;
    uint64_t* data = (uint64_t*)map->addr;

    get_col(map, column, &start, &end);

    if(node->op == '<'){
        /* for each row in column we filter */
        for (size_t i = start; i < end; ++i){
            if(data[i] < atoi(node->right_part)){
                /* add id to the array of ids */
                id_array[(*count)] = row_id;
                (*count)++;
            }
            row_id++;
        }
    }
    else if(node->op == '>'){
        for (size_t i = start; i < end; ++i){
            if(data[i] > atoi(node->right_part)){
                /* add id to the array of ids */
                id_array[(*count)] = row_id;
                (*count)++;
            }
            row_id++;
        }
    }
    else{
        for (size_t i = start; i < end; ++i){
            if(data[i] == atoi(node->right_part)){
                /* add id to the array of ids */
                id_array[(*count)] = row_id;
                (*count)++;
            }
            row_id++;
        }
    }

    id_array = realloc(id_array, sizeof(uint64_t)*(*count));
    return id_array;
}

//delete old id array and return a new id array when the column does belong in the intermidate results
int* filter_intermidiate(IntermediateResultsFilter intermediate_result, MappedRelation map, PredicateNode node, int column, int* count){

    int* id_array = (int*)malloc(sizeof(uint64_t) * intermediate_result->size_filter_array);
    size_t start, end;
    uint64_t* data = (uint64_t*)map->addr;

    get_col(map, column, &start, &end);

    if(node->op == '>'){
        for(int i=0; i<intermediate_result->size_filter_array; i++){

            if(data[(size_t)intermediate_result->filter_array[i] + start] > atoi(node->right_part)){
                id_array[*count] = intermediate_result->filter_array[i];
                (*count)++;
            }
        }
    }
    else if(node->op == '<'){
        for(int i=0; i<intermediate_result->size_filter_array; i++){

            if(data[(size_t)intermediate_result->filter_array[i] + start] < atoi(node->right_part)){
                id_array[*count] = intermediate_result->filter_array[i];
                (*count)++;
            }
        }
    }
    else{
        for(int i=0; i<intermediate_result->size_filter_array; i++){

            if(data[(size_t)intermediate_result->filter_array[i] + start] == atoi(node->right_part)){
                id_array[*count] = intermediate_result->filter_array[i];
                (*count)++;
            }
        }        
    }
    intermediateResultsFilter_delete(intermediate_result, false);
    id_array = realloc(id_array, sizeof(int)*(*count));
    return id_array;
}

//returns array number and column number
void split_part(char* part, int* number_of_array, int* number_of_column){

    char* temp = malloc(strlen(part)+1);
    strcpy(temp, part);

    char* pos = strchr(temp, '.');
    *number_of_column = atoi(pos+1);

    char* str;
    *number_of_array = atoi(strtok_r(temp, ".", &str));
    
    free(temp);
    return;
}

MappedRelation get_map(List list_array, int* array_value, int number_of_array){
    /* traverse list to find array to apply filter on.*/
    Node current_node = list_array->head;
    for(int i = 0; i < array_value[number_of_array]; i++)
        current_node = current_node->next;

    return (MappedRelation)current_node->data;
}

void self_join_nofilter(IntermediateResultsFilter* filters, int num_array1, MappedRelation map, int col1, int col2){

    int* id_array = (int*)malloc(sizeof(uint64_t)* map->rows);
    int count = 0;

    size_t start1, start2, end1, end2;
    uint64_t* data = (uint64_t*)map->addr;

    get_col(map, col1, &start1, &end1);
    get_col(map, col2, &start2, &end2);

    for(size_t i=0; i < map->rows; i++){
        if(data[i + start1] == data[i + start2]){
            id_array[count] = i;
            count++;
        }
    }

    id_array = realloc(id_array, sizeof(int)*(count));
    filters[num_array1] = intermediateResultsFilter_create(id_array, count, map->cols, true);

    return;
}

void self_join_onefilter(IntermediateResultsFilter* filters, int num_array1, MappedRelation map, int col1, int col2){

    IntermediateResultsFilter filter = filters[num_array1];

    int* id_array = (int*)malloc(sizeof(uint64_t)* filter->size_filter_array);
    int count = 0;

    size_t start1, start2, end1, end2;
    uint64_t* data = (uint64_t*)map->addr;

    get_col(map, col1, &start1, &end1);
    get_col(map, col2, &start2, &end2);

    for(size_t i=0; i < filter->size_filter_array; i++){
        if(data[(size_t)filter->filter_array[i] + start1] == data[(size_t)filter->filter_array[i] + start2]){
            id_array[count] = filter->filter_array[i];
            count++;
        }
    }

    intermediateResultsFilter_delete(filter, false);

    id_array = realloc(id_array, sizeof(int)*(count));
    filters[num_array1] = intermediateResultsFilter_create(id_array, count, map->cols,false);

    return;
}

void apply_filter(List list_array, int* array_value, IntermediateResultsFilter* intermediate_results, PredicateNode node){

    int number_of_array, column;
    split_part(node->left_part, &number_of_array, &column);

    MappedRelation map = get_map(list_array, array_value, number_of_array);

    int count = 0;
    int* id_array;
    
    Stats* old_stats;
    boolean filtered = false;
    if(intermediate_results[number_of_array] != NULL){
        filtered=true;
        old_stats = intermediate_results[number_of_array]->stats;
    }    

    if(strchr(node->right_part, '.') != NULL){
        //self join that is handled as a filter.
        int arr, col2;
        split_part(node->right_part, &arr, &col2);
        //otan den iparxei filtro
        if(intermediate_results[number_of_array] == NULL){
            self_join_nofilter(intermediate_results, number_of_array, map, column, col2);
        }
        //otan iparxei filtro
        else{
            self_join_onefilter(intermediate_results, number_of_array, map, column, col2);
        }
    }else if(intermediate_results[number_of_array] == NULL){
        id_array = filter(map, node, column, &count);
        intermediate_results[number_of_array] = intermediateResultsFilter_create(id_array, count, map->cols, true);
    }
    else{
        id_array = filter_intermidiate(intermediate_results[number_of_array], map, node, column, &count);
        intermediate_results[number_of_array] = intermediateResultsFilter_create(id_array, count, map->cols,false);
    }
    
    if(!filtered)
        initialize_statistics(intermediate_results[number_of_array], map);
    
    else
        intermediate_results[number_of_array]->stats = old_stats;
    
        
    
    return;
}

//should freed after | returns an array of values from map
uint64_t* get_filtered_values(IntermediateResultsFilter* filters, int num_array, MappedRelation map, int col){
    uint64_t* values = (uint64_t*)malloc(sizeof(uint64_t) * filters[num_array]->size_filter_array);
    size_t start, end;
    uint64_t* data = (uint64_t*)map->addr;
    get_col(map, col, &start, &end);

    for(int i=0; i<filters[num_array]->size_filter_array; i++)
        values[i] =  data[(size_t)filters[num_array]->filter_array[i] + start];

    return values;
}

//should freed after | returns an array of values from map
uint64_t* get_joined_values(JoinIntermidiate join_intermidiate, int num_array, MappedRelation map, int col){

    //θελουμε να παρουμε την σωστη στηλη απο το indermidiate
    int col_intermidiate = join_intermidiate->pos[num_array];
    int rows = join_intermidiate->rows_id_array;
    uint64_t* values = malloc(sizeof(*values) * rows);

    size_t start, end;
    uint64_t* data = (uint64_t*)map->addr;
    get_col(map, col, &start, &end);

    for(int i=0; i < rows; i++)
        values[i] = data[(size_t)join_intermidiate->id_array[i][col_intermidiate] + start];

    return values;
}

uint64_t sum_joined_values(JoinIntermidiate join_intermidiate, int num_array, MappedRelation map, int col){

    //θελουμε να παρουμε την σωστη στηλη απο το indermidiate
    int col_intermidiate = join_intermidiate->pos[num_array];
    int rows = join_intermidiate->rows_id_array;
    uint64_t sum = 0;

    size_t start, end;
    uint64_t* data = (uint64_t*)map->addr;
    get_col(map, col, &start, &end);

    for(int i=0; i < rows; i++)
        sum += data[(size_t)join_intermidiate->id_array[i][col_intermidiate] + start];

    return sum;
}

//καλουμε αυτη την συναρτηση οταν θελουμε να κανουμε update το join intermidiate
//στην περιπτωση που υπηρχε μονο το ενα relation μέσα στο intermidiate οταν εγινε το join
void update_join_intermidiate(JoinIntermidiateArray joinArray, JoinIntermidiate old_join_internidiate, int final_rows, int** array_id, int pos_in_array_id, int num_array_in, int num_array_out){
    JoinIntermidiate join_intermidiate = old_join_internidiate;
    //φτιαχνουμε ενα array που θα κραταει την πληροφορια για το καινουργιο intermidiate
    //θα εχει οσες γραμμες με το αποτελέσμα της join
    int** new_id_array = malloc(sizeof(int*) * final_rows);
    //τον αρχικοποιουμε
    for(int i=0; i<final_rows; i++){

        //θα εχει οσες στηλες ειχε και ο παλιος + 1
        new_id_array[i] = malloc(sizeof(int) * (join_intermidiate->cols_id_array + 1));

        //αρχικα θα προσθεσουμε τις παλιες θεσεις του intermidiate στον καινουργιο πινακα
        for(int j=0; j < join_intermidiate->cols_id_array; j++){

            // το result[i][0] μας δινει τα row ids του παλιου indermidiate γιατι το num_array1 ηταν σε ενδιαμεσο join intermidiate
            // εμεις τα θελουμε αυτα τα rows ids για να παμε στο παλιο και να παρουμε τα σωστα rowids απο τα παλια ενδιαμεσα αποτελεσματα

            new_id_array[i][j] = join_intermidiate->id_array[array_id[i][pos_in_array_id]][j];
            
        }

        //μετα θα βαλουμε και την καινουργια στηλη με τα ids απο το num_array2
        new_id_array[i][join_intermidiate->cols_id_array] = array_id[i][1-pos_in_array_id];
        
        free(array_id[i]);
    }
    free(array_id);

    //Στην παλια θεση του intermidiate φτιαχνουμε το καινουργιο με αυξημενο cols και κανουργιο id array
    JoinIntermidiate temp = joinIntermidiate_create(join_intermidiate->cols_id_array + 1, final_rows, joinArray->size_pos, new_id_array);
    
    //αντιγραφουμε στο καινουργιο intermidiate τις θεσεις των στηλων των παλιών relation
    for(int i=0; i< joinArray->size_pos; i++){
        temp->pos[i] = join_intermidiate->pos[i];
    }
    //το καινουργιο relation που θα μπει στο intermidiate θα βρισκεται στην τελυταια στηλη του πινακα με τα ids
    temp->pos[num_array_out] = join_intermidiate->cols_id_array;

    //τωρα το num_array2 θα δειχνει οτι εχει και αυτο intermidiate results και θα ανοικει σε αυτο του num array 1
    joinArray->pos[num_array_out] = joinArray->pos[num_array_in];

    //βαζουμε το κανουργιο intermidiate result στη θεση του παλιου
    joinArray->array[joinArray->pos[num_array_in]] = temp;

    //διαγραφουμε το παλιο intermidiate που δεν χρειαζεται πια
    joinIntermidiate_delete(join_intermidiate);

    return;
}

//καλουμε αυτη την συναρτηση οταν θελουμε να κανουμε update το join intermidiate
//στην περιπτωση που υπηρχουν και τα 2 relation μέσα σε διαφορετικα intermidiate οταν εγινε το join
void update_join_intermidiate_different(JoinIntermidiate old_intermidiate1, JoinIntermidiate old_intermidiate2, int final_rows, int** array_id, JoinIntermidiateArray joinArray, int num_array_in, int num_array_out){

    JoinIntermidiate join_intermidiate1 = old_intermidiate1;
    JoinIntermidiate join_intermidiate2 = old_intermidiate2;

    //φτιαχνουμε ενα array που θα κραταει την πληροφορια για το καινουργιο intermidiate
    //θα εχει οσες γραμμες με το αποτελέσμα της join
    int** new_id_array = malloc(sizeof(int*) * final_rows);

    //τον αρχικοποιουμε
    //cols ειναι οι στηλες του καινουργιου id array στον καινουργιο join intermidiate
    int cols = join_intermidiate1->cols_id_array + join_intermidiate2->cols_id_array;
    for(int i=0; i<final_rows; i++){

        //θα εχει οσες στηλες ειχε και ο παλιος + 1
        new_id_array[i] = malloc(sizeof(int) * cols);

        for(int j=0; j< join_intermidiate1->cols_id_array; j++){
            new_id_array[i][j] = join_intermidiate1->id_array[array_id[i][num_array_in]][j];
        }

        for(int j=join_intermidiate1->cols_id_array; j<cols; j++){
            new_id_array[i][j] = join_intermidiate2->id_array[array_id[i][num_array_out]][j];
        }
    }

    for(int i=0; i<final_rows; i++)
        free(array_id[i]);
    free(array_id);

    //Στην παλια θεση του intermidiate φτιαχνουμε το καινουργιο με αυξημενο cols και κανουργιο id array
    JoinIntermidiate temp = joinIntermidiate_create(cols, final_rows, joinArray->size_pos, new_id_array);

    //αντιγραφουμε στο καινουργιο intermidiate τις θεσεις των στηλων των παλιών relation
    int pos_count = 0;
    for(int i=0; i< joinArray->size_pos; i++){
        if(join_intermidiate1->pos[i] != -1){
            temp->pos[i] = join_intermidiate1->pos[i];
            pos_count++;
        }
    }

    for(int i=0; i<joinArray->size_pos; i++){
        if(join_intermidiate2->pos[i] != -1){
            temp->pos[i] = join_intermidiate2->pos[i] + pos_count;
        }
    }

    //παμε στα παλια relations και τους βαζουμε να δειχνουν στη καινουργια θεση των intermidiates
    //θα πρέπει για ολα τα relations που βρισκονταν σε ενα απο το δυο intermidiates να παμε και να τα βαλουμε μαζι στο intermidiate του αλλου
    for(int i=0; i<joinArray->size_pos; i++){
        if(join_intermidiate2->pos[i] != -1){
            joinArray->pos[join_intermidiate2->pos[i]] = joinArray->pos[num_array_in];
        }
    }

    //βαζουμε το κανουργιο intermidiate result στη θεση του παλιου
    joinArray->array[joinArray->pos[num_array_in]] = temp;

    joinArray->array[joinArray->pos[num_array_out]] = NULL;

    //διαγραφουμε τα παλια intermidiate που δεν χρειαζονται πια
    joinIntermidiate_delete(join_intermidiate2);
    joinIntermidiate_delete(join_intermidiate1);
    
    return;
}

void self_join_intermediate(JoinIntermidiate joinIntermidiate, int num_array1, MappedRelation map, int col1, int col2){
    
    int* id_array = (int*)malloc(sizeof(int)* joinIntermidiate->rows_id_array);
    int count = 0;
    int pos_in_array = joinIntermidiate->pos[num_array1];       //i stili poy einai sto id_array tou join

    size_t start1, start2, end1, end2;
    uint64_t* data = (uint64_t*)map->addr;

    get_col(map, col1, &start1, &end1);
    get_col(map, col2, &start2, &end2);

    for(size_t i=0; i<joinIntermidiate->rows_id_array; i++){
        if(data[(size_t)joinIntermidiate->id_array[i][pos_in_array] + start1] == data[(size_t)joinIntermidiate->id_array[i][pos_in_array] + start2]){
            id_array[count] = i;
            count++;
        }
    }

    id_array = realloc(id_array, sizeof(int) * count);

    //το καινουργιο id array του intermidiate
    int** new_id_array = malloc(sizeof(int*) * count);
   
    //οποτε θα παμε στο καινουργιο id_array του intermidiate και θα αποθηκευσουμε μονο τις γραμμές που βρισκονται στο παραπανω id_array
    int** old_id_array = joinIntermidiate->id_array;

    for(int i=0; i<count; i++){
        new_id_array[i] = malloc(sizeof(int) * joinIntermidiate->cols_id_array);
        for(int j=0; j<joinIntermidiate->cols_id_array; j++){
            new_id_array[i][j] = old_id_array[id_array[i]][j];
        }
    }

    //ανανεωνουμε το intermidiate
    joinIntermidiate->id_array = new_id_array;

    //διαγραφουμε το παλιο array id του intermidiate
    for(int i=0; i<joinIntermidiate->rows_id_array; i++)
        free(old_id_array[i]);
    free(old_id_array);

    free(id_array);

    //ανανεωνουμε τις γραμμες του intermidiate
    joinIntermidiate->rows_id_array = count;
}

struct check_args{
    int start;
    int end;
    int* filter;
    int** array_id;
    int col;
};
typedef struct check_args* CheckArgs;

CheckArgs create_check_args(int start, int end, int* filter, int** array_id, int col){
    CheckArgs args = malloc(sizeof(*args));
    
    args->start = start;
    args->end = end;
    args->filter = filter;
    args->array_id = array_id;
    args->col = col;

    return args;
}

int CheckFilterJob(Pointer value){
    if(value == NULL)
        return 0;
    CheckArgs args = (CheckArgs)value;
    for(int i=args->start; i<args->end; i++)
        args->array_id[i][args->col] = args->filter[args->array_id[i][args->col]];

    return 0;
}

void parallel_filter_check(int* filter_array, int final_rows, int num_array, int** array_id, int col){
    int part_size = final_rows / NUM_THREADS;
    int start, end;
    CheckArgs* args = malloc(sizeof(CheckArgs*) * NUM_THREADS);
    for(int i=0; i<NUM_THREADS; i++){
        start = i*part_size;
        if(i == NUM_THREADS-1)
            end = final_rows;
        else
            end = start + part_size;
        args[i] = create_check_args(start, end, filter_array, array_id, col);
        serve_task(CheckFilterJob, (Pointer)args[i]);
    }
    barrier();

    for(int i=0; i<NUM_THREADS; i++)
        free(args[i]);
    free(args);
}

struct merge_args{
    int start;
    int end;
    int** new_id_array;
    int** old_id_array;
    int* array_id;
    int cols;
};
typedef struct merge_args* MergeArgs;

MergeArgs create_merge(int start, int end, int** new_id_array, int** old_id_array, int* array_id, int cols){
    MergeArgs args = malloc(sizeof(*args));

    args->start = start;
    args->end = end;
    args->new_id_array = new_id_array;
    args->old_id_array = old_id_array;
    args->array_id = array_id;
    args->cols = cols;

    return args;
}

int MergeTask(Pointer value){
    if(value == NULL)
        return 0;
    MergeArgs args = (MergeArgs)value;

    for(int i=args->start; i<args->end; i++){
        for(int j=0; j<args->cols; j++){
            args->new_id_array[i][j] = args->old_id_array[args->array_id[i]][j];
        }
    }

    return 0;
}

void parallel_merge(int count, int** new_id_array, int** old_id_array, int* array_id, int cols){
    int part_size = count / NUM_THREADS;
    int start, end;
    MergeArgs* args = malloc(sizeof(MergeArgs*) * NUM_THREADS);
    for(int i=0; i<NUM_THREADS; i++){
        start = i*part_size;
        if(i == NUM_THREADS-1)
            end = count;
        else
            end = start + part_size;
        args[i] = create_merge(start, end, new_id_array, old_id_array, array_id, cols);
        serve_task(MergeTask, (Pointer)args[i]);
    }
    barrier();

    for(int i=0; i<NUM_THREADS; i++)
        free(args[i]);
    free(args);
}

void apply_join(List list_array, int* array_value, IntermediateResultsFilter* filters, JoinIntermidiateArray joinArray, PredicateNode node){
    int num_array1, num_array2, col1, col2;
    split_part(node->left_part, &num_array1, &col1);
    split_part(node->right_part, &num_array2, &col2);
    
    MappedRelation map1 = get_map(list_array, array_value, num_array1);
    MappedRelation map2 = get_map(list_array, array_value, num_array2);



    if(joinArray->pos[num_array1] == -1 && joinArray->pos[num_array2] == -1){

        //fprintf(stderr, "different relation | without intermidiates \n");

        //ψαχνουμε στο map για καθε id το αντιστοιχο value και φτιαχνουμε τον πινακα απο τα σωστα values
        uint64_t* values_num1 = NULL;
        int rows_num1 = 0;
        if(filters[num_array1] != NULL){
            values_num1 = get_filtered_values(filters, num_array1, map1, col1);
            rows_num1 = filters[num_array1]->size_filter_array;
        }

        uint64_t* values_num2 = NULL;
        int rows_num2 = 0;
        if(filters[num_array2] != NULL){
            values_num2 = get_filtered_values(filters, num_array2, map2, col2);
            rows_num2 = filters[num_array2]->size_filter_array;
        }

        //περναμε τον καινουργιο πινακα με τα values που ικανοποιουν το φιλτρο ωστε η συναρτηση να μην πάρει όλη την στήλη
        int** array_id;
        int final_rows;
        join_hash_different_relation(map1, col1, values_num1, rows_num1, map2, col2, values_num2, rows_num2, &final_rows, &array_id);

        //το final id array έχει ids σειριακά τα οποία δεν είναι σωστα οπότε τα αλλάζουμε με αυτά του φιλτρου
        if(filters[num_array1] != NULL){
            parallel_filter_check(filters[num_array1]->filter_array, final_rows, num_array1, array_id, 0);
            // for(int i=0; i<final_rows; i++)
            //     array_id[i][0] = filters[num_array1]->filter_array[array_id[i][0]];
        }

        if(filters[num_array2] != NULL){
            parallel_filter_check(filters[num_array2]->filter_array, final_rows, num_array2, array_id, 1);
            // for(int i=0; i<final_rows; i++)
            //     array_id[i][1] = filters[num_array2]->filter_array[array_id[i][1]];
        }

        //παρομοια με την παραπανω περιπτωση ιδια αρχικοποιήση του indermidiate result
        joinArray->array[joinArray->in_array] = joinIntermidiate_create(2, final_rows, joinArray->size_pos, array_id);
        joinArray->array[joinArray->in_array]->pos[num_array1] = 0;
        joinArray->array[joinArray->in_array]->pos[num_array2] = 1;

        joinArray->pos[num_array1] = joinArray->in_array;
        joinArray->pos[num_array2] = joinArray->in_array;

        joinArray->in_array++;
    }

    else if(joinArray->pos[num_array1] != -1 && joinArray->pos[num_array2] != -1){
        //σημαινει οτι τα relation ειναι στο ιδιο intermidiate οποτε κανουμε φιλτρο
        if(joinArray->pos[num_array1] == joinArray->pos[num_array2]){
            //τα relations δειχνουν στο ιδιο intermidiate
            JoinIntermidiate join_intermidiate = joinArray->array[joinArray->pos[num_array1]];

            //παιρνουμε τα join values για το καθε relation
            uint64_t* values1 = get_joined_values(join_intermidiate, num_array1, map1, col1);
            int rows1 = join_intermidiate->rows_id_array;

            uint64_t* values2 = get_joined_values(join_intermidiate, num_array2, map2, col2);
            int* array_id = malloc(sizeof(int) * rows1);
            //σειριακος ελεγχος για να ελεγξουμε τα values
            int count = 0;
            for(int i=0; i<rows1; i++){
                if(values1[i] == values2[i]){
                    array_id[count] = i;
                    count++;
                }
            }

            //και αυτες ειναι οι γραμμες του intermidiate που ικανοποιουν την ισοτητητα
            array_id = realloc(array_id, sizeof(int)*(count));

            //το καινουργιο id array του intermidiate
            int** new_id_array = malloc(sizeof(int*) * count);
            for(int i=0; i<count; i++)
                new_id_array[i] = malloc(sizeof(int) * join_intermidiate->cols_id_array);
            
            //οποτε θα παμε στο καινουργιο id_array του intermidiate και θα αποθηκευσουμε μονο τις γραμμές που βρισκονται στο παραπανω id_array
            int** old_id_array = join_intermidiate->id_array;

            
            parallel_merge(count, new_id_array, old_id_array, array_id, join_intermidiate->cols_id_array);
            // for(int i=0; i<count; i++){
            //     for(int j=0; j<join_intermidiate->cols_id_array; j++){
            //         new_id_array[i][j] = old_id_array[array_id[i]][j];
            //     }
            // }

            //ανανεωνουμε το intermidiate
            join_intermidiate->id_array = new_id_array;

            //διαγραφουμε το παλιο array id του intermidiate
            for(int i=0; i<join_intermidiate->rows_id_array; i++)
                free(old_id_array[i]);
            free(old_id_array);

            free(array_id);

            //ανανεωνουμε τις γραμμες του intermidiate
            join_intermidiate->rows_id_array = count;
            free(values1);
            free(values2);
        }
        //τα relations ειναι σε διαφορετικο intermidiate
        else{
            //printf("different relation | both different intermidiates \n");

            //παιρνουμε μονο τα αντιστοιχα values χωρις να ελεγχουμε τα filter
            JoinIntermidiate join_intermidiate1 = joinArray->array[joinArray->pos[num_array1]];
            uint64_t* values1 = get_joined_values(join_intermidiate1, num_array1, map1, col1);
            int rows1 = join_intermidiate1->rows_id_array;

            JoinIntermidiate join_intermidiate2 = joinArray->array[joinArray->pos[num_array2]];
            uint64_t* values2 = get_joined_values(join_intermidiate2, num_array2, map2, col2);
            int rows2 = join_intermidiate2->rows_id_array;

            int final_rows;
            int** results;

            join_hash_different_relation(map1, col1, values1, rows1, map2, col2, values2, rows2, &final_rows, &results);

            update_join_intermidiate_different(join_intermidiate1, join_intermidiate2, final_rows, results, joinArray, num_array1, num_array2);
        }
    }
    //ενα από τα δύο relation έχουν join intermidiate
    else{

        //fprintf(stderr, "different relation | one intermidiates \n");

        //Ψαχνουμε να δουμε ποιο απο τα δυο relation ειναι αυτο
        JoinIntermidiate join_intermidiate;
        
        uint64_t* values1 = NULL;
        int rows1 = 0;
        //αμα ειναι το πρωτο
        if(joinArray->pos[num_array1] != -1){
            //παιρνουμε το intermidiate
            join_intermidiate = joinArray->array[joinArray->pos[num_array1]];
            //παιρνουμε το join values
            values1 = get_joined_values(join_intermidiate, num_array1, map1, col1);
            //και το ποσες ειναι οι γραμμές των join values
            rows1 = join_intermidiate->rows_id_array;
        }
        else if(filters[num_array1] != NULL){
            values1 = get_filtered_values(filters, num_array1, map1, col1);
            rows1 = filters[num_array1]->size_filter_array;
        }

        uint64_t* values2 = NULL;
        int rows2 = 0;
        //αμα ειναι το δευτερο
        if(joinArray->pos[num_array2] != -1){
            //παρομοια για το num_array2
            join_intermidiate = joinArray->array[joinArray->pos[num_array2]];
            values2 = get_joined_values(join_intermidiate, num_array2, map2, col2);
            rows2 = join_intermidiate->rows_id_array;
        }
        else if(filters[num_array2] != NULL){
            values2 = get_filtered_values(filters, num_array2, map2, col2);
            rows2 = filters[num_array2]->size_filter_array;
        }

        int final_rows;
        int** results;
        join_hash_different_relation(map1, col1, values1, rows1, map2, col2, values2, rows2, &final_rows, &results);

        //το final id array έχει ids σειριακά τα οποία δεν είναι σωστα οπότε τα αλλάζουμε με αυτά του φιλτρου
        if(filters[num_array1] != NULL && joinArray->pos[num_array1] == -1){
            parallel_filter_check(filters[num_array1]->filter_array, final_rows, num_array1, results, 0);
            // for(int i=0; i<final_rows; i++)
            //     results[i][0] = filters[num_array1]->filter_array[results[i][0]];
        }

        if(filters[num_array2] != NULL && joinArray->pos[num_array2] == -1){
            parallel_filter_check(filters[num_array2]->filter_array, final_rows, num_array2, results, 1);
            // for(int i=0; i<final_rows; i++)
            //     results[i][1] = filters[num_array2]->filter_array[results[i][1]];
        }

        //αμα το num_array1 ηταν μεσα σε αλλο intermidiate τοτε πρεπει να φτιαξουμε καινουργιο 
        //μαζι με το num_array2 και να διαγραψουμε το παλιο intermidiate
        if(joinArray->pos[num_array1] != -1){
            update_join_intermidiate(joinArray, join_intermidiate, final_rows, results, 0, num_array1, num_array2);
        }
        else{
            update_join_intermidiate(joinArray, join_intermidiate, final_rows, results, 1, num_array2, num_array1);
        }

    }
    return;
}

void apply_sum(IntermediateResultsFilter* filters, List list_mapping, int* array_value, JoinIntermidiateArray joinArray, Node node, uint64_t* sum){
    int number_of_array, number_of_column;
    split_part(node->data, &number_of_array, &number_of_column);

    MappedRelation map = get_map(list_mapping, array_value, number_of_array);

    JoinIntermidiate join = joinArray->array[joinArray->pos[number_of_array]];

    *sum = sum_joined_values(join, number_of_array, map, number_of_column);

    if(*sum == 0)
        printf("NULL");
    else
        printf("%ld", *sum);
}