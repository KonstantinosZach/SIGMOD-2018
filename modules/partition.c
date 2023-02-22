#include "partition.h"

// static void print_hist(Histogram hist){
//     printf("\n");
//     for(int i=0; i<hist->size; i++)
//         printf("id:%d value:%d\n",hist->array[i]->id,hist->array[i]->value);
//     return;
// }

// static void print_relation(Relation relation){
//     printf("\n");
//     for(int i=0; i<relation->num_tuples; i++)
//     printf("id:%d value:%d\n",relation->array[i]->id,relation->array[i]->value);
//     return;
// }

int L2 = 512;
int N_LOWER_BOUND = 4;
int N_UPPER_BOUND = 8;
int N_SECOND_PASS_UPPER_BOUND = 16;

//hash based on <n> primary bytes
int hash(int n, uint64_t value){

    //decimal to binary
    char binary[n + 1];
    for(int i = (n-1); i >= 0; i--){
        binary[i] = value % 2 ? '1' : '0';
        value = value / 2;
    }
    binary[n] = '\0';   //last for for '\0'

    return (int) strtol(binary, NULL, 2);   //convert binary to decimal
}

/* Creates Prefix sum array from Histogram */
Histogram create_psum(Histogram hist){

    int counter = 0;            /* Corresponding position in new array */

    Histogram psum = malloc(sizeof(struct histogram));
    psum->size = hist->size;
    psum->array = malloc(sizeof(Tuple*) * psum->size);

    for(int i=0; i<psum->size; i++){
        *((psum->array)+i) = malloc(sizeof(struct tuple));
    }

    for(int i=0; i< psum->size; i++){
        psum->array[i]->id = hist->array[i]->id;
        psum->array[i]->value = counter;

        /* update counter */
        counter += hist->array[i]->value;

    }

    return psum;
}

/* Returns a new allocated psum */
Histogram copy_psum(Histogram psum){
    Histogram new_psum = malloc(sizeof(struct histogram));
    new_psum->size = psum->size;
    new_psum->array = malloc(sizeof(Tuple*) * psum->size);

    for(int i=0; i<new_psum->size; i++){
        new_psum->array[i] = malloc(sizeof(struct tuple));
        memcpy(new_psum->array[i], psum->array[i], sizeof(struct tuple));
    }

    return new_psum;
}

/* Το hist θα εχει 2^n tuples */
Histogram create_hist(int n){
    Histogram hist = malloc(sizeof(*hist));
    
    hist->array = malloc(sizeof(Tuple*) * pow(2,n));
    hist->size = pow(2,n);
    
    /* αρχικοποίηση id με ψευδοτιμή και πληθος 0 */
    for(int i=0; i<hist->size; i++){
        hist->array[i] = malloc(sizeof(struct tuple));
        hist->array[i]->id = -1;  
        hist->array[i]->value = 0;  
    }

    return hist;
}

void delete_hist(Histogram hist){
    for(int i=0; i<hist->size; i++){
        if(hist->array[i] != NULL){
            /* free each tuple */
            free(hist->array[i]);
        }
    }
    /* free array of tuples */
    free(hist->array);

    /* free histogram */
    free(hist);
}

void update_hist(Histogram hist, int hash_value){
    /* hash value -> bucket_id */
    /* if element exists in array */
    if(hist->array[hash_value]->id == hash_value){
        /* update value */
        hist->array[hash_value]->value = hist->array[hash_value]->value + 1;
    }
    else if(hist->array[hash_value]->id == -1){
        /* add bucket id and value */
        hist->array[hash_value]->id = hash_value;
        hist->array[hash_value]->value = hist->array[hash_value]->value + 1;
    }
}

void delete_psum(Histogram psum){
    delete_hist(psum);
}

void delete_relation(Relation relation){
    for(int i=0; i<relation->num_tuples; i++){
        free(relation->array[i]);
    }

    free(relation->array);

    free(relation);
}

Relation create_relation(int size){
    Relation relation = malloc(sizeof(* relation));

    //init the relation's array
    relation->array = malloc(sizeof(Tuple*) * size);
    for(int i=0; i<size; i++)
        relation->array[i] = malloc(sizeof(struct tuple));
    
    //init the relation's num
    relation->num_tuples = 0;

    //init primary bits
    relation->n_bits = N_LOWER_BOUND;

    return relation;
}

struct psum_args{
    int row_size;
    Tuple* row;
    int n;
    Histogram psum;
    Relation relation;
    int start;
    int end;
};
typedef struct psum_args* Psum_args;

Psum_args create_psum_args(int end, int start, int n, Histogram psum, Relation relation, Tuple* row, int row_size){
    Psum_args args = malloc(sizeof(*args));
    args->end = end;
    args->start = start;
    args->n = n;
    args->psum = psum;
    args->relation = relation;
    args->row = row;
    args->row_size = row_size;
    return args;
}

int psum_task(Pointer value){
    Psum_args args = (Pointer)value;
    for(int i = 0; i < args->row_size; i++){
        int hash_value = hash(args->n, args->row[i]->value);

        if(hash_value >= args->start && hash_value < args->end){
            args->relation->array[args->psum->array[hash_value]->value]->value = args->row[i]->value;
            args->relation->array[args->psum->array[hash_value]->value]->id = args->row[i]->id;
            args->psum->array[hash_value]->value ++;
            //args->relation->num_tuples ++;
        }

    }
    free(args);
    return 0;
}
//we need row_size of the intial row to construct the new relation
Relation re_ordered(Tuple* row, int row_size, Histogram psum, int n){
    Relation relation = create_relation(row_size);
    relation->n_bits = n;
    relation->num_tuples = row_size;

    //we create a copy cause we dont want to lose the original psum
    Histogram new_psum = copy_psum(psum);

    int workload = psum->size / NUM_THREADS;

    if(workload == 0)
        workload = psum->size / (NUM_THREADS - (NUM_THREADS - psum->size));

    int start = 0;
    int end = workload;
    for(int i=0; i<NUM_THREADS; i++){
        Psum_args args = create_psum_args(end, start, n, new_psum, relation, row, row_size);
        serve_task(psum_task, (Pointer)args);

        start = end;
        end += workload;
        if(i == (NUM_THREADS-2) && end < psum->size)
            end += (psum->size - end);
    }
    barrier();

    delete_psum(new_psum);

    return relation;
}

/* return the n, where n is the number of bits needed for the partition of the bucket*/
int get_n(Tuple* row, int row_size, int n, int n_end){
    int max = n;
    Histogram hist;
    boolean fits = false;           /* if data of largest bucket fits in L2 cache */
    
    /* for every value */
    while(!fits && n <= n_end){
        max = 0;
        hist = create_hist(n);
        fits = true;
        for(int i=0; i< row_size; i++){
            int hashed_value = hash(n, row[i]->value);
            update_hist(hist, hashed_value);
            
            if(hist->array[hashed_value]->value > max){
                /* update max value */
                max = hist->array[hashed_value]->value;
            }
            if((max > L2) && (n < n_end)){
                fits = false;
                
                delete_hist(hist);
                n++;
                
                break;
            }
        }
    }

    delete_hist(hist);

    return n;
}

//finds n that we will create pass 2 from
int find_max_n(Pass pass1){

    int psum_size = pass1->psum->size;

    Tuple* bucket;
    int last_position = 0;

    /* Create seperate buckets that need partitioning*/
    int max = 0;
    for(int i=0; i<psum_size; i++){
        /* if last bucket */
        if(i == (psum_size - 1)){
            bucket = malloc(sizeof(*bucket) * (pass1->relation->num_tuples - pass1->psum->array[psum_size-1]->value));
            last_position = pass1->relation->num_tuples - 1; /* last pos should be at least +1 from psum[psum_size-1]->value */
        }
        else{
            bucket = malloc(sizeof(*bucket)*(pass1->psum->array[i+1]->value - pass1->psum->array[i]->value));
            last_position = pass1->psum->array[i+1]->value;
        }
        
        int bucket_counter = 0;
        for(int j=pass1->psum->array[i]->value; j<last_position; j++){
            bucket[bucket_counter] = malloc(sizeof(struct tuple));
            bucket[bucket_counter]->value = pass1->relation->array[j]->value;
            bucket[bucket_counter]->id = pass1->relation->array[j]->id;
            bucket_counter++;
        }

        //we get the max n of each bucket and we keep the overall max
        int temp = get_n(bucket, bucket_counter, N_UPPER_BOUND+1, N_SECOND_PASS_UPPER_BOUND);
        if(temp > max)
            max = temp;

        delete_id_array(bucket, bucket_counter);
    }

    return max;
}

static int max(int a, int b){
    if(a < b)
        return b;
    return a;
}

struct hist_args{
    Tuple* row;
    int row_size;
    int start;
    int end;
    int n;
    int n_end;
    int* curr_n;
    Histogram hist;
};
typedef struct hist_args* HistArgs;

HistArgs create_hist_args(Tuple* row, int row_size, int start, int end, int n, int n_end, int* curr_n, Histogram hist){
    HistArgs args = malloc(sizeof(*args));
    args->row = row;
    args->row_size = row_size;
    args->start = start;
    args->end = end;
    args->n = n;
    args->n_end = n_end;
    args->curr_n = curr_n;
    args->hist = hist;

    return args;
}
/* caclulates histogram and n needed */
int HistogramJob(Pointer value){
    HistArgs args = (HistArgs)value;
    int max;
    Histogram hist;
    boolean fits = false;
    while(!fits && args->n <= args->n_end){
        max = 0;
        hist = create_hist(args->n);
        fits = true;
        for(int i=args->start; i<args->end; i++){
            int hashed_value = hash(args->n, args->row[i]->value);
            update_hist(hist, hashed_value);
            
            if(hist->array[hashed_value]->value > max){
                /* update max value */
                max = hist->array[hashed_value]->value;
            }
            if((max > L2) && (args->n < args->n_end)){
                fits = false;
                
                delete_hist(hist);
                args->n++;
                
                break;
            }
        }
    }
    *args->curr_n = args->n;
    args->hist->size = hist->size;
    args->hist->array = malloc(sizeof(Tuple*) * hist->size);
    for(int i=0; i<hist->size; i++){
        args->hist->array[i] = malloc(sizeof(struct tuple));
        args->hist->array[i]->value = hist->array[i]->value;
        args->hist->array[i]->id = hist->array[i]->id;
    }

    delete_hist(hist);

    return 0;
}

struct hist_args_n{
    Tuple* bucket;
    Pass pass1;
    int i;
    int psum_size;
    int* temp;
};
typedef struct hist_args_n* HistArgsN;

HistArgsN create_hist_args_n(Tuple* bucket, Pass pass1, int i, int psum_size, int* temp){
    HistArgsN args = malloc(sizeof(*args));
    /* bucket values and rowids*/
    args->bucket = bucket;
    args->pass1 = pass1;
    args->i = i;
    args->psum_size = psum_size;
    args->temp = temp;

    return args;
}
/* gets max n of every bucket */
int HistogramJobN(Pointer value){
    if(value == NULL)
        return 0;
    HistArgsN args = (HistArgsN)value;
    int last_position = 0;
    /* if last bucket */
    if(args->i == (args->psum_size - 1)){
        args->bucket = malloc(sizeof(*args->bucket) * (args->pass1->relation->num_tuples - args->pass1->psum->array[args->psum_size-1]->value));
        last_position = args->pass1->relation->num_tuples - 1; /* last pos should be at least +1 from psum[psum_size-1]->value */
    }
    else{
        args->bucket = malloc(sizeof(*args->bucket)*(args->pass1->psum->array[args->i+1]->value - args->pass1->psum->array[args->i]->value));
        last_position = args->pass1->psum->array[args->i+1]->value;
    }
    
    int bucket_counter = 0;
    for(int j=args->pass1->psum->array[args->i]->value; j<last_position; j++){
        args->bucket[bucket_counter] = malloc(sizeof(struct tuple));
        args->bucket[bucket_counter]->value = args->pass1->relation->array[j]->value;
        args->bucket[bucket_counter]->id = args->pass1->relation->array[j]->id;
        bucket_counter++;
    }

    //we get the max n of each bucket and we keep the overall max
    *args->temp = get_n(args->bucket, bucket_counter, N_UPPER_BOUND+1, N_SECOND_PASS_UPPER_BOUND);

    delete_id_array(args->bucket, bucket_counter);

    return 0;
}
/* gets max n of pass */
int parallel_max(Pass pass1){
    /* number of buckets */
    int psum_size = pass1->psum->size;

    /* iterations */
    int iterations;
    if(psum_size % NUM_THREADS == 0)
        iterations = psum_size / NUM_THREADS;
    else
        iterations = (psum_size / NUM_THREADS) + 1;

    int start, max1 = 0;
    for(int iter=0; iter<iterations; iter++){
        HistArgsN* args = malloc(sizeof(HistArgsN*) * NUM_THREADS);
        int* temp = calloc(NUM_THREADS, sizeof(int));
        for(int j=0; j<NUM_THREADS; j++){
            start = iter * NUM_THREADS;

            Tuple bucket;
            
            int i = start + j;

            if(i >= psum_size)
                args[j] = NULL;
            else
                args[j] = create_hist_args_n(&bucket, pass1, i, psum_size, &temp[j]);
            
            serve_task(HistogramJobN, (Pointer)args[j]);
        }
        barrier();
        
        for(int l=0; l<NUM_THREADS; l++){
            if(temp[l] > 0)
                max1 = max(temp[l], max1);
            if(args[l] != NULL)
                free(args[l]);
        }
        free(args);
        free(temp);
    }

    return max1;
}
/* calculates smaller histograms in parallel using parts of initial aray */
static void parallel_histograms(Histogram* histograms, HistArgs* args, Tuple* row_id_array, int* finaln, int part_size, int row_size, int n_start, int n_end){
    int start,end;
    for(int i=0; i<NUM_THREADS; i++){
        start = i*part_size;
        if(i == NUM_THREADS-1)
            end = row_size;
        else
            end = start+part_size;
        histograms[i] = malloc(sizeof(struct histogram));
        args[i] = create_hist_args(row_id_array, row_size, start, end, n_start, n_end, &finaln[i], histograms[i]);
        serve_task(HistogramJob, (Pointer)args[i]);    
    }
    barrier();
}
/* creates a big histogram using the smaller ones created before */
static Histogram merge_histograms(Histogram* histograms, HistArgs* args, Tuple* row_id_array, int* finaln, int m, int part_size, int row_size, boolean* case_c, int* cnt){
    Histogram big_hist;
    boolean fit = false, br = false; 
    int counter = m;
    while(!fit && counter <= N_UPPER_BOUND){
        histograms = malloc(sizeof(Histogram*) * NUM_THREADS);
        big_hist = create_hist(counter);
        finaln = calloc(NUM_THREADS, sizeof(int));
        args = malloc(sizeof(HistArgs*) * NUM_THREADS);

        /* create HistogramJobs for different n */
        parallel_histograms(histograms, args, row_id_array, finaln, part_size, row_size, counter, N_UPPER_BOUND);

        /* merge histograms to a big histogram and check if bucket size > L2 if yes redo for n++ */
        for(int i=0; i<big_hist->size; i++){
            for(int j=0; j<NUM_THREADS; j++){
                big_hist->array[i]->value += histograms[j]->array[i]->value;
                if((big_hist->array[i]->value > L2) && (counter<N_UPPER_BOUND)){
                    br = true;
                    counter++;
                    
                    break;
                }
            }
        }
        if(!br){
            fit = true;
            if(counter == N_UPPER_BOUND)
                *case_c = true;
        }
        else{
            br = false;

            /* free big_hist */
            delete_hist(big_hist);
        }
        /* free histograms */
        for(int i=0; i<NUM_THREADS; i++){
            delete_hist(histograms[i]);
            free(args[i]);
        }
        free(args);
        free(finaln);
        free(histograms);
    }
    *cnt = counter;
    return big_hist;
}

Partition partitioning(uint64_t* row, int row_size, boolean DEBUG){
    
    //if partitioning is called from tests
    if(DEBUG){
        L2 = 2;
        N_LOWER_BOUND = 2;
        N_UPPER_BOUND = 4;
        N_SECOND_PASS_UPPER_BOUND = 8;
    }

    /*** CASE a ~ Array fits entirely in L2 cache ***/

    if(row_size <= L2){
        Relation relation = create_relation(row_size);          /* Create relation */
        relation->n_bits = N_LOWER_BOUND;

        for(int i = 0; i < row_size; i++){
            relation->array[i]->value = row[i];
            relation->array[i]->id = i;
            relation->num_tuples ++;
        }

        //no psum needed
        Pass pass = create_pass(relation, NULL);

        //no secondary passages created
        Partition partition = create_partition(pass, NULL);

        return partition;
    }

    /*** CASE b ~ Array is bigger than L2 cache ***/
    
/*------------------------------------------------------------------------------------*/
    Tuple* row_id_array = create_id_array(row, row_size);

    /* Calculate smaller histograms */
    int part_size = row_size/NUM_THREADS;
    int* finaln = calloc(NUM_THREADS, sizeof(int));
    Histogram* histograms = malloc(sizeof(Histogram*) * NUM_THREADS);
    HistArgs* args = malloc(sizeof(HistArgs*) * NUM_THREADS);
    parallel_histograms(histograms, args, row_id_array, finaln, part_size, row_size, N_LOWER_BOUND, N_UPPER_BOUND);

    /* get the max n of smaller hists */
    int m=0;
    for(int i=0; i<NUM_THREADS; i++){
        m = max(finaln[i], m);
        delete_hist(histograms[i]);
        free(args[i]);
    }
    free(args);
    free(finaln);
    free(histograms);

    /* Now we create small histograms for m */
    int counter;
    boolean case_c = false;
    Histogram big_hist = merge_histograms(histograms, args, row_id_array, finaln, m, part_size, row_size, &case_c, &counter);

    /* Create the final pass */
    Histogram psum = create_psum(big_hist);
    Relation R = re_ordered(row_id_array, row_size, psum, counter);
    Pass pass1 = create_pass(R, psum);

    delete_hist(big_hist);
    delete_id_array(row_id_array, row_size);
/*------------------------------------------------------------------------------------*/
    // boolean case_c = false;
    // /* add the ids to the values */
    // row_id_array = create_id_array(row,row_size);
    // Pass pass1 = split(row_id_array, row_size, N_LOWER_BOUND, N_UPPER_BOUND, &case_c);
    // delete_id_array(row_id_array, row_size);

    /*** CASE c ~ Buckets of re ordered array cant fit in L2 cache ***/
    if(case_c){
        /* find the max */
        int max = parallel_max(pass1);
        Histogram hist = create_hist(max);

        /* split relation to NUM_THREADS parts and calculate small hists */
        finaln = calloc(NUM_THREADS, sizeof(int));
        histograms = malloc(sizeof(Histogram*) * NUM_THREADS);
        args = malloc(sizeof(HistArgs*) * NUM_THREADS);
        parallel_histograms(histograms, args, pass1->relation->array, finaln, part_size, pass1->relation->num_tuples, max, N_SECOND_PASS_UPPER_BOUND);

        /* create the big histogram */
        for(int i=0; i<hist->size; i++)
            for(int j=0; j<NUM_THREADS; j++)
                hist->array[i]->value += histograms[j]->array[i]->value;

        for(int i=0; i<NUM_THREADS; i++){
            delete_hist(histograms[i]);
            free(args[i]);
        }
        free(args);
        free(finaln);
        free(histograms);

        //int max = find_max_n(pass1);

        //Histogram hist = create_hist(max);

        // for(int i=0; i<pass1->relation->num_tuples; i++){
        //     int hash_value = hash(max, pass1->relation->array[i]->value);
        //     update_hist(hist, hash_value);
        // }

        Histogram psum = create_psum(hist);
        delete_hist(hist);
        
        Relation relation = re_ordered(pass1->relation->array, pass1->relation->num_tuples, psum, max);
        Pass pass2 = create_pass(relation, psum);

        return create_partition(pass1, pass2);

    }
    else{    
        return create_partition(pass1, NULL);
    }    
}

/* Takes an array and rearranges its elements to be in order according to buckets*/
Pass split(Tuple* row, int row_size, int n, int n_end, boolean* case_c){

    int max = 0;
    Histogram hist;
    boolean fits = false;           /* if data of largest bucket fits in L2 cache */
    
    /* for every value */
    while(!fits && n <= n_end){
        max = 0;
        hist = create_hist(n);
        fits = true;
        max=0;
        for(int i=0; i< row_size; i++){
            int hashed_value = hash(n, row[i]->value);
            update_hist(hist, hashed_value);
            
            if(hist->array[hashed_value]->value > max){
                /* update max value */
                max = hist->array[hashed_value]->value;
            }
            if((max > L2) && (n < n_end)){
                fits = false;
                
                delete_hist(hist);
                n++;
                
                break;
            }
        }
    }

    if(case_c != NULL && ((max > L2) && (n == n_end))){
        *case_c = true;
    }

    /* build prefix sum structure */
    Histogram psum = create_psum(hist);
    
    /* create reordered relation */
    Relation R = re_ordered(row, row_size, psum, n);
    Pass pass = create_pass(R, psum);

    delete_hist(hist);

    return pass;
}

Pass create_pass(Relation relation, Histogram psum){

    Pass pass = malloc(sizeof(*pass));
    pass->relation = relation;
    pass->psum = psum;

    return pass;
}

void delete_pass(Pass pass){

    delete_relation(pass->relation);

    //psum may be null if we have case (a) on partition
    if(pass->psum != NULL)
        delete_hist(pass->psum);
    free(pass);

    return;
}

Partition create_partition(Pass pass1, Pass pass2){

    Partition partition = malloc(sizeof(*partition));
    partition->basic_pass = pass1;
    partition->second_pass = pass2;

    return partition;
}

void delete_partition(Partition partition){

    //secondary_passages may be null if we have case (b) on partition
    if(partition->second_pass != NULL)
        delete_pass(partition->second_pass);
    
    delete_pass(partition->basic_pass);
    free(partition);

    return;
}

Tuple* create_id_array(uint64_t* row, int row_size){
    /* add the ids to the values */
    Tuple* row_id_array = malloc(sizeof(Tuple*) * row_size);
    for(int i=0; i<row_size; i++){
        row_id_array[i] = malloc(sizeof(struct tuple));
        row_id_array[i]->id = i;
        row_id_array[i]->value = row[i];
    }
    return row_id_array;
}

void delete_id_array(Tuple* array, int row_size){
    for(int i=0; i<row_size; i++)
        free(array[i]);
    free(array);
}
