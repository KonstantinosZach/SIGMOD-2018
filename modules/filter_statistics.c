#include "filter_statistics.h"

void calculate_statistics(MappedRelation mappedRelation){
    size_t start, end;
    uint64_t n = 50000000;
    for(uint64_t i=0; i<mappedRelation->cols; i++){
        get_col(mappedRelation, i, &start, &end);
        
        uint64_t* data = (uint64_t*)mappedRelation->addr;
        uint64_t min, max;
        min = data[start];
        max = data[start];

        for(size_t j=start; j<end; j++){
            if(data[j] < min){
                min = data[j];
            }
            else if(data[j] > max){
                max = data[j];
            }
        }
        mappedRelation->statistics[i] = malloc(sizeof(struct stats));
        mappedRelation->statistics[i]->l = min;
        mappedRelation->statistics[i]->u = max;
        mappedRelation->statistics[i]->f = mappedRelation->rows;

        /* malloc the boolean table */
        uint64_t size = (max-min+1);
        boolean big = false;
        if(size > n){
            size = n;
            big = true;
        }
        char* array = malloc(sizeof(*array)*size);
        memset(array, '0', sizeof(*array)*size);
        int distinct = 0;
        for(size_t j=start; j<end; j++){
            if(!big){
                if(array[data[j] - min] == '0')
                    distinct++;
                array[data[j] - min] = '1';
            }
            else{
                if(array[(data[j] - min) % n] == '0')
                    distinct++;
                array[(data[j] - min) % n] = '1';
            }
        }
        mappedRelation->statistics[i]->d = distinct;
        free(array);
    }
}

/* initialize statistics from mappings to filter data structure */
void initialize_statistics(IntermediateResultsFilter filter, MappedRelation map){
    for(int i=0; i<map->cols; i++){
        filter->stats[i]->d =  map->statistics[i]->d;
        filter->stats[i]->u =  map->statistics[i]->u;
        filter->stats[i]->l =  map->statistics[i]->l;
        filter->stats[i]->f =  map->statistics[i]->f;
    }
    filter->filtered = true;
      
    return;
}

static void equality_statistics(IntermediateResultsFilter* filters, int number_of_array, int column, MappedRelation map, PredicateNode node){
    IntermediateResultsFilter filter = filters[number_of_array];

    filter->stats[column]->l = strtol(node->right_part, NULL, 10); 
    filter->stats[column]->u = strtol(node->right_part, NULL, 10);

    int initial_f = filter->stats[column]->f;
    
    /* if k not in dA */
    if(filter->size_filter_array == 0){
        filter->stats[column]->d = 0;
        filter->stats[column]->f = 0;
    }else{
        //use previous values from filter stats.
        filter->stats[column]->f = filter->stats[column]->f / filter->stats[column]->d;
        filter->stats[column]->d = 1;
    }
    //update other columns stats
    for(int i=0; i<map->cols; i++){
        if(i != column){
            if(filter->stats[i]->d != 0)
                filter->stats[i]->d = filter->stats[i]->d * (1-pow(1- filter->stats[column]->f/initial_f, filter->stats[i]->f/filter->stats[i]->d));
            filter->stats[i]->f = filter->stats[column]->f;
        }
    }
}

//finds maximum of 2 numbers
static uint64_t max(uint64_t a, uint64_t b){
    if(a > b)
        return a;
    return b;
}

//finds minimum of 2 numbers
static uint64_t min(uint64_t a, uint64_t b){
    if(a < b)
        return a;
    return b;
}

static void same_array_statistics(IntermediateResultsFilter* filters, int number_of_array, int col1, int col2, MappedRelation map, PredicateNode node){
    IntermediateResultsFilter filter = filters[number_of_array];
    
    uint64_t initial_f = filter->stats[col1]->f;

    /*calculate lower statistic*/
    filter->stats[col1]->l = max(filter->stats[col1]->l, filter->stats[col2]->l);
    filter->stats[col2]->l = filter->stats[col1]->l;
    /*calculate upper statistic*/
    filter->stats[col1]->u = min(filter->stats[col1]->u, filter->stats[col2]->u);
    filter->stats[col2]->u = filter->stats[col1]->u;

    uint64_t n =  filter->stats[col1]->u - filter->stats[col1]->l + 1;
    /*calculate frequency statistic*/
    filter->stats[col1]->f = filter->stats[col1]->f / n;
    filter->stats[col2]->f = filter->stats[col1]->f;
    /*calculate distinct statistic*/
    filter->stats[col1]->d = filter->stats[col1]->d * (1.0-pow(1-((float)filter->stats[col1]->f/(float)initial_f), ((float)initial_f/(float)filter->stats[col1]->d)));
    filter->stats[col2]->d = filter->stats[col1]->d;

    for(int i=0; i<map->cols; i++){
        if(i != col1 && i!= col2){    
            filter->stats[i]->d = filter->stats[i]->d * (1-pow(1-((float)filter->stats[col1]->f/(float)initial_f), ((float)filter->stats[i]->f/(float)filter->stats[i]->d)));
            filter->stats[i]->f = filter->stats[col1]->f;
        }
    }
}

static void range_statistics(IntermediateResultsFilter* filters, int number_of_array, int column, MappedRelation map, PredicateNode node){
    IntermediateResultsFilter filter = filters[number_of_array];
    long k = strtol(node->right_part, NULL, 10);

    //first we check whether k is inside the limits, if not we change it

    if(k < filter->stats[column]->l)
        k = filter->stats[column]->l;
    if(k > filter->stats[column]->u)
        k = filter->stats[column]->u;

    int initial_f = filter->stats[column]->f;

    //case where op ">"
    //example: 0.1 > 200 <=> 200 < 0.1 < max where max is the upper of 0.1
    if(node->op == '>'){
        //distinct
        filter->stats[column]->d = (filter->stats[column]->u - k) * filter->stats[column]->d / (filter->stats[column]->u - filter->stats[column]->l);
        //f
        filter->stats[column]->f = (filter->stats[column]->u - k)  * filter->stats[column]->f / (filter->stats[column]->u - filter->stats[column]->l);

        //lower
        filter->stats[column]->l = k;

        //upper stays same
    }

    //case where op "<"
    //example: 0.1 < 200 <=> min < 0.1 < 200 where min is the lower of 0.1
    else{

        //distinct
        filter->stats[column]->d = (k - filter->stats[column]->l) * filter->stats[column]->d / (filter->stats[column]->u - filter->stats[column]->l);
        
        //f
        filter->stats[column]->f = (k - filter->stats[column]->l) * filter->stats[column]->f / (filter->stats[column]->u - filter->stats[column]->l);

        //upper is updated from query
        filter->stats[column]->u = k;

        //lowers stays same
    }

    //then we update the stats for the other columns
    for(int i=0; i<map->cols; i++){
        //lower stays same
        //upper stays same

        if(i != column){
            //distinct
            if(filter->stats[i]->d != 0)
                filter->stats[i]->d = filter->stats[i]->d * (1-pow(1 - filter->stats[column]->f/initial_f, filter->stats[i]->f/filter->stats[i]->d));
            //freq
            filter->stats[i]->f = filter->stats[column]->f;
        }
    }

}

void update_filter_statistics(List list_mapping, int* array_value, IntermediateResultsFilter* filters, PredicateNode node){
    int number_of_array, column, col2;
    split_part(node->left_part, &number_of_array, &column);

    MappedRelation map = get_map(list_mapping, array_value, number_of_array);
    
    if(node->op == '='){
        if(strchr(node->right_part, '.') == NULL){
            equality_statistics(filters, number_of_array, column, map, node);
        }else{
            split_part(node->right_part,&number_of_array, &col2);
            same_array_statistics(filters, number_of_array, column, col2, map, node);
        }
    }else{
        range_statistics(filters, number_of_array, column, map, node);
    }
}
