#include "join.h"

uint64_t* get_row(uint64_t** array, int size, int col){
    uint64_t* row = malloc(sizeof(uint64_t) * size);
    for(int i=0; i<size; i++){
        row[i] = array[i][col];
    }

    return row;
}

Join create_join(int** final, uint64_t** R, int r_colsize, uint64_t** S, int s_colsize, int final_rows){
    Join join = malloc(sizeof(*join));
    
    join->array = malloc(sizeof(int*)*final_rows);

    for(int i=0; i<final_rows; i++){
        join->array[i] = malloc(sizeof(int)*(s_colsize + r_colsize));

        for(int j=0; j<r_colsize; j++)
            join->array[i][j] = R[final[i][0]][j];
        
        for(int k=0; k<s_colsize; k++)
            join->array[i][r_colsize + k] = S[final[i][1]][k];
    }
    
    join->row_size = final_rows;
    if(final_rows == 0)
        join->col_size = 0;
    else
        join->col_size = r_colsize + s_colsize;

    return join;
}

void delete_join(Join joined){
    for(int i=0; i<joined->row_size; i++)
        free(joined->array[i]);
    free(joined->array);
    free(joined);
}

/*arrays must be initialized*/
Join join_hash_function(uint64_t** R, int r_rowsize, int r_colsize, int r_col, uint64_t** S, int s_rowsize, int s_colsize, int s_col){
    uint64_t* r_row = get_row(R, r_rowsize, r_col);
    uint64_t* s_row = get_row(S, s_rowsize, s_col);

    /* partitioning */
    Partition partition1 = partitioning(r_row, r_rowsize, false);
    Partition partition2 = partitioning(s_row, s_rowsize, false);
    /* building */
    Build build = build_hash_table(partition1, partition2, false);

    /* probing */
    int final_rows = 0;
    List list = probing(build, &final_rows);

    int** final = final_id_array(list, final_rows);

    /* join */
    Join join = create_join(final, R, r_colsize, S, s_colsize, final_rows);
   
    free(r_row);
    free(s_row);

    delete_final_id_array(final, final_rows);
    list->destroy_value(list);
    delete_build(build);
    delete_partition(partition1);
    delete_partition(partition2);

    return join;
}

void join_hash_different_relation(MappedRelation mapR, int r_col_num, uint64_t* R_values, int Rfilter_size, MappedRelation mapS, int s_col_num, uint64_t* S_values, int Sfilter_size, int* final_rows, int*** id_array){
    
    int sizeR = mapR->rows;
    int sizeS = mapS->rows; 
    uint64_t* r_col;
    if(R_values == NULL){
        size_t startR, endR;
        r_col = malloc(sizeof(uint64_t)*mapR->rows);
        uint64_t* dataR = (uint64_t*)mapR->addr;
        get_col(mapR, r_col_num, &startR, &endR);
    
        int count = 0;
        for (size_t i = startR; i < endR; ++i){
            r_col[count] = dataR[i];
            count++;
        }
    }
    else{
        r_col = R_values;
        sizeR = Rfilter_size; 
    }

    uint64_t* s_col;
    if(S_values == NULL){
        size_t startS, endS;
        s_col =  malloc(sizeof(uint64_t)*mapS->rows);
        uint64_t* dataS = (uint64_t*)mapS->addr;
        get_col(mapS, s_col_num, &startS, &endS);

        int count = 0;
        for (size_t i = startS; i < endS; ++i){
            s_col[count] = dataS[i];
            count++;
        }
    }
    else{
        s_col = S_values;
        sizeS = Sfilter_size;
    }
    /* partitioning */
    Partition partition1 = partitioning(r_col, sizeR, false);
    Partition partition2 = partitioning(s_col, sizeS, false);
    /* building */
    Build build = build_hash_table(partition1, partition2, false);

    /* probing */
    *final_rows = 0;
    List list = probing(build, final_rows);

    *id_array = final_id_array(list, *final_rows);


    list->destroy_value(list);
    delete_build(build);
    delete_partition(partition1);
    delete_partition(partition2);

    //remove that
    free(r_col);
    free(s_col);

    return;
}
