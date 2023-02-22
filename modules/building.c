#include "partition.h"

int H_START = 8;

static void delete_chain_list(Pointer chain){
    List list = (List)chain;
    Node current_node = list->head;
    Node next_node;
    
    while(current_node != NULL){
        free(current_node->data);   /* free the tuple on list_node */

        next_node = current_node->next;
        free(current_node);         /* free list_node */
        current_node = next_node;
    }

    free(chain);                    /* free list */
}

//init a hash_cell {bitmap={0,0,..,0}, full = false}
HashCell create_hash_cell(int h){
    HashCell hash_cell = malloc(sizeof(*hash_cell));
    hash_cell->bitmap = calloc(h, sizeof(int));
    hash_cell->tuple = malloc(sizeof(struct tuple));    // malloc for first tuple
    hash_cell->chain = list_create(delete_chain_list,NULL);          // malloc for list of same values
    hash_cell->initialized = false;

    return hash_cell;
}

void delete_hash_cell(HashCell hash_cell){
    if(hash_cell != NULL){
        free(hash_cell->bitmap);
        free(hash_cell->tuple);
        /* TOADD delete_chain_list */
        hash_cell->chain->destroy_value(hash_cell->chain);

        free(hash_cell);
    }
}

HashTable create_hash_table(int h, int n_bits){
    HashTable hash_table = malloc(sizeof(*hash_table));
    hash_table->n = n_bits;
    hash_table->size = n_bits;
    hash_table->hash_table = malloc(sizeof(HashCell*) * (hash_table->size));
    hash_table->h = h;

    for(int i=0; i<hash_table->size; i++)
        hash_table->hash_table[i] = create_hash_cell(hash_table->h);
    
    return hash_table;
}

void delete_hash_table(HashTable hash_table){
    for(int i=0; i<hash_table->size; i++)
        delete_hash_cell(hash_table->hash_table[i]);
    free(hash_table->hash_table);
    
    free(hash_table);

    return;
}

//random hash function for ints
int hash2(int n, uint64_t value){
    value = ((value >> 16) ^ value) * 0x45d9f3b;
    value = ((value >> 16) ^ value) * 0x45d9f3b;
    value = (value >> 16) ^ value;
    return value % n;
}

/* returns hashtable doubled */
HashTable rehash(HashTable old_table){
    
    HashTable new_table = create_hash_table(old_table->h + old_table->h/2, 2*old_table->size);    
    
    /* for every element in old hashtable */
    for(int i=0; i<old_table->size; i++){
        if(old_table->hash_table[i]->initialized == true){
            /* if cell is not empty rehash value and insert to new hashtable */
            new_table = insert(old_table->hash_table[i]->tuple, new_table);
            Node current_node = old_table->hash_table[i]->chain->head;
                    
            while(current_node != NULL){
                Tuple node_data = (Tuple)current_node->data;
                
                /* if cell is not empty rehash value and insert to new hashtable */
                new_table = insert(node_data, new_table);

                current_node = current_node->next;
            }
        }
    }

    /*delete old table*/
    delete_hash_table(old_table);

    return new_table;
}

HashTable* build_hash_table_a(Partition partition){
    HashTable* hash_tables = malloc(sizeof(HashTable*));
    hash_tables[0] = create_hash_table(H_START, partition->basic_pass->relation->n_bits * 2);

    for(int i=0; i<partition->basic_pass->relation->num_tuples; i++){
        hash_tables[0] = insert(partition->basic_pass->relation->array[i], hash_tables[0]);
    }

    return hash_tables;
}

HashTable* build_hash_table_b(Partition partition){

    HashTable* hash_tables = malloc(sizeof(HashTable*) * partition->basic_pass->psum->size);

    for(int i=0; i<partition->basic_pass->psum->size; i++){
        int start = partition->basic_pass->psum->array[i]->value;
        int end;
        
        if( i == (partition->basic_pass->psum->size - 1))
            end = partition->basic_pass->relation->num_tuples; 
        else
            end = partition->basic_pass->psum->array[i+1]->value;
        
        hash_tables[i] = create_hash_table(H_START, partition->basic_pass->relation->n_bits * 2);
        for(int j=start; j<end; j++){
            hash_tables[i] = insert(partition->basic_pass->relation->array[j], hash_tables[i]);
        }
    }

    return hash_tables;
}

HashTable* build_hash_table_c(Partition partition){
    HashTable* hash_tables = malloc(sizeof(HashTable*) * partition->second_pass->psum->size);
    for(int i=0; i<partition->second_pass->psum->size; i++){
        int start = partition->second_pass->psum->array[i]->value;
        int end;
        
        if( i == (partition->second_pass->psum->size - 1))
            end = partition->second_pass->relation->num_tuples; 
        else
            end = partition->second_pass->psum->array[i+1]->value;
        
        hash_tables[i] = create_hash_table(H_START, partition->second_pass->relation->n_bits * 2);
        for(int j=start; j<end; j++){
            hash_tables[i] = insert(partition->second_pass->relation->array[j], hash_tables[i]);
        }
    }

    return hash_tables;
}

/* function that checks if given tuple already exists in hash_table. If yes, adds it on chain list and returns true - returns false otherwise */
static boolean duplicate_check(HashTable hash_table, int j, Tuple x){
    if(hash_table->hash_table[j]->initialized){
        Tuple first = hash_table->hash_table[j]->tuple; // tuple of cell

        /* check if given tuple has the same value as the tuple of cell */
        if(first->value == x->value){
            /* if so add it on chain list and return */
            Tuple tuple = malloc(sizeof(*tuple));
            tuple->id = x->id;
            tuple->value = x->value;
            
            add(tuple, hash_table->hash_table[j]->chain);

            return true;
        }
    }

    return false;
}

/* Guarantees that an entry will always be found in the neighborhood of its initial bucket */
HashTable insert(Tuple x, HashTable hash_table){
    int i = hash2(hash_table->n, x->value); /* find where x hashes */

    /** check if we have a value in neighborhood that is the same as x->value if so add it on list **/

    /* for neighborhood right of i (i+h < hash_table->size) that fits in table size */
    /*              [,,,,,,,,,,,,,,,,,,,,,,(i),,,,,,,,,,,,,,,,,,(h+i)] size             */
    if((i+hash_table->h) < hash_table->size){
        for(int j=i; j<hash_table->h+i; j++){
            if(duplicate_check(hash_table, j, x))
                return hash_table;
        }
    }
    /* for neighborhood right of i (i+h >= hash_table->size) that doesnt fit in table size */
    /*              [,,,(end of neighborhood),,,,,,,,,,,,,,,,,,,(i),,,,,,,,,,,,,,] size             */
    else{
        /* counter for how many slots we have left to iterate over in the beginning of hash table */
        int slots = hash_table->h;
        /* we iterate until we reach the size of hash table */
        for(int j=i; j<hash_table->size; j++){
            if(duplicate_check(hash_table, j, x))
                return hash_table;
            slots--;
        }
        for(int j=0; j<slots; j++){
            if(duplicate_check(hash_table, j, x))
                return hash_table;
        }
    }
    


    /* 1. Go to hash_table[i] and check bitmap */
    boolean empty_pos = true;
    for(int j=0; j<hash_table->h; j++){
        /* if bitmap[j] = true => j pos is occupied by element that hashes in i */
        if(!hash_table->hash_table[i]->bitmap[j]){
            empty_pos = false;
            break;
        }
    }

    /* 1. if every one of those H cells is occupied by elements that hash in i empty_pos = true */
    if(empty_pos == true){
        hash_table = rehash(hash_table);
        hash_table = insert(x, hash_table);

        return hash_table;
    }

    /* 2. Starting from pos i */
    int j = -1;
    for(int l=i; l<hash_table->size; l++){
        if(hash_table->hash_table[l]->initialized == false){
            j = l;  /* find empty pos j */
            break;
        }
    }
    /* if there is no empty position right of i */
    if(j == -1){
        /* search from the beginning */
        for(int l=0; l<i; l++){
            if(hash_table->hash_table[l]->initialized == false){
                j = l;  /* find empty pos j */
                break;
            }
        }
    }
    /* if there is still no empty position hash table is full */
    if(j == -1){
        hash_table = rehash(hash_table);
        hash_table = insert(x, hash_table);
        return hash_table;
    }

    int k = -1;
    int y_pos = -1;
    
    /* if empty bucket is far away from the neighborhood of i we need to swap buckets to bring it closer  */
    boolean in_while = false;
    /* empty pos left of i */
    if(j-i < 0){
        /* neighborhood doesnt fit right of i */
        if(i+hash_table->h > hash_table->size){
            int right = hash_table->size - i; /* slots of neighborhood right of i */
            int left = hash_table->h - right; /* slots of neighborhood left to check from the beginning of array */
            if( j > left - 1){
                in_while = true;
            }
        }
        /* neighborhood right of i and empty pos left */
        else{
            in_while = true;
        }
    }

    /* while empty bucket j is not in neighborhood of i */
    while(((j-i) % hash_table->size >= hash_table->h) || in_while){
        y_pos = -1;
        /* 3. search the H-1 pos preceding j for an element y where k=hash(y) and (j-k) mod n < H */
        if(j-(hash_table->h-1) >= 0){
            for(int l=(j-(hash_table->h-1)); l<j; l++){
                if(hash_table->hash_table[l]->initialized == true){
                    k = hash2(hash_table->n, hash_table->hash_table[l]->tuple->value);
                    if(((j-k % hash_table->size) < hash_table->h) && (j-k > 0)){
                        y_pos = l;
                        break;
                    }
                }
            }
        }
        
        /* 3. If there is no y hash table is full */
        if(y_pos == -1){
            hash_table = rehash(hash_table);
            hash_table = insert(x, hash_table);
            return hash_table;
        }

        /* 3. Move y to j(empty) creating an empty slot near i */
        hash_table->hash_table[j]->tuple->id = hash_table->hash_table[y_pos]->tuple->id;        /* tuple of j now has the row id of tuple y */
        hash_table->hash_table[j]->tuple->value = hash_table->hash_table[y_pos]->tuple->value;  /* tuple of j now has the value of tuple y  */
        /* We must now change the chain lists */
        List temp = hash_table->hash_table[j]->chain;                                           /*    temp -> j(empty)                      */
        hash_table->hash_table[j]->chain = hash_table->hash_table[y_pos]->chain;                /*    j -> y(not empty)                     */
        hash_table->hash_table[y_pos]->chain = temp;                                            /*    y -> temp(empty)                      */
        hash_table->hash_table[j]->initialized = true;                                          /*    j is now initialized                  */
        hash_table->hash_table[y_pos]->initialized = false;                                     /*    y_pos is now empty                    */
       
        /* 3. update bitmaps of j and y_pos */
        int my_pos = j-hash_table->h+1;
        if(my_pos >= 0){
            for(int p=my_pos; p<=j; p++){
                update_bitmap(hash_table, p);
            }
        }
        else{
            int myslots = hash_table->h;
            my_pos = hash_table->size + my_pos;
            for(int p=my_pos; p<hash_table->size; p++){
                update_bitmap(hash_table, p);
                myslots--;
            }
            for(int p=0; p<myslots; p++){
                update_bitmap(hash_table, p);
            }
        }
                
        /* 3. give j the value of the empty slot y and repeat */
        j = y_pos;

        in_while = false;
        if(j-i < 0){
            /* neighborhood doesnt fit right of i */
            if(i+hash_table->h > hash_table->size){
                int right = hash_table->size - i; /* slots of neighborhood right of i */
                int left = hash_table->h - right; /* slots of neighborhood left to check from the beginning of array */
                if( j > left - 1){
                    in_while = true;
                }
            }
            else{
                in_while = true;
            }
        }
    }

    /* 4. save x on j pos and return table */
    /* since we have the duplicate check in the beginning of insert we will reach this point only for non-existing values in hash_table
    therefore there is no reason to check something on chain list */
    hash_table->hash_table[j]->tuple->id = x->id;
    hash_table->hash_table[j]->tuple->value = x->value;
    hash_table->hash_table[j]->initialized = true;
    
    update_bitmap(hash_table, i);

    return hash_table;
}

HashTable update_bitmap(HashTable hash_table, int pos){
    int bitmap_counter = 0;
    
    /* search pos - pos + H */
    if(pos+hash_table->h < hash_table->size){
        for(int i=pos; i<pos+hash_table->h; i++){
            /* if slot is empty or if value in slot doesnt hash in pos, bitmap = false */
            if((hash_table->hash_table[i]->initialized == false) || (hash2(hash_table->n, hash_table->hash_table[i]->tuple->value) != pos)){
                hash_table->hash_table[pos]->bitmap[bitmap_counter] = 0;
            }
            else{
                hash_table->hash_table[pos]->bitmap[bitmap_counter] = 1;
            }
            bitmap_counter++;
        }
    }
    else{
        /* we search for H slots */
        int slots = hash_table->h;          // [,i,,,pos,] 
        /* reach the end of hash table */
        for(int i=pos; i<hash_table->size; i++){
            if((hash_table->hash_table[i]->initialized == false) || (hash2(hash_table->n, hash_table->hash_table[i]->tuple->value) != pos)){
                hash_table->hash_table[pos]->bitmap[bitmap_counter] = 0;
            }
            else{
                hash_table->hash_table[pos]->bitmap[bitmap_counter] = 1;
            }
            slots--;    /* we still have (slots - 1) slots to search for this neighborhood */
            bitmap_counter++;
        }
        for(int i=0; i<slots; i++){
            if((hash_table->hash_table[i]->initialized == false) || (hash2(hash_table->n, hash_table->hash_table[i]->tuple->value) != pos)){
                hash_table->hash_table[pos]->bitmap[bitmap_counter] = 0;
            }
            else{
                hash_table->hash_table[pos]->bitmap[bitmap_counter] = 1;
            }
            bitmap_counter++;
        }
    }
    
    return hash_table;
}

enum cases find_case(Partition partition){
    //case a
    if(partition->basic_pass->psum == NULL)
        return case_a;
    
    //case b
    else if(partition->second_pass == NULL)
        return case_b;
    
    //case c
    else
        return case_c;
}

Build build_hash_table(Partition left, Partition right, boolean DEBUG){

    if(DEBUG){
        H_START = 2;
    }

    Build build = malloc(sizeof(*build));
    build->partition_hash = left;
    build->partition_search = right;
    build->search_case = find_case(right);
    build->build_case = find_case(left);

    //case a
    if(build->build_case == case_a)
        build->hash_table = build_hash_table_a(left);

    //case b
    else if(build->build_case == case_b)
        build->hash_table = build_hash_table_b(left);

    //case c
    else
        build->hash_table = build_hash_table_c(left);
    

    return build;
}

void delete_build(Build build){
    if(build->build_case == case_a){
        delete_hash_table(build->hash_table[0]);
    }
    else if(build->build_case == case_b){
        for(int i=0; i<build->partition_hash->basic_pass->psum->size; i++)
            delete_hash_table(build->hash_table[i]);
    }
    else{
        for(int i=0; i<build->partition_hash->second_pass->psum->size; i++)
            delete_hash_table(build->hash_table[i]);        
    }
    free(build->hash_table);
    free(build);
}

/* returns array of tuples that match */
List find(HashTable hash_table, uint64_t value){

    List tuples = list_create(delete_chain_list, NULL);  /* List of tuples with the same value */
    
    int hashed_value = hash2(hash_table->n, value);
    /* condition must be > and not >= */
    if(hash_table->size > hashed_value + hash_table->h - 1 ){
        /* neighborhood of value is in the right of hashvalue */
        
        for(int i=0; i<hash_table->h; i++){
            if(hash_table->hash_table[hashed_value]->bitmap[i]){      /* search only if corresponding bitmap value is 1 */
                /* check if cells tuple->value is the same as given value */
                if(hash_table->hash_table[hashed_value+i]->tuple->value == value){
                    /* add cell tuple on list of tuples */
                    Tuple first = malloc(sizeof(struct tuple));
                    first->id = hash_table->hash_table[hashed_value+i]->tuple->id;
                    first->value = hash_table->hash_table[hashed_value+i]->tuple->value;
                    add(first, tuples);

                    /* add duplicates-if exist on tuples list */
                    Node current_node = hash_table->hash_table[hashed_value+i]->chain->head;
                    
                    while(current_node != NULL){
                        Tuple node_data = (Tuple)current_node->data;
                        
                        Tuple inserted = malloc(sizeof(struct tuple));
                        inserted->id = node_data->id;
                        inserted->value = node_data->value;
                        add(inserted, tuples);

                        current_node = current_node->next;
                    }       
                    return tuples;             
                }
            }
        }
    }
    else{
        /* neighborhood is in the end and the beginning of hash_table */
        int right = hash_table->size - hashed_value;        /* number of cells in the end of the array */

        for(int i=0; i<hash_table->h; i++){
            if(hash_table->hash_table[hashed_value]->bitmap[i]){
                if(i < right){
                    /* value is at the end */
                    if(hash_table->hash_table[hashed_value+i]->tuple->value == value){
                        /* add cell tuple on list of tuples */
                        Tuple first = malloc(sizeof(struct tuple));
                        first->id = hash_table->hash_table[hashed_value+i]->tuple->id;
                        first->value = hash_table->hash_table[hashed_value+i]->tuple->value;
                        add(first, tuples);

                        Node current_node = hash_table->hash_table[hashed_value+i]->chain->head;

                        /* add duplicates - if exist - on tuples list */
                        while(current_node != NULL){
                            Tuple node_data = (Tuple)current_node->data;
                            
                            Tuple inserted = malloc(sizeof(struct tuple));
                            inserted->id = node_data->id;
                            inserted->value = node_data->value;
                            add(inserted, tuples);

                            current_node = current_node->next;
                        }
                        return tuples; 
                    }
                }
                else{
                    /* value at the start */
                    if(hash_table->hash_table[i-right]->tuple->value == value){
                        Tuple first = malloc(sizeof(struct tuple));
                        first->id = hash_table->hash_table[i-right]->tuple->id;
                        first->value = hash_table->hash_table[i-right]->tuple->value;
                        add(first, tuples);

                        Node current_node = hash_table->hash_table[i-right]->chain->head;
                    
                        while(current_node != NULL){
                            Tuple node_data = (Tuple)current_node->data;
                            
                            Tuple inserted = malloc(sizeof(struct tuple));
                            inserted->id = node_data->id;
                            inserted->value = node_data->value;
                            add(inserted, tuples);

                            current_node = current_node->next;
                        }

                        return tuples; 
                    }
                }
            }
        }
    }
    /* returns empty list if not found */
    return tuples;
}
