#include "predicatesQueue.h"

//returns array number and column number
static void split_part(char* part, int* number_of_array, int* number_of_column){

    char* temp = malloc(strlen(part)+1);
    strcpy(temp, part);

    char* pos = strchr(temp, '.');
    *number_of_column = atoi(pos+1);

    char* str;
    *number_of_array = atoi(strtok_r(temp, ".", &str));
    
    free(temp);
    return;
}

//does NOT aquire memory for each string
PredicateNode predicate_node_create(char* left_part, char op, char* right_part){
    PredicateNode predicate_node = malloc(sizeof(*predicate_node));
    predicate_node->left_part = left_part;
    predicate_node->op = op;
    predicate_node->right_part = right_part;
    
    int array1, array2, col1, col2;
    split_part(left_part, &array1, &col1);

    if(strchr(right_part, '.') == NULL)
        predicate_node->filter = true;                
    else{
        //right part is not number
        split_part(right_part, &array2, &col2);
        if(array1 == array2){
            //it is a filter if join is on same array
            predicate_node->filter = true; 
        }else{
            predicate_node->filter = false;
        }
    }
    return predicate_node;
}

void predicate_node_delete(PredicateNode node){
    free(node->left_part);
    free(node->right_part);
    free(node);

    return;
}

PredicatesQueue predicates_queue_create(DestroyFunc destroy_value, PrintFunc print_value){
    return (PredicatesQueue)list_create(destroy_value, print_value);
}

void predicates_queue_add(PredicateNode data, PredicatesQueue queue){
    List list = (List)queue;
    // Αδεια λίστα
    if(list->head == NULL){
        list->head = malloc(sizeof(*list->head));
        list->head->data = data;
        list->head->next = NULL;
        list->last = list->head;
    }
    else{
        // Αμα η λίστα έχει μόνο ένα στοιχείο
        if(list->head==list->last){
            list->last = malloc(sizeof(*list->last));

            //ελεγχουμε αν ειναι φιλτρο η join
            if(data->op == '='){
                //Περιπτωση οπου εχουμε φιλτρο με '='
                if(data->filter){
                    list->last->data = list->head->data;
                    list->last->next = NULL;
                    list->head->data = data;
                    list->head->next = list->last;                    
                }
                else{
                    list->last->data = data;
                    list->last->next = NULL;
                    list->head->next = list->last;
                }
            }
            //Αλλιως στην αρχή
            else{
                list->last->data = list->head->data;
                list->last->next = NULL;
                list->head->data = data;
                list->head->next = list->last;
            }
            
        }
        else{
            Node new_node = malloc(sizeof(*new_node));
            new_node->data = data;

            //παρόμοια
            if(data->op == '='){
                if(data->filter){
                    new_node->next = list->head;
                    list->head = new_node;
                }
                else{
                    new_node->next = NULL;
                    list->last->next = new_node;
                    list->last = new_node;
                }
            }
            else{
                new_node->next = list->head;
                list->head = new_node;
            } 
        }

    }
    list->size++;
}

PredicateNode predicates_queue_pop(PredicatesQueue queue){
    PredicateNode data_to_return = NULL;

    if(queue->head != NULL){
        Node tnode = queue->head;

        data_to_return = tnode->data;
        queue->head = tnode->next;
        queue->size --;
        
        free(tnode);
    }

    return data_to_return;
}

void delete_pq(Pointer pq){

    Node current_node = ((List)pq)->head;
    Node next_node;

    PredicateNode pq_node;
    while(current_node != NULL){
        pq_node = current_node->data;
        predicate_node_delete(pq_node);
        next_node = current_node->next;

        free(current_node);
        current_node = next_node;
    } 
    free(pq);

    return;
}

void print_pq(Pointer pq){
    Node current_node = ((List)pq)->head;
    Node next_node;
    PredicateNode pq_node;
    while(current_node != NULL){
        pq_node = current_node->data;
        printf("left: %s op: %c right: %s\n", pq_node->left_part, pq_node->op, pq_node->right_part);
        next_node = current_node->next;
        current_node = next_node;
    } 
    
    return;
}