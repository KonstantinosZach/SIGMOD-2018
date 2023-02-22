#include "data_structures.h"

List list_create(DestroyFunc destroy_func, PrintFunc print_value){
    List list = malloc(sizeof(*list));
    list->head = NULL;
    list->last = NULL;
    list->size = 0;
    list->destroy_value = destroy_func;
    list->print_value = print_value;
    return list;
}

void add(Pointer data, List list){
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
            list->last->data = data;
            list->last->next = NULL;
            list->head->next = list->last;
        }
        else{
            // Αμα η λίστα έχει παραπάνω από ένα στοιχείο
            Node new_node = malloc(sizeof(*new_node));
            new_node->data = data;
            new_node->next = NULL;
            list->last->next = new_node;
            list->last = new_node;    
        }
    }
    
    list->size++;
}

Node pop(List list){
    Node node = list->head;
    list->head = list->head->next;
    list->size--;
    return node;
}
