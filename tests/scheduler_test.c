#include "acutest.h"
#include "scheduler.h"

int task_add_one(Pointer value){
    int* new = (int*)value;
    *new = *new + 1;
    return 0;
};

void scheduler_test(void){
    init_scheduler();

    int** array = malloc(sizeof(int*) * 10);
    for(int i=0; i<10; i++){

        int* j = malloc(sizeof(int));
        *j = i;
        array[i] = j;

        serve_task(task_add_one, (Pointer)j);
        barrier();
        
        //check that the task added one to the pointer value
        TEST_CHECK(*j == i+1);
    }

    barrier();

    for(int i=0; i<10; i++)
        free(array[i]);
    free(array);

    destroy_scheduler();

}

TEST_LIST = {
    { "scheduler_test", scheduler_test},
	{ NULL, NULL }
};
