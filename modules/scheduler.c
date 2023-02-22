#include "scheduler.h"

Scheduler scheduler;

void* standby(void* args){

    while(true){
        //try lock the list to get a task
        pthread_mutex_lock(&scheduler->list_mutex);

        //block until a task is in the list
        while(scheduler->job_list->size == 0)
            pthread_cond_wait(&scheduler->cond_list_nonempty, &scheduler->list_mutex);
        //pop the task from the list
        Node node = pop(scheduler->job_list);

        //unlock the mutex so other threads can access the list
        pthread_mutex_unlock(&scheduler->list_mutex);

        //run the task
        //first cast the node as Job_list_node
        Job_list_node data = (Job_list_node)(node->data);
        //take the task function
        Task task = (Task)(data->task);
        
        //then call funtion with args
        if(task((Pointer)data->task_args) == -1){
            free(data);
            free(node);
            break;
        }

        //inform the scheduler that the job is done
        pthread_mutex_lock(&scheduler->jobs_mutex);
        scheduler->jobs_todo--;

        //if all the jobs are done send signal to the barrier
        if(scheduler->jobs_todo == 0)
            pthread_cond_signal(&scheduler->cond_jobs_done);
        pthread_mutex_unlock(&scheduler->jobs_mutex);

        free(data);
        free(node);
    }
    
    pthread_exit(0);
}

void init_scheduler(){
    scheduler = malloc(sizeof(*scheduler));
    scheduler->threads = malloc(sizeof(Scheduler_node*) * NUM_THREADS);
    scheduler->job_list = list_create(free, NULL);
    scheduler->jobs_todo = 0;

    pthread_mutex_init(&scheduler->list_mutex, NULL);
    pthread_mutex_init(&scheduler->jobs_mutex, NULL);
    pthread_cond_init(&scheduler->cond_list_nonempty, NULL);
    pthread_cond_init(&scheduler->cond_jobs_done, NULL);

    for(int i=0; i<NUM_THREADS; i++){
        scheduler->threads[i] = malloc(sizeof(Scheduler_node));
        pthread_create(&scheduler->threads[i]->thread, NULL, standby, (void*)scheduler->threads[i]);
    }

}

void serve_task(Task task, Pointer task_args){
    pthread_mutex_lock(&scheduler->list_mutex);

    Job_list_node node = malloc(sizeof(*node));
    node->task = task;
    node->task_args = task_args;
    add(node, scheduler->job_list);
    pthread_mutex_lock(&scheduler->jobs_mutex);
    scheduler->jobs_todo++;
    pthread_mutex_unlock(&scheduler->jobs_mutex);

    pthread_cond_signal(&scheduler->cond_list_nonempty);
    pthread_mutex_unlock(&scheduler->list_mutex);
}

int cleanup(Pointer value){
    return -1;
}

static void wait_threads(){
    for(int i=0; i<NUM_THREADS; i++){
        serve_task(cleanup, NULL);
    }

    for(int i=0; i<NUM_THREADS; i++){
        pthread_join(scheduler->threads[i]->thread, NULL);
    }
}

void barrier(){
    //waits until all the jobs in the list have finished
    pthread_mutex_lock(&scheduler->jobs_mutex);
    while(scheduler->jobs_todo != 0)
        pthread_cond_wait(&scheduler->cond_jobs_done, &scheduler->jobs_mutex);

    pthread_mutex_unlock(&scheduler->jobs_mutex);
    return;
}

void destroy_scheduler(){

    wait_threads();

    for(int i=0; i<NUM_THREADS; i++){
        free(scheduler->threads[i]);
    }

    scheduler->job_list->destroy_value(scheduler->job_list);
    pthread_mutex_destroy(&scheduler->list_mutex);
    pthread_mutex_destroy(&scheduler->jobs_mutex);
    pthread_cond_destroy(&scheduler->cond_list_nonempty);
    pthread_cond_destroy(&scheduler->cond_jobs_done);
    free(scheduler->threads);
    free(scheduler);
}