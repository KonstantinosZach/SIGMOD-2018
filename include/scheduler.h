#pragma once
#include <pthread.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>                                                           
#include <signal.h>

#include "data_structures.h"

#define NUM_THREADS 6

typedef int (*Task)(Pointer value);

struct scheduler_node{
    pthread_t thread;
};

typedef struct scheduler_node* Scheduler_node;

struct job_list_node{
    Task task;
    Pointer task_args;
};
typedef struct job_list_node* Job_list_node;

struct scheduler{
    Scheduler_node* threads;
    List job_list;  //list with the tasks
    int jobs_todo;  //number of jobs to do

    pthread_mutex_t list_mutex; //mutex to access the list
    pthread_mutex_t jobs_mutex; //mutex to access the counter jobs_todo

    pthread_cond_t cond_list_nonempty;  //cond_var to inform the threads that a job is added in the list
    pthread_cond_t cond_jobs_done;      //cond_var to inform the barrier that all jobs have finished
};
typedef struct scheduler* Scheduler;

void init_scheduler();

void serve_task(Task task, Pointer task_args);

void barrier();

void destroy_scheduler();