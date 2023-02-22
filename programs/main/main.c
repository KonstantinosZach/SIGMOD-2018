#include "init.h"
#include "scheduler.h"
/*
    tests:  map (create, delete, get_col)
            intermidiateResults (create, delete)
            read_workload

            first_filter, n_filter, apply_filter

*/
int main(int argc, char **argv){
    init_scheduler();
    init();
    destroy_scheduler();
    
    return 0;
}
