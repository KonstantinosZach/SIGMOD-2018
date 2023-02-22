# 1. προσθέτουμε για κάθε εκτελέσιμο <foo> μια μεταβλητή <foo>_OBJS με
#    όλα τα objects (.o) που χρειάζονται για το πρόγραμμα. Πχ
#    myprogram_OBJS = main.o module1.o module2.o
#
# 2. Κάνουμε: include common.mk

# Κάνοντας compile το <foo>_test.c με μια υλοποίηση <foo>.c του
# συγκεκριμένου τύπου, παράγουμε ένα tets για την υλοποίηση αυτή.
#

partition_test_OBJS	=	partition_test.o	$(MODULES)/building.o	$(MODULES)/init.o	$(MODULES)/join_statistics.o	$(MODULES)/join.o	$(MODULES)/list.o	$(MODULES)/operations.o	$(MODULES)/partition.o	$(MODULES)/predicatesQueue.o	$(MODULES)/probing.o	$(MODULES)/scheduler.o	$(MODULES)/filter_statistics.o
building_test_OBJS	=	building_test.o	$(MODULES)/building.o	$(MODULES)/init.o	$(MODULES)/join_statistics.o	$(MODULES)/join.o	$(MODULES)/list.o	$(MODULES)/operations.o	$(MODULES)/partition.o	$(MODULES)/predicatesQueue.o	$(MODULES)/probing.o	$(MODULES)/scheduler.o	$(MODULES)/filter_statistics.o
probing_test_OBJS	=	probing_test.o	$(MODULES)/building.o	$(MODULES)/init.o	$(MODULES)/join_statistics.o	$(MODULES)/join.o	$(MODULES)/list.o	$(MODULES)/operations.o	$(MODULES)/partition.o	$(MODULES)/partition.o	$(MODULES)/predicatesQueue.o	$(MODULES)/probing.o	$(MODULES)/scheduler.o	$(MODULES)/filter_statistics.o
join_test_OBJS	= join_test.o	$(MODULES)/building.o	$(MODULES)/init.o	$(MODULES)/join_statistics.o	$(MODULES)/join.o	$(MODULES)/list.o	$(MODULES)/operations.o	$(MODULES)/partition.o	$(MODULES)/partition.o	$(MODULES)/predicatesQueue.o	$(MODULES)/probing.o	$(MODULES)/scheduler.o	$(MODULES)/filter_statistics.o
predicate_queue_test_OBJS	=	predicatesQueue_test.o	$(MODULES)/building.o	$(MODULES)/init.o	$(MODULES)/join_statistics.o	$(MODULES)/join.o	$(MODULES)/list.o	$(MODULES)/operations.o	$(MODULES)/partition.o	$(MODULES)/partition.o	$(MODULES)/predicatesQueue.o	$(MODULES)/probing.o	$(MODULES)/scheduler.o	$(MODULES)/filter_statistics.o
init_test_OBJS	=	init_test.o	$(MODULES)/building.o	$(MODULES)/init.o	$(MODULES)/join_statistics.o	$(MODULES)/join.o	$(MODULES)/list.o	$(MODULES)/operations.o	$(MODULES)/partition.o	$(MODULES)/predicatesQueue.o	$(MODULES)/probing.o	$(MODULES)/scheduler.o	$(MODULES)/filter_statistics.o
filter_statistics_test_OBJS	= filter_statistics_test.o	$(MODULES)/building.o	$(MODULES)/init.o	$(MODULES)/join_statistics.o	$(MODULES)/join.o	$(MODULES)/list.o	$(MODULES)/operations.o	$(MODULES)/partition.o	$(MODULES)/partition.o	$(MODULES)/predicatesQueue.o	$(MODULES)/probing.o	$(MODULES)/scheduler.o	$(MODULES)/filter_statistics.o
list_test_OBJS	=	list_test.o	$(MODULES)/building.o	$(MODULES)/init.o	$(MODULES)/join_statistics.o	$(MODULES)/join.o	$(MODULES)/list.o	$(MODULES)/operations.o	$(MODULES)/partition.o	$(MODULES)/partition.o	$(MODULES)/predicatesQueue.o	$(MODULES)/probing.o	$(MODULES)/scheduler.o	$(MODULES)/filter_statistics.o
scheduler_test_OBJS	= scheduler_test.o	$(MODULES)/building.o	$(MODULES)/init.o	$(MODULES)/join_statistics.o	$(MODULES)/join.o	$(MODULES)/list.o	$(MODULES)/operations.o	$(MODULES)/partition.o	$(MODULES)/partition.o	$(MODULES)/predicatesQueue.o	$(MODULES)/probing.o	$(MODULES)/scheduler.o	$(MODULES)/filter_statistics.o
operations_test_OBJS = operations_test.o	$(MODULES)/building.o	$(MODULES)/init.o	$(MODULES)/join_statistics.o	$(MODULES)/join.o	$(MODULES)/list.o	$(MODULES)/operations.o	$(MODULES)/partition.o	$(MODULES)/predicatesQueue.o	$(MODULES)/probing.o	$(MODULES)/scheduler.o	$(MODULES)/filter_statistics.o
join_statistics_test_OBJS =	join_statistics_test.o	$(MODULES)/building.o	$(MODULES)/init.o	$(MODULES)/join_statistics.o	$(MODULES)/join.o	$(MODULES)/list.o	$(MODULES)/operations.o	$(MODULES)/partition.o	$(MODULES)/predicatesQueue.o	$(MODULES)/probing.o	$(MODULES)/scheduler.o	$(MODULES)/filter_statistics.o

# Ο βασικός κορμός του Makefile
include ../common.mk	