# 1. προσθέτουμε για κάθε εκτελέσιμο <foo> μια μεταβλητή <foo>_OBJS με
#    όλα τα objects (.o) που χρειάζονται για το πρόγραμμα. Πχ
#    myprogram_OBJS = main.o module1.o module2.o
#
# 2. Κάνουμε: include common.mk

prog_OBJS	=	main.o	$(MODULES)/partition.o	$(MODULES)/building.o	$(MODULES)/probing.o	$(MODULES)/list.o	$(MODULES)/join.o	$(MODULES)/random_matrices.o	$(MODULES)/init.o	$(MODULES)/predicatesQueue.o	$(MODULES)/operations.o	$(MODULES)/scheduler.o	$(MODULES)/operations.o	$(MODULES)/filter_statistics.o	$(MODULES)/join_statistics.o
prog_ARGS	=	../../workloads/small/small.init ../../workloads/small/small.work

# Ο βασικός κορμός του Makefile
include ../../common.mk