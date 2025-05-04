CC = gcc
CFLAGS  = -g -Wall

default: test_assign2_1

test_assign2_1: test_assign2_1.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) $(CFLAGS) -o test_assign2_1 test_assign2_1.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o -lm

test_assign2_2: test_assign2_2.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) $(CFLAGS) -o test_assign2_2 test_assign2_2.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o -lm

test_assign2_1.o: test_assign2_1.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h
	$(CC) $(CFLAGS) -c test_assign2_1.c -lm

test_assign2_2.o: test_assign2_2.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h
	$(CC) $(CFLAGS) -c test_assign2_2.c -lm

storage_mgr.o: storage_mgr.c storage_mgr.h 
	$(CC) $(CFLAGS) -c storage_mgr.c -lm

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	$(CC) $(CFLAGS) -c buffer_mgr_stat.c

buffer_mgr.o: buffer_mgr.c buffer_mgr.h dt.h storage_mgr.h
	$(CC) $(CFLAGS) -c buffer_mgr.c

dberror.o: dberror.c dberror.h 
	$(CC) $(CFLAGS) -c dberror.c

# for windows del - use rm for others os
clean: 
	del test_assign2_1 test_assign2_2 *.o *~ *.exe

run1:
	./test_assign2_1

run2:
	./test_assign2_2