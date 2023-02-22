#pragma once

#include "data_structures.h"

HashCell create_hash_cell(int h);

void delete_hash_cell(HashCell hash_cell);

//create hashtable
HashTable create_hash_table(int h, int n_bits);

//delete hashtable
void delete_hash_table(HashTable hash_table);

int hash2(int n, uint64_t value);

HashTable rehash(HashTable old_table);

HashTable* build_hash_table_a(Partition partition);

HashTable* build_hash_table_b(Partition partition);

HashTable* build_hash_table_c(Partition partition);

HashTable insert(Tuple element, HashTable hash_table);

HashTable update_bitmap(HashTable hash_table, int pos);

Build build_hash_table(Partition left, Partition right, boolean DEBUG);

void delete_build(Build build);

List find(HashTable hash_table, uint64_t value);

