#ifndef ALTAVL_H
#define ALTAVL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "avl.h"
#include "operators.h"


// SUM
typedef struct sum_pair{
	long key;
	double value;
} SumPair;

Tree createSumTree();
Tree fromCSCtoSumTree(Tree t, long *col_ptr, long *row_ind, double *values, long n_cols, long n_rows, long nnz);
void fromSumTreeToCSC(Tree t, long num_labels, long **col_ptr, long **row_ind, double **values, long *n_cols, long *n_rows, long *nnz);

// AVG
typedef struct avg_pair {
	long key;
	double value;
	int num;
} AvgPair;

Tree createAvgTree();
Tree fromCSCtoAvgTree(Tree t, long *col_ptr, long *row_ind, double *values, long n_cols, long n_rows, long nnz);
void fromAvgTreeToCSC(Tree t, long num_labels, long **col_ptr, long **row_ind, double **values, long *n_cols, long *n_rows, long *nnz);

// EXISTS
typedef struct exists_pair{
	long key;
} ExistsPair;

Tree createExistsTree();
Tree fromCSCtoExistsTree(Tree t, long *col_ptr, long *row_ind, double *values, long n_cols, long n_rows, long nnz);
void fromExistsTreeToCSC_Col(Tree t, long num_labels, long **col_ptr, long **row_ind, double **values, long *n_cols, long *n_rows, long *nnz);
void fromExistsTreeToCSC_Row(Tree t, long num_labels, long **col_ptr, long **row_ind, double **values, long *n_cols, long *n_rows, long *nnz);

// COUNT
typedef struct count_pair{
	long key;
	long value;
} CountPair;

Tree createCountTree();
Tree fromCSCtoCountTree(Tree t, long *col_ptr, long *row_ind, double *values, long n_cols, long n_rows, long nnz);
void fromCountTreeToCSC(Tree t, long num_labels, long **col_ptr, long **row_ind, double **values, long *n_cols, long *n_rows, long *nnz);

#endif
