#ifndef _TOOLS_H_
#define _TOOLS_H_

#include <glib.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <fcntl.h>

#include "strings.h"

#define MEM_LINE_SIZE 64 //32
#define MAX_REG_SIZE 1024

void read_column(
    char *col_name, long n_elems,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long *A_nnz, long *A_n_rows, long *A_n_cols,
    GHashTable **id_label, GHashTable **label_id, long *next_id_label
    );

void read_column_measure(
    char *col_name, long n_elems,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long *A_nnz, long *A_n_rows, long *A_n_cols
    );

void print_column(
    char *col_name,
    double *values, long *row_ind, long *col_ptr,
    long nnz, long n_rows, long n_cols
    );

#endif
