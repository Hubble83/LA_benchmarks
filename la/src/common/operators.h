#ifndef _OPERATORS_H_
#define _OPERATORS_H_

#include <glib.h>
#include <assert.h>
#include "omp.h"

#include "strings.h"

#define NUM_THREADS 20

#define MEM_LINE_SIZE 64 //32

#define LESS 1
#define LESS_EQ 2
#define GREATER 3
#define GREATER_EQ 4
#define EQUAL 5

typedef int (*Comparator) (const void*, const void*);

// Comparators

int intcmp(char *str1, char *str2);
int dblcmp(char *str1, char *str2);
int strstrcmp(char *str1, char *str2);

// Operators

void unvec_v_col ( 
    long out_n_rows,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,	
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

void sp_fdv_hadamard(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

void filtro_vetor_coluna(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    int opp_code, double comparation_key,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

/* Consider that A is a full valued vector and B is a filtered vector with values */
void sp_v_v_filter_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    int opp_code, double comparation_key,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

void sp_bm_bv_filter_in_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    GHashTable *A_id_label,
    Comparator comparator,
    char ** __restrict__ comparation_keys, int cmpkeys_size,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

void sp_bm_bm_filter_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    GHashTable *A_id_label,
    Comparator comparator,
    int opp_code, char * __restrict__ comparation_key,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

void sp_bm_bm_filter_par(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    GHashTable *A_id_label,
    Comparator comparator,
    int opp_code, char * __restrict__ comparation_key,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

void sp_bm_bv_filter_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    GHashTable *A_id_label,
    Comparator comparator,
    int opp_code, char * __restrict__ comparation_key,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

void sp_bm_bv_filter_par(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    GHashTable *A_id_label,
    Comparator comparator,
    int opp_code, char * __restrict__ comparation_key,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

void sp_bm_bv_filter_and_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    GHashTable *A_id_label,
    Comparator comparator,
    int opp_code, char * __restrict__ comparation_key,
    int opp_code2, char * __restrict__ comparation_key2,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

void sp_bm_bv_filter_and_par(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    GHashTable *A_id_label,
    Comparator comparator,
    int opp_code, char * __restrict__ comparation_key,
    int opp_code2, char * __restrict__ comparation_key2,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

void sp_bmbm_bv_filter_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    GHashTable *A_id_label,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    GHashTable *B_id_label,
    Comparator comparator,
    int opp_code,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

/* Consider that both matrices have at most one '1' by column */
void sp_bvbm_dot_product_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

void sp_bvbm_dot_product_par(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

/* Consider that both matrices have at most one '1' by column */
void sp_bmbv_krao_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

void sp_bmbv_krao_par(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

/* Consider that both matrices have at most one '1' by column */
void sp_bmbm_krao_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

void sp_bmbm_krao_par(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

/* Consider that both matrices have at most one '1' by column */
void sp_bmbm_dot_product_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

void sp_bmbm_dot_product_par(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

/* Consider that vector is full of values */
void sp_v_bang_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

void sp_v_bang_par(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

/* Consider that both vectors are bitmap and not totally full */
void sp_bvbv_hadamard_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

void sp_bvbv_hadamard_par(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

/* Consider that:
 *   A -> Bitmap vector
 *   B -> Vector full of values
 */
void sp_bvv_hadamard_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

void sp_bvv_hadamard_par(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

/* Consider that both vectors are full of values */
void sp_vv_hadamard_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

void sp_vv_hadamard_par(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

void sp_bvbv_hadamard_or_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

/* Tranpose all types of matrices/vectors*/
void sp_transpose(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

/* Consider that matrix have at most one '1' by column and vector is full of values */
void sp_bmv_krao_seq(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

void sp_bmv_krao_par(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long *__restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long B_nnz, long B_n_rows, long B_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) C_col_ptr,
    long *C_nnz, long *C_n_rows, long *C_n_cols
    );

void sp_v_map_mult(
    double x,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

void sp_v_map_div(
    double x,
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

void sp_bv_map_not(
    double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long A_nnz, long A_n_rows, long A_n_cols,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
    long *B_nnz, long *B_n_rows, long *B_n_cols
    );

#endif
