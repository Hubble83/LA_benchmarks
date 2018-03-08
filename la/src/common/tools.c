#include "tools.h"

void read_column(
    char *col_name, long n_elems,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long *A_nnz, long *A_n_rows, long *A_n_cols,
    GHashTable **id_label, GHashTable **label_id, long *next_id_label
    ) {
    
    int fd;
    long i, *id, *res, curr_major_row, out_next_id_label;
    GHashTable *out_id_label, *out_label_id;
    char field[MAX_REG_SIZE], *label;

    __declspec(align(MEM_LINE_SIZE)) double *out_values;
    __declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
    __declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

    out_values = (double*) _mm_malloc(n_elems * sizeof(double), MEM_LINE_SIZE);
    out_row_ind = (long*) _mm_malloc(n_elems * sizeof(long), MEM_LINE_SIZE);
    out_col_ptr = (long*) _mm_malloc((n_elems+1) * sizeof(long), MEM_LINE_SIZE);

    if (*id_label == NULL && *label_id == NULL) {
        out_id_label = g_hash_table_new_full(g_int_hash, g_int_equal, free, free);
        out_label_id = g_hash_table_new(g_str_hash, g_str_equal);
        out_next_id_label = 0;
    } else {
        out_id_label = *id_label;
        out_label_id = *label_id;
        out_next_id_label = *next_id_label;
    }

    fd = open(col_name, O_RDONLY | O_NONBLOCK);
    FILE* stream = fdopen(fd, "r");

    if (stream == NULL) {
        exit(EXIT_FAILURE);
    }

    for (i = 0, curr_major_row = out_next_id_label-1; i < n_elems; ++i) {

        fgets(field, MAX_REG_SIZE, stream);
        
        field[strlen(field)-1] = '\0';

        res = (long*) g_hash_table_lookup(out_label_id, field);

        if (res == NULL) {
            id = (long*) malloc(sizeof(long));
            *id = out_next_id_label++;
            label = strdup(field);
            g_hash_table_insert(out_id_label, id, label);
            g_hash_table_insert(out_label_id, label, id);
        } else {
            id = res;
        }

        if (curr_major_row < *id) {
            curr_major_row = *id;
        }

        out_values[i] = 1.0;
        out_row_ind[i] = *id;
        out_col_ptr[i] = i;
    }

    out_col_ptr[i] = i;
    
    fclose(stream);

    *A_values = out_values;
    *A_col_ptr = out_col_ptr;
    *A_row_ind = out_row_ind;

    *A_nnz = i;
    *A_n_rows = curr_major_row + 1;
    *A_n_cols = i;

    *id_label = out_id_label;
    *label_id = out_label_id;
    *next_id_label = out_next_id_label;
}

void read_column_measure (
    char *col_name, long n_elems,
    double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
    long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
    long *A_nnz, long *A_n_rows, long *A_n_cols
    ) {

    int fd;
    long i;
    char field[MAX_REG_SIZE];
    double value;

    __declspec(align(MEM_LINE_SIZE)) double *out_values;
    __declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
    __declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

    out_values = (double*) _mm_malloc(n_elems * sizeof(double), MEM_LINE_SIZE);
    out_row_ind = (long*) _mm_malloc(n_elems * sizeof(long), MEM_LINE_SIZE);
    out_col_ptr = (long*) _mm_malloc((n_elems+1) * sizeof(long), MEM_LINE_SIZE);

    fd = open(col_name, O_RDONLY | O_NONBLOCK);
    FILE* stream = fdopen(fd, "r");

    if (stream == NULL) {
        exit(EXIT_FAILURE);
    }

    for (i = 0; i < n_elems; ++i) {

        fgets(field, MAX_REG_SIZE, stream);
        
        value = atof(field);

        out_values[i] = value;
        out_row_ind[i] = 0;
        out_col_ptr[i] = i;
    }

    out_col_ptr[i] = i;
    
    fclose(stream);

    *A_values = out_values;
    *A_col_ptr = out_col_ptr;
    *A_row_ind = out_row_ind;
    *A_nnz = i;
    *A_n_rows = 1;
    *A_n_cols = i;
}

void print_column(
    char *col_name,
    double *values, long *row_ind, long *col_ptr,
    long nnz, long n_rows, long n_cols
    ) {
    
    long i;

    printf("values:  [ ");
    for (i = 0; i < nnz-1; ++i) {
        printf("%f, ", values[i]);
    }
    printf("%f ]\nrow_ind: [ ", values[i]);
    for (i = 0; i < nnz-1; ++i){
        printf("%ld, ", row_ind[i]);
    }
    printf("%ld ]\ncol_ptr: [ ", row_ind[i]);
    for (i = 0; i < n_cols; ++i){
        printf("%ld, ", col_ptr[i]);
    }
    printf("%ld ]\n", col_ptr[i]);
}
