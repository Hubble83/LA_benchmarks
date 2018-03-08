#include "operators.h"

// Comparators

int intcmp(char *str1, char *str2) {
	return (atoi(str1) - atoi(str2));
}

int dblcmp(char *str1, char *str2) {

	const double d1 = atof(str1);
	const double d2 = atof(str2);

	return  (d1 > d2) - (d1 < d2);
}

int strstrcmp(char *str1, char *str2) {
	return (strstr(str1, str2) != NULL) ? 0 : 1;
}

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
    ) {

    #ifdef D_DEBUGGING
    assert(A_nnz <= A_n_rows);
    assert(A_n_cols == 1);
    #endif

    long out_n_cols = A_n_rows / out_n_rows;

    __declspec(align(MEM_LINE_SIZE)) double *out_values;
    __declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
    __declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

    out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
    out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
    out_col_ptr = (long*) _mm_malloc((out_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

    __assume_aligned(A_row_ind, MEM_LINE_SIZE);
    __assume_aligned(A_values, MEM_LINE_SIZE);

    long i, nnz = 0;
    for (i = 0; i < out_n_cols; ++i ) {
        out_col_ptr[i] = nnz;
        while (nnz < A_nnz && A_row_ind[nnz] < (i+1)*out_n_rows ) {
            out_values[nnz] = A_values[nnz];
            out_row_ind[nnz] = A_row_ind[nnz] % out_n_rows;
            ++nnz;
        }
    }
    out_col_ptr[i] = nnz;

    *B_values = out_values;
    *B_row_ind = out_row_ind;
    *B_col_ptr = out_col_ptr;
    *B_nnz = nnz;
    *B_n_rows = out_n_rows;
    *B_n_cols = out_n_cols;
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_cols);
	assert(A_n_rows == 1);
	assert(B_n_rows == 1);
	#endif

	long i, nnz;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	__assume_aligned(B_values, MEM_LINE_SIZE);
	__assume_aligned(B_col_ptr, MEM_LINE_SIZE);

	for (i = 0, nnz = 0; i < A_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		if ( (A_col_ptr[i+1] > A_col_ptr[i]) && (B_col_ptr[i+1] > B_col_ptr[i]) ) {
			out_values[nnz] = B_values[B_col_ptr[i]];
			out_row_ind[nnz] = 0;
			nnz++;
		}
	}

	out_col_ptr[i] = nnz;

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = A_nnz;
	*C_n_rows = 1;
	*C_n_cols = A_n_cols;
}

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
	) {

	#ifdef D_DEBUGGING
	assert(A_nnz <= A_n_rows);
	assert(A_n_cols == 1);
	#endif

	long i, nnz;
	double value;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_row_ind, MEM_LINE_SIZE);
	__assume_aligned(A_values, MEM_LINE_SIZE);

	for (i = 0, nnz = 0; i < A_nnz; ++i) {

		value = A_values[i];

		if (
			( (opp_code == LESS) && (value < comparation_key) )
			||
			( (opp_code == LESS_EQ) && (value <= comparation_key) )
			||
			( (opp_code == EQUAL) && (value == comparation_key) )
			||
			( (opp_code == GREATER) && (value > comparation_key) )
			||
			( (opp_code == GREATER_EQ) && (value >= comparation_key) )
		   ) {

			out_row_ind[nnz] = A_row_ind[i];
			out_values[nnz] = value;
			nnz++;
		}
	}

	out_col_ptr[0] = 0;
	out_col_ptr[1] = nnz;

	*B_values = out_values;
	*B_row_ind = out_row_ind;
	*B_col_ptr = out_col_ptr;
	*B_nnz = nnz;
	*B_n_rows = A_n_rows;
	*B_n_cols = A_n_cols;
}

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
    ) {

	#ifdef D_DEBUGGING
	assert(A_nnz <= A_n_cols);
	assert(A_n_rows == 1);
	#endif

	long i, nnz;
	double value;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);
	__assume_aligned(A_values, MEM_LINE_SIZE);

	for (i = 0, nnz = 0; i < A_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		value = A_values[A_col_ptr[i]];

		if (
			( (opp_code == LESS) && (value < comparation_key) )
			||
			( (opp_code == LESS_EQ) && (value <= comparation_key) )
			||
			( (opp_code == EQUAL) && (value == comparation_key) )
			||
			( (opp_code == GREATER) && (value > comparation_key) )
			||
			( (opp_code == GREATER_EQ) && (value >= comparation_key) )
		   ) {

			out_row_ind[nnz] = 0;
			out_values[nnz] = 1.0;
			nnz++;
		}
	}

	out_col_ptr[i] = nnz;

	*B_values = out_values;
	*B_row_ind = out_row_ind;
	*B_col_ptr = out_col_ptr;
	*B_nnz = nnz;
	*B_n_rows = A_n_rows;
	*B_n_cols = A_n_cols;
}

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
    ) {

	#ifdef D_DEBUGGING
	assert(A_nnz <= A_n_cols);
	#endif

	char *label;
	long *id, i, j, nnz, row;
	int returned_strcmp;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	__declspec(align(MEM_LINE_SIZE)) int *filter;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	filter = (int*) _mm_malloc(A_n_rows * sizeof(int), MEM_LINE_SIZE);

	id = (long*) malloc(sizeof(long));

	__assume_aligned(A_row_ind, MEM_LINE_SIZE);
	__assume_aligned(filter, MEM_LINE_SIZE);

	for (i = 0; i < A_n_rows; ++i) {

		*id = i;
		label = (char*) g_hash_table_lookup(A_id_label, id);

		filter[i] = 0;
		for (j = 0; j < cmpkeys_size; ++j) {

			returned_strcmp = comparator(label, comparation_keys[j]);

			if (!returned_strcmp) {
				filter[i] = 1;
				break;
			}
		}
	}

	for (i = 0, nnz = 0; i < A_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		row = A_row_ind[i];

		if (filter[row]) {
			out_values[nnz] = 1.0;
			out_row_ind[nnz] = 0;
			nnz++;
		}
	}

	out_col_ptr[i] = nnz;

	*B_values = out_values;
	*B_row_ind = out_row_ind;
	*B_col_ptr = out_col_ptr;
	*B_nnz = nnz;
	*B_n_rows = 1;
	*B_n_cols = A_n_cols;

	_mm_free(filter);
	free(id);
}

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
	) {

	#ifdef D_DEBUGGING
	assert(A_nnz <= A_n_cols);
	#endif

	char *label;
	long *id, i, nnz, row;
	int returned_strcmp;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	__declspec(align(MEM_LINE_SIZE)) int *filter;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	filter = (int*) _mm_malloc(A_n_rows * sizeof(int), MEM_LINE_SIZE);

	id = (long*) malloc(sizeof(long));

	__assume_aligned(A_row_ind, MEM_LINE_SIZE);
	__assume_aligned(filter, MEM_LINE_SIZE);

	for (i = 0; i < A_n_rows; ++i) {

		*id = i;
		label = (char*) g_hash_table_lookup(A_id_label, id);
		returned_strcmp = comparator(label, comparation_key);

		filter[i] = (
			( (opp_code == LESS) && (returned_strcmp < 0) )
			||
			( (opp_code == LESS_EQ) && (returned_strcmp <= 0) )
			||
			( (opp_code == EQUAL) && (returned_strcmp == 0) )
			||
			( (opp_code == GREATER) && (returned_strcmp > 0) )
			||
			( (opp_code == GREATER_EQ) && (returned_strcmp >= 0) )
			)
			? 1 : 0;
	}

	for (i = 0, nnz = 0; i < A_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		row = A_row_ind[i];

		if (filter[row]) {
			out_values[nnz] = 1.0;
			out_row_ind[nnz] = row;
			nnz++;
		}
	}

	out_col_ptr[i] = nnz;

	*B_values = out_values;
	*B_row_ind = out_row_ind;
	*B_col_ptr = out_col_ptr;
	*B_nnz = nnz;
	*B_n_rows = A_n_rows;
	*B_n_cols = A_n_cols;

	_mm_free(filter);
	free(id);
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_nnz <= A_n_cols);
	#endif

	long total_nnz;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	__declspec(align(MEM_LINE_SIZE)) int *filter;
	__declspec(align(MEM_LINE_SIZE)) long *global_nnz;

	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	filter = (int*) _mm_malloc(A_n_rows * sizeof(int), MEM_LINE_SIZE);
	global_nnz = (long*) _mm_malloc((NUM_THREADS+1) * sizeof(long), MEM_LINE_SIZE);

	#pragma omp parallel shared(filter)
	{
		char *label;
		long *id;
		int returned_strcmp;

		id = (long*) malloc(sizeof(long));

		#pragma omp for schedule(static)
		for (int i = 0; i < A_n_rows; ++i) {

			*id = i;
			label = (char*) g_hash_table_lookup(A_id_label, id);
			returned_strcmp = comparator(label, comparation_key);

			filter[i] = (
				( (opp_code == LESS) && (returned_strcmp < 0) )
				||
				( (opp_code == LESS_EQ) && (returned_strcmp <= 0) )
				||
				( (opp_code == EQUAL) && (returned_strcmp == 0) )
				||
				( (opp_code == GREATER) && (returned_strcmp > 0) )
				||
				( (opp_code == GREATER_EQ) && (returned_strcmp >= 0) )
				)
				? 1 : 0;
		}

		free(id);
	}

	global_nnz[0] = 0;

	#pragma omp parallel shared(out_col_ptr, out_row_ind, global_nnz)
	{
		long i, j, start, finish, nnz, row;
		int thread_id;

		__declspec(align(MEM_LINE_SIZE)) long *local_row_ind;

		local_row_ind = (long*) _mm_malloc(((A_nnz / NUM_THREADS) + (A_nnz % NUM_THREADS)) * sizeof(long), MEM_LINE_SIZE);

		thread_id = omp_get_thread_num();
		start = (long) (thread_id * A_n_cols) / NUM_THREADS;
		finish = (long) ((thread_id+1) * A_n_cols) / NUM_THREADS;

		__assume_aligned(A_row_ind, MEM_LINE_SIZE);
		__assume_aligned(filter, MEM_LINE_SIZE);

		for (i = start, nnz = 0; i < finish; ++i) {

			out_col_ptr[i] = nnz;

			row = A_row_ind[i];

			if (filter[row]) {
				local_row_ind[nnz] = row;
				nnz++;
			}
		}

		global_nnz[thread_id+1] = nnz;
		//#pragma omp flush (global_nnz)

		#pragma omp barrier

		#pragma omp master
		for (int i = 1; i <= NUM_THREADS; ++i) {
			global_nnz[i] += global_nnz[i-1];
		}

		//#pragma omp flush (global_nnz)

		#pragma omp barrier

		for (i = start; i < finish; ++i) {
			out_col_ptr[i] += global_nnz[thread_id];
		}

		start = global_nnz[thread_id];
		finish = global_nnz[thread_id+1];

		for (i = start, j = 0; i < finish; ++i, ++j) {
			out_row_ind[i] = local_row_ind[j];
		}

		_mm_free(local_row_ind);
	}

	total_nnz = global_nnz[NUM_THREADS];
	out_col_ptr[A_n_cols] = total_nnz;

	out_values = (double*) _mm_malloc(total_nnz * sizeof(double), MEM_LINE_SIZE);

	#pragma omp parallel for schedule(static)
	for (int i = 0; i < total_nnz; ++i) {
		out_values[i] = 1.0;
	}

	*B_values = out_values;
	*B_row_ind = out_row_ind;
	*B_col_ptr = out_col_ptr;
	*B_nnz = total_nnz;
	*B_n_rows = A_n_rows;
	*B_n_cols = A_n_cols;

	_mm_free(filter);
	_mm_free(global_nnz);
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_nnz <= A_n_cols);
	#endif

	char *label;
	long *id, i, nnz;
	int returned_strcmp;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	__declspec(align(MEM_LINE_SIZE)) int *filter;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	filter = (int*) _mm_malloc(A_n_rows * sizeof(int), MEM_LINE_SIZE);

	id = (long*) malloc(sizeof(long));

	__assume_aligned(A_row_ind, MEM_LINE_SIZE);
	__assume_aligned(filter, MEM_LINE_SIZE);

	for (i = 0; i < A_n_rows; ++i) {

		*id = i;
		label = (char*) g_hash_table_lookup(A_id_label, id);
		returned_strcmp = comparator(label, comparation_key);

		filter[i] = (
			( (opp_code == LESS) && (returned_strcmp < 0) )
			||
			( (opp_code == LESS_EQ) && (returned_strcmp <= 0) )
			||
			( (opp_code == EQUAL) && (returned_strcmp == 0) )
			||
			( (opp_code == GREATER) && (returned_strcmp > 0) )
			||
			( (opp_code == GREATER_EQ) && (returned_strcmp >= 0) )
			)
			? 1 : 0;
	}

	for (i = 0, nnz = 0; i < A_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		if (filter[A_row_ind[i]]) {
			out_values[nnz] = 1.0;
			out_row_ind[nnz] = 0;
			nnz++;
		}
	}

	out_col_ptr[i] = nnz;

	*B_values = out_values;
	*B_row_ind = out_row_ind;
	*B_col_ptr = out_col_ptr;
	*B_nnz = nnz;
	*B_n_rows = 1;
	*B_n_cols = A_n_cols;

	_mm_free(filter);
	free(id);
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_nnz <= A_n_cols);
	#endif

	long total_nnz;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	__declspec(align(MEM_LINE_SIZE)) int *filter;
	__declspec(align(MEM_LINE_SIZE)) long *global_nnz;

	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	filter = (int*) _mm_malloc(A_n_rows * sizeof(int), MEM_LINE_SIZE);
	global_nnz = (long*) _mm_malloc((NUM_THREADS+1) * sizeof(long), MEM_LINE_SIZE);

	#pragma omp parallel shared(filter)
	{
		char *label;
		long *id;
		int returned_strcmp;

		id = (long*) malloc(sizeof(long));

		#pragma omp for schedule(static)
		for (int i = 0; i < A_n_rows; ++i) {

			*id = i;
			label = (char*) g_hash_table_lookup(A_id_label, id);
			returned_strcmp = comparator(label, comparation_key);

			filter[i] = (
				( (opp_code == LESS) && (returned_strcmp < 0) )
				||
				( (opp_code == LESS_EQ) && (returned_strcmp <= 0) )
				||
				( (opp_code == EQUAL) && (returned_strcmp == 0) )
				||
				( (opp_code == GREATER) && (returned_strcmp > 0) )
				||
				( (opp_code == GREATER_EQ) && (returned_strcmp >= 0) )
				)
				? 1 : 0;
		}

		free(id);
	}

	global_nnz[0] = 0;

	#pragma omp parallel shared(out_col_ptr, global_nnz)
	{
		long i, start, finish, nnz;
		int thread_id;

		thread_id = omp_get_thread_num();
		start = (long) (thread_id * A_n_cols) / NUM_THREADS;
		finish = (long) ((thread_id+1) * A_n_cols) / NUM_THREADS;

		__assume_aligned(A_row_ind, MEM_LINE_SIZE);
		__assume_aligned(filter, MEM_LINE_SIZE);

		for (i = start, nnz = 0; i < finish; ++i) {

			out_col_ptr[i] = nnz;

			if (filter[A_row_ind[i]]) {
				nnz++;
			}
		}

		global_nnz[thread_id+1] = nnz;
		//#pragma omp flush (global_nnz)

		#pragma omp barrier

		#pragma omp master
		for (int i = 1; i <= NUM_THREADS; ++i) {
			global_nnz[i] += global_nnz[i-1];
		}

		//#pragma omp flush (global_nnz)

		#pragma omp barrier

		for (i = start; i < finish; ++i) {
			out_col_ptr[i] += global_nnz[thread_id];
		}
	}

	total_nnz = global_nnz[NUM_THREADS];
	out_col_ptr[A_n_cols] = total_nnz;

	out_values = (double*) _mm_malloc(total_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(total_nnz * sizeof(long), MEM_LINE_SIZE);

	#pragma omp parallel for schedule(static)
	for (int i = 0; i < total_nnz; ++i) {
		out_row_ind[i] = 0;
	}

	#pragma omp parallel for schedule(static)
	for (int i = 0; i < total_nnz; ++i) {
		out_values[i] = 1.0;
	}

	*B_values = out_values;
	*B_row_ind = out_row_ind;
	*B_col_ptr = out_col_ptr;
	*B_nnz = total_nnz;
	*B_n_rows = 1;
	*B_n_cols = A_n_cols;

	_mm_free(filter);
	_mm_free(global_nnz);
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_nnz <= A_n_cols);
	#endif

	char *label;
	long *id, i, nnz;
	int returned_strcmp, returned_strcmp2;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	__declspec(align(MEM_LINE_SIZE)) int *filter;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	filter = (int*) _mm_malloc(A_n_rows * sizeof(int), MEM_LINE_SIZE);

	id = (long*) malloc(sizeof(long));

	__assume_aligned(A_row_ind, MEM_LINE_SIZE);
	__assume_aligned(filter, MEM_LINE_SIZE);

	for (i = 0; i < A_n_rows; ++i) {

		*id = i;
		label = (char*) g_hash_table_lookup(A_id_label, id);
		returned_strcmp = comparator(label, comparation_key);

		if (
			( (opp_code == LESS) && (returned_strcmp < 0) )
			||
			( (opp_code == LESS_EQ) && (returned_strcmp <= 0) )
			||
			( (opp_code == EQUAL) && (returned_strcmp == 0) )
			||
			( (opp_code == GREATER) && (returned_strcmp > 0) )
			||
			( (opp_code == GREATER_EQ) && (returned_strcmp >= 0) )
		   )
		{
			returned_strcmp2 = comparator(label, comparation_key2);

			if (
				( (opp_code2 == LESS) && (returned_strcmp2 < 0) )
				||
				( (opp_code2 == LESS_EQ) && (returned_strcmp2 <= 0) )
				||
				( (opp_code2 == EQUAL) && (returned_strcmp2 == 0) )
				||
				( (opp_code2 == GREATER) && (returned_strcmp2 > 0) )
				||
				( (opp_code2 == GREATER_EQ) && (returned_strcmp2 >= 0) )
			   )
			{
				filter[i] = 1;
			} else {
				filter[i] = 0;
			}
		} else {
			filter[i] = 0;
		}
	}

	for (i = 0, nnz = 0; i < A_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		if (filter[A_row_ind[i]]) {
			out_values[nnz] = 1.0;
			out_row_ind[nnz] = 0;
			nnz++;
		}
	}

	out_col_ptr[i] = nnz;

	*B_values = out_values;
	*B_row_ind = out_row_ind;
	*B_col_ptr = out_col_ptr;
	*B_nnz = nnz;
	*B_n_rows = 1;
	*B_n_cols = A_n_cols;

	_mm_free(filter);
	free(id);
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_nnz <= A_n_cols);
	#endif

	long total_nnz;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	__declspec(align(MEM_LINE_SIZE)) int *filter;
	__declspec(align(MEM_LINE_SIZE)) long *global_nnz;

	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	filter = (int*) _mm_malloc(A_n_rows * sizeof(int), MEM_LINE_SIZE);
	global_nnz = (long*) _mm_malloc((NUM_THREADS+1) * sizeof(long), MEM_LINE_SIZE);

	#pragma omp parallel shared(filter)
	{
		char *label;
		long *id;
		int returned_strcmp, returned_strcmp2;

		id = (long*) malloc(sizeof(long));

		#pragma omp for schedule(static)
		for (int i = 0; i < A_n_rows; ++i) {

			*id = i;
			label = (char*) g_hash_table_lookup(A_id_label, id);
			returned_strcmp = comparator(label, comparation_key);

			if (
				( (opp_code == LESS) && (returned_strcmp < 0) )
				||
				( (opp_code == LESS_EQ) && (returned_strcmp <= 0) )
				||
				( (opp_code == EQUAL) && (returned_strcmp == 0) )
				||
				( (opp_code == GREATER) && (returned_strcmp > 0) )
				||
				( (opp_code == GREATER_EQ) && (returned_strcmp >= 0) )
			   )
			{
				returned_strcmp2 = comparator(label, comparation_key2);

				if (
					( (opp_code2 == LESS) && (returned_strcmp2 < 0) )
					||
					( (opp_code2 == LESS_EQ) && (returned_strcmp2 <= 0) )
					||
					( (opp_code2 == EQUAL) && (returned_strcmp2 == 0) )
					||
					( (opp_code2 == GREATER) && (returned_strcmp2 > 0) )
					||
					( (opp_code2 == GREATER_EQ) && (returned_strcmp2 >= 0) )
				   )
				{
					filter[i] = 1;
				} else {
					filter[i] = 0;
				}
			} else {
				filter[i] = 0;
			}
		}

		free(id);
	}

	global_nnz[0] = 0;

	#pragma omp parallel shared(out_col_ptr, global_nnz)
	{
		long i, start, finish, nnz;
		int thread_id;

		thread_id = omp_get_thread_num();
		start = (long) (thread_id * A_n_cols) / NUM_THREADS;
		finish = (long) ((thread_id+1) * A_n_cols) / NUM_THREADS;

		__assume_aligned(A_row_ind, MEM_LINE_SIZE);
		__assume_aligned(filter, MEM_LINE_SIZE);

		for (i = start, nnz = 0; i < finish; ++i) {

			out_col_ptr[i] = nnz;

			if (filter[A_row_ind[i]]) {
				nnz++;
			}
		}

		global_nnz[thread_id+1] = nnz;
		//#pragma omp flush (global_nnz)

		#pragma omp barrier

		#pragma omp master
		for (int i = 1; i <= NUM_THREADS; ++i) {
			global_nnz[i] += global_nnz[i-1];
		}

		//#pragma omp flush (global_nnz)

		#pragma omp barrier

		for (i = start; i < finish; ++i) {
			out_col_ptr[i] += global_nnz[thread_id];
		}
	}

	total_nnz = global_nnz[NUM_THREADS];
	out_col_ptr[A_n_cols] = total_nnz;

	out_values = (double*) _mm_malloc(total_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(total_nnz * sizeof(long), MEM_LINE_SIZE);

	#pragma omp parallel for schedule(static)
	for (int i = 0; i < total_nnz; ++i) {
		out_row_ind[i] = 0;
	}

	#pragma omp parallel for schedule(static)
	for (int i = 0; i < total_nnz; ++i) {
		out_values[i] = 1.0;
	}

	*B_values = out_values;
	*B_row_ind = out_row_ind;
	*B_col_ptr = out_col_ptr;
	*B_nnz = total_nnz;
	*B_n_rows = 1;
	*B_n_cols = A_n_cols;

	_mm_free(filter);
	_mm_free(global_nnz);
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_cols);
	assert(A_nnz == A_n_cols);
	assert(B_nnz == B_n_cols);
	#endif

	char *A_label, *B_label;
	long *A_id, *B_id, i, nnz;
	int returned_strcmp;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	A_id = (long*) malloc(sizeof(long));
	B_id = (long*) malloc(sizeof(long));

	__assume_aligned(A_row_ind, MEM_LINE_SIZE);
	__assume_aligned(B_row_ind, MEM_LINE_SIZE);

	for (i = 0, nnz = 0; i < A_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		*A_id = A_row_ind[i];
		*B_id = B_row_ind[i];
		A_label = (char*) g_hash_table_lookup(A_id_label, A_id);
		B_label = (char*) g_hash_table_lookup(B_id_label, B_id);
		returned_strcmp = comparator(A_label, B_label);

		if (
			( (opp_code == LESS) && (returned_strcmp < 0) )
			||
            ( (opp_code == LESS_EQ) && (returned_strcmp <= 0) )
            ||
            ( (opp_code == EQUAL) && (returned_strcmp == 0) )
            ||
            ( (opp_code == GREATER) && (returned_strcmp > 0) )
            ||
            ( (opp_code == GREATER_EQ) && (returned_strcmp >= 0) )
		   )
		{
			out_values[nnz] = 1.0;
			out_row_ind[nnz] = 0;
			nnz++;
		}
	}

	out_col_ptr[i] = nnz;

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = nnz;
	*C_n_rows = 1;
	*C_n_cols = A_n_cols;

	free(A_id); free(B_id);
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_rows);
	assert(A_nnz <= A_n_cols);
	assert(B_nnz <= B_n_cols);
	assert(A_n_rows == 1);
	#endif

	long i, nnz, row, a_col_start, b_col_start, a_col_end, b_col_end;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	nnz = A_nnz > B_nnz ? A_nnz : B_nnz;

	out_values = (double*) _mm_malloc(nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((B_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	__assume_aligned(B_row_ind, MEM_LINE_SIZE);
	__assume_aligned(B_col_ptr, MEM_LINE_SIZE);

	for (i = 0, nnz = 0; i < B_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		b_col_start = B_col_ptr[i];
		b_col_end = B_col_ptr[i+1];

		if (b_col_end > b_col_start) {

			row = B_row_ind[b_col_start];

			a_col_start = A_col_ptr[row];
			a_col_end = A_col_ptr[row+1];

			if (a_col_end > a_col_start) {

				out_values[nnz] = 1.0;
				out_row_ind[nnz] = 0;
				nnz++;
			}
		}
	}

	out_col_ptr[i] = nnz;

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = nnz;
	*C_n_rows = A_n_rows;
	*C_n_cols = B_n_cols;
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_rows);
	assert(A_nnz <= A_n_cols);
	assert(B_nnz <= B_n_cols);
	assert(A_n_rows == 1);
	#endif

	long total_nnz;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	__declspec(align(MEM_LINE_SIZE)) long *global_nnz;

	out_col_ptr = (long*) _mm_malloc((B_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	global_nnz = (long*) _mm_malloc((NUM_THREADS+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	__assume_aligned(B_row_ind, MEM_LINE_SIZE);
	__assume_aligned(B_col_ptr, MEM_LINE_SIZE);

	global_nnz[0] = 0;

	#pragma omp parallel shared(out_col_ptr, global_nnz)
	{
		long i, start, finish, nnz, row, a_col_start, b_col_start, a_col_end, b_col_end;
		int thread_id;

		thread_id = omp_get_thread_num();
		start = (long) (thread_id * B_n_cols) / NUM_THREADS;
		finish = (long) ((thread_id+1) * B_n_cols) / NUM_THREADS;

		for (i = start, nnz = 0; i < finish; ++i) {

			out_col_ptr[i] = nnz;

			b_col_start = B_col_ptr[i];
			b_col_end = B_col_ptr[i+1];

			if (b_col_end > b_col_start) {

				row = B_row_ind[b_col_start];

				a_col_start = A_col_ptr[row];
				a_col_end = A_col_ptr[row+1];

				if (a_col_end > a_col_start) {
					nnz++;
				}
			}
		}

		global_nnz[thread_id+1] = nnz;

		#pragma omp barrier

		#pragma omp master
		for (int i = 1; i <= NUM_THREADS; ++i) {
			global_nnz[i] += global_nnz[i-1];
		}

		#pragma omp barrier

		for (i = start; i < finish; ++i) {
			out_col_ptr[i] += global_nnz[thread_id];
		}
	}

	total_nnz = global_nnz[NUM_THREADS];
	out_col_ptr[B_n_cols] = total_nnz;

	out_values = (double*) _mm_malloc(total_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(total_nnz * sizeof(long), MEM_LINE_SIZE);

	#pragma omp parallel for schedule(static)
	for (int i = 0; i < total_nnz; ++i) {
		out_row_ind[i] = 0;
	}

	#pragma omp parallel for schedule(static)
	for (int i = 0; i < total_nnz; ++i) {
		out_values[i] = 1.0;
	}

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = total_nnz;
	*C_n_rows = A_n_rows;
	*C_n_cols = B_n_cols;

	_mm_free(global_nnz);
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_cols);
	assert(A_nnz <= A_n_cols);
	assert(B_nnz <= B_n_cols);
	assert(B_n_rows == 1);
	#endif

	long i, nnz, a_col_start, b_col_start, a_col_end, b_col_end;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	out_values = (double*) _mm_malloc(B_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(B_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((B_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_row_ind, MEM_LINE_SIZE);
	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	__assume_aligned(B_col_ptr, MEM_LINE_SIZE);

	for (i = 0, nnz = 0; i < B_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		b_col_start = B_col_ptr[i];
		b_col_end = B_col_ptr[i+1];

		if (b_col_end > b_col_start) {

			a_col_start = A_col_ptr[i];
			a_col_end = A_col_ptr[i+1];

			if (a_col_end > a_col_start) {

				out_values[nnz] = 1.0;
				out_row_ind[nnz] = A_row_ind[a_col_start];
				nnz++;
			}
		}
	}

	out_col_ptr[i] = nnz;

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = nnz;
	*C_n_rows = A_n_rows;
	*C_n_cols = A_n_cols;
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_cols);
	assert(A_nnz <= A_n_cols);
	assert(B_nnz <= B_n_cols);
	assert(B_n_rows == 1);
	#endif

	long total_nnz;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	__declspec(align(MEM_LINE_SIZE)) long *global_nnz;

	out_row_ind = (long*) _mm_malloc(B_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((B_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	global_nnz = (long*) _mm_malloc((NUM_THREADS+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_row_ind, MEM_LINE_SIZE);
	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	__assume_aligned(B_col_ptr, MEM_LINE_SIZE);

	global_nnz[0] = 0;

	#pragma omp parallel shared(out_col_ptr, out_row_ind, global_nnz)
	{

		long i, j, start, finish, nnz, a_col_start, a_col_end, b_col_start, b_col_end;
		int thread_id;

		__declspec(align(MEM_LINE_SIZE)) long *local_row_ind;

		local_row_ind = (long*) _mm_malloc(((B_n_cols / NUM_THREADS) + (B_n_cols % NUM_THREADS)) * sizeof(long), MEM_LINE_SIZE);

		thread_id = omp_get_thread_num();
		start = (long) (thread_id * B_n_cols) / NUM_THREADS;
		finish = (long) ((thread_id+1) * B_n_cols) / NUM_THREADS;

		for (i = start, nnz = 0; i < finish; ++i) {

			out_col_ptr[i] = nnz;

			b_col_start = B_col_ptr[i];
			b_col_end = B_col_ptr[i+1];

			if (b_col_end > b_col_start) {

				a_col_start = A_col_ptr[i];
				a_col_end = A_col_ptr[i+1];

				if (a_col_end > a_col_start) {

					local_row_ind[nnz] = A_row_ind[a_col_start];
					nnz++;
				}
			}
		}

		global_nnz[thread_id+1] = nnz;

		#pragma omp barrier

		#pragma omp master
		for (int i = 1; i <= NUM_THREADS; ++i) {
			global_nnz[i] += global_nnz[i-1];
		}

		#pragma omp barrier

		for (i = start; i < finish; ++i) {
			out_col_ptr[i] += global_nnz[thread_id];
		}

		start = global_nnz[thread_id];
		finish = global_nnz[thread_id+1];

		for (i = start, j = 0; i < finish; ++i, ++j) {
			out_row_ind[i] = local_row_ind[j];
		}

		_mm_free(local_row_ind);
	}

	total_nnz = global_nnz[NUM_THREADS];
	out_col_ptr[A_n_cols] = total_nnz;

	out_values = (double*) _mm_malloc(total_nnz * sizeof(double), MEM_LINE_SIZE);

	#pragma omp parallel for schedule(static)
	for (int i = 0; i < total_nnz; ++i) {
		out_values[i] = 1.0;
	}

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = total_nnz;
	*C_n_rows = A_n_rows;
	*C_n_cols = A_n_cols;

	_mm_free(global_nnz);
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_cols);
	assert(A_nnz <= A_n_cols);
	assert(B_nnz <= B_n_cols);
	#endif

	long i, nnz, a_col_start, b_col_start, a_col_end, b_col_end, scalar;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	out_values = (double*) _mm_malloc(B_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(B_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((B_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_row_ind, MEM_LINE_SIZE);
	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	__assume_aligned(B_row_ind, MEM_LINE_SIZE);
	__assume_aligned(B_col_ptr, MEM_LINE_SIZE);

	for (i = 0, nnz = 0, scalar = A_n_rows; i < B_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		b_col_start = B_col_ptr[i];
		b_col_end = B_col_ptr[i+1];

		if (b_col_end > b_col_start) {

			a_col_start = A_col_ptr[i];
			a_col_end = A_col_ptr[i+1];

			if (a_col_end > a_col_start) {

				out_values[nnz] = 1.0;
				out_row_ind[nnz] = A_row_ind[a_col_start] + (B_row_ind[b_col_start] * scalar);
				nnz++;
			}
		}
	}

	out_col_ptr[i] = nnz;

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = nnz;
	*C_n_rows = A_n_rows * B_n_rows;
	*C_n_cols = A_n_cols;
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_cols);
	assert(A_nnz <= A_n_cols);
	assert(B_nnz <= B_n_cols);
	#endif

	long total_nnz;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	__declspec(align(MEM_LINE_SIZE)) long *global_nnz;

	out_row_ind = (long*) _mm_malloc(B_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((B_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	global_nnz = (long*) _mm_malloc((NUM_THREADS+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_row_ind, MEM_LINE_SIZE);
	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	__assume_aligned(B_row_ind, MEM_LINE_SIZE);
	__assume_aligned(B_col_ptr, MEM_LINE_SIZE);

	global_nnz[0] = 0;

	#pragma omp parallel shared(out_col_ptr, out_row_ind, global_nnz)
	{
		long i, j, start, finish, nnz, a_col_start, b_col_start, a_col_end, b_col_end, scalar;
		int thread_id;

		__declspec(align(MEM_LINE_SIZE)) long *local_row_ind;

		local_row_ind = (long*) _mm_malloc(((B_n_cols / NUM_THREADS) + (B_n_cols % NUM_THREADS)) * sizeof(long), MEM_LINE_SIZE);

		thread_id = omp_get_thread_num();
		start = (long) (thread_id * A_n_cols) / NUM_THREADS;
		finish = (long) ((thread_id+1) * A_n_cols) / NUM_THREADS;

		for (i = start, nnz = 0, scalar = A_n_rows; i < finish; ++i) {

			out_col_ptr[i] = nnz;

			b_col_start = B_col_ptr[i];
			b_col_end = B_col_ptr[i+1];

			if (b_col_end > b_col_start) {

				a_col_start = A_col_ptr[i];
				a_col_end = A_col_ptr[i+1];

				if (a_col_end > a_col_start) {

					local_row_ind[nnz] = A_row_ind[a_col_start] + (B_row_ind[b_col_start] * scalar);
					nnz++;
				}
			}
		}

		global_nnz[thread_id+1] = nnz;

		#pragma omp barrier

		#pragma omp master
		for (int i = 1; i <= NUM_THREADS; ++i) {
			global_nnz[i] += global_nnz[i-1];
		}

		#pragma omp barrier

		for (i = start; i < finish; ++i) {
			out_col_ptr[i] += global_nnz[thread_id];
		}

		start = global_nnz[thread_id];
		finish = global_nnz[thread_id+1];

		for (i = start, j = 0; i < finish; ++i, ++j) {
			out_row_ind[i] = local_row_ind[j];
		}


		_mm_free(local_row_ind);
	}

	total_nnz = global_nnz[NUM_THREADS];
	out_col_ptr[B_n_cols] = total_nnz;

	out_values = (double*) _mm_malloc(total_nnz * sizeof(double), MEM_LINE_SIZE);

	#pragma omp parallel for schedule(static)
	for (int i = 0; i < total_nnz; ++i) {
		out_values[i] = 1.0;
	}

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = total_nnz;
	*C_n_rows = A_n_rows * B_n_rows;
	*C_n_cols = A_n_cols;

	_mm_free(global_nnz);
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_rows);
	assert(A_nnz <= A_n_cols);
	assert(B_nnz <= B_n_cols);
	#endif

	long i, nnz, row, a_col_start, b_col_start, a_col_end, b_col_end;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	nnz = A_nnz > B_nnz ? A_nnz : B_nnz;

	out_values = (double*) _mm_malloc(nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((B_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_row_ind, MEM_LINE_SIZE);
	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	__assume_aligned(B_row_ind, MEM_LINE_SIZE);
	__assume_aligned(B_col_ptr, MEM_LINE_SIZE);

	for (i = 0, nnz = 0; i < B_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		b_col_start = B_col_ptr[i];
		b_col_end = B_col_ptr[i+1];

		if (b_col_end > b_col_start) {

			row = B_row_ind[b_col_start];

			a_col_start = A_col_ptr[row];
			a_col_end = A_col_ptr[row+1];

			if (a_col_end > a_col_start) {

				out_values[nnz] = 1.0;
				out_row_ind[nnz] = A_row_ind[a_col_start];
				nnz++;
			}
		}
	}

	out_col_ptr[i] = nnz;

	*C_n_rows = A_n_rows;
	*C_n_cols = B_n_cols;
	*C_nnz = nnz;
	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_rows);
	assert(A_nnz <= A_n_cols);
	assert(B_nnz <= B_n_cols);
	#endif

	long total_nnz, nnz;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	__declspec(align(MEM_LINE_SIZE)) long *global_nnz;

	nnz = A_nnz > B_nnz ? A_nnz : B_nnz;

	out_row_ind = (long*) _mm_malloc(nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((B_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	global_nnz = (long*) _mm_malloc((NUM_THREADS+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_row_ind, MEM_LINE_SIZE);
	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	__assume_aligned(B_row_ind, MEM_LINE_SIZE);
	__assume_aligned(B_col_ptr, MEM_LINE_SIZE);

	global_nnz[0] = 0;

	#pragma omp parallel shared(out_col_ptr, out_row_ind, global_nnz)
	{
		long i, j, start, finish, nnz, row, a_col_start, b_col_start, a_col_end, b_col_end;
		int thread_id;

		__declspec(align(MEM_LINE_SIZE)) long *local_row_ind;

		local_row_ind = (long*) _mm_malloc(((B_n_cols / NUM_THREADS) + (B_n_cols % NUM_THREADS)) * sizeof(long), MEM_LINE_SIZE);

		thread_id = omp_get_thread_num();
		start = (long) (thread_id * B_n_cols) / NUM_THREADS;
		finish = (long) ((thread_id+1) * B_n_cols) / NUM_THREADS;

		for (i = start, nnz = 0; i < finish; ++i) {

			out_col_ptr[i] = nnz;

			b_col_start = B_col_ptr[i];
			b_col_end = B_col_ptr[i+1];

			if (b_col_end > b_col_start) {

				row = B_row_ind[b_col_start];

				a_col_start = A_col_ptr[row];
				a_col_end = A_col_ptr[row+1];

				if (a_col_end > a_col_start) {
					local_row_ind[nnz] = A_row_ind[a_col_start];
					nnz++;
				}
			}
		}

		global_nnz[thread_id+1] = nnz;

		#pragma omp barrier

		#pragma omp master
		for (int i = 1; i <= NUM_THREADS; ++i) {
			global_nnz[i] += global_nnz[i-1];
		}

		#pragma omp barrier

		for (i = start; i < finish; ++i) {
			out_col_ptr[i] += global_nnz[thread_id];
		}

		start = global_nnz[thread_id];
		finish = global_nnz[thread_id+1];

		for (i = start, j = 0; i < finish; ++i, ++j) {
			out_row_ind[i] = local_row_ind[j];
		}

		_mm_free(local_row_ind);
	}

	total_nnz = global_nnz[NUM_THREADS];
	out_col_ptr[B_n_cols] = total_nnz;

	out_values = (double*) _mm_malloc(total_nnz * sizeof(double), MEM_LINE_SIZE);

	#pragma omp parallel for schedule(static)
	for (int i = 0; i < total_nnz; ++i) {
		out_values[i] = 1.0;
	}

	*C_n_rows = A_n_rows;
	*C_n_cols = B_n_cols;
	*C_nnz = total_nnz;
	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;

	_mm_free(global_nnz);
}


void sp_v_bang_seq(
	double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
	long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
	long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
	long A_nnz, long A_n_rows, long A_n_cols,
	double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
	long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
	long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
	long *B_nnz, long *B_n_rows, long *B_n_cols
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == A_nnz);
	assert(A_n_rows == 1);
	#endif

	long i;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_values, MEM_LINE_SIZE);

	for (i = 0; i < A_nnz; ++i) {
		out_values[i] = 1.0 - A_values[i];
	}

	for (i = 0; i < A_nnz; ++i) {
		out_row_ind[i] = 0;
	}

	for (i = 0; i <= A_n_cols; ++i) {
		out_col_ptr[i] = i;
	}

	*B_values = out_values;
	*B_row_ind = out_row_ind;
	*B_col_ptr = out_col_ptr;
	*B_nnz = A_nnz;
	*B_n_rows = 1;
	*B_n_cols = A_n_cols;
}


void sp_v_bang_par(
	double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
	long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
	long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
	long A_nnz, long A_n_rows, long A_n_cols,
	double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
	long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
	long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
	long *B_nnz, long *B_n_rows, long *B_n_cols
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == A_nnz);
	assert(A_n_rows == 1);
	#endif

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_values, MEM_LINE_SIZE);

	#pragma omp parallel for schedule(static)
	for (long i = 0; i < A_nnz; ++i) {
		out_values[i] = 1.0 - A_values[i];
	}

	#pragma omp parallel for schedule(static)
	for (long i = 0; i < A_nnz; ++i) {
		out_row_ind[i] = 0;
	}

	#pragma omp parallel for schedule(static)
	for (long i = 0; i <= A_n_cols; ++i) {
		out_col_ptr[i] = i;
	}

	*B_values = out_values;
	*B_row_ind = out_row_ind;
	*B_col_ptr = out_col_ptr;
	*B_nnz = A_nnz;
	*B_n_rows = 1;
	*B_n_cols = A_n_cols;
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_cols);
	assert(A_n_rows == 1);
	assert(B_n_rows == 1);
	#endif

	long i, nnz, a_col_start, a_col_end, b_col_start, b_col_end;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	nnz = A_nnz < B_nnz ? A_nnz : B_nnz;

	out_values = (double*) _mm_malloc(nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((B_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	__assume_aligned(B_col_ptr, MEM_LINE_SIZE);

	for (i = 0, nnz = 0; i < B_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		a_col_start = A_col_ptr[i];
		a_col_end = A_col_ptr[i+1];

		if (a_col_end > a_col_start) {

			b_col_start = B_col_ptr[i];
			b_col_end = B_col_ptr[i+1];

			if (b_col_end > b_col_start) {

				out_values[nnz] = 1.0;
				out_row_ind[nnz] = 0;
				nnz++;
			}
		}
	}

	out_col_ptr[i] = nnz;

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = nnz;
	*C_n_rows = 1;
	*C_n_cols = A_n_cols;
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_cols);
	assert(A_n_rows == 1);
	assert(B_n_rows == 1);
	#endif

	long total_nnz;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	__declspec(align(MEM_LINE_SIZE)) long *global_nnz;

	out_col_ptr = (long*) _mm_malloc((B_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	global_nnz = (long*) _mm_malloc((NUM_THREADS+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);
	__assume_aligned(B_col_ptr, MEM_LINE_SIZE);

	global_nnz[0] = 0;

	#pragma omp parallel shared(out_col_ptr, global_nnz)
	{
		long i, start, finish, nnz, a_col_start, a_col_end, b_col_start, b_col_end;
		int thread_id;

		thread_id = omp_get_thread_num();
		start = (long) (thread_id * A_n_cols) / NUM_THREADS;
		finish = (long) ((thread_id+1) * A_n_cols) / NUM_THREADS;

		for (i = start, nnz = 0; i < finish; ++i) {

			out_col_ptr[i] = nnz;

			a_col_start = A_col_ptr[i];
			a_col_end = A_col_ptr[i+1];

			if (a_col_end > a_col_start) {

				b_col_start = B_col_ptr[i];
				b_col_end = B_col_ptr[i+1];

				if (b_col_end > b_col_start) {
					nnz++;
				}
			}
		}

		global_nnz[thread_id+1] = nnz;

		#pragma omp barrier

		#pragma omp master
		for (int i = 1; i <= NUM_THREADS; ++i) {
			global_nnz[i] += global_nnz[i-1];
		}

		#pragma omp barrier

		for (i = start; i < finish; ++i) {
			out_col_ptr[i] += global_nnz[thread_id];
		}
	}

	total_nnz = global_nnz[NUM_THREADS];
	out_col_ptr[A_n_cols] = total_nnz;

	out_row_ind = (long*) _mm_malloc(total_nnz * sizeof(long), MEM_LINE_SIZE);
	out_values = (double*) _mm_malloc(total_nnz * sizeof(double), MEM_LINE_SIZE);

	#pragma omp parallel for schedule(static)
	for (int i = 0; i < total_nnz; ++i) {
		out_row_ind[i] = 0;
	}

	#pragma omp parallel for schedule(static)
	for (int i = 0; i < total_nnz; ++i) {
		out_values[i] = 1.0;
	}

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = total_nnz;
	*C_n_rows = 1;
	*C_n_cols = A_n_cols;

	_mm_free(global_nnz);
}

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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_cols);
	assert(A_n_rows == 1);
	assert(B_n_rows == 1);
	assert(A_nnz <= B_nnz);
	#endif

	long i, nnz, a_col_start, a_col_end;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	__assume_aligned(B_values, MEM_LINE_SIZE);
	__assume_aligned(B_col_ptr, MEM_LINE_SIZE);

	for (i = 0, nnz = 0; i < A_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		a_col_start = A_col_ptr[i];
		a_col_end = A_col_ptr[i+1];

		if (a_col_end > a_col_start) {

			out_values[nnz] = B_values[B_col_ptr[i]];
			out_row_ind[nnz] = 0;
			nnz++;
		}
	}

	out_col_ptr[i] = nnz;

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = A_nnz;
	*C_n_rows = 1;
	*C_n_cols = A_n_cols;
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_cols);
	assert(A_n_rows == 1);
	assert(B_n_rows == 1);
	assert(A_nnz <= B_nnz);
	#endif

	long total_nnz;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	__declspec(align(MEM_LINE_SIZE)) long *global_nnz;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	global_nnz = (long*) _mm_malloc((NUM_THREADS+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	__assume_aligned(B_values, MEM_LINE_SIZE);
	__assume_aligned(B_col_ptr, MEM_LINE_SIZE);

	global_nnz[0] = 0;

	#pragma omp parallel shared(out_col_ptr, global_nnz)
	{
		long i, j, start, finish, nnz, a_col_start, a_col_end;
		int thread_id;

		__declspec(align(MEM_LINE_SIZE)) double *local_values;

		local_values = (double*) _mm_malloc(((A_n_cols / NUM_THREADS) + (A_n_cols % NUM_THREADS)) * sizeof(double), MEM_LINE_SIZE);

		thread_id = omp_get_thread_num();
		start = (long) (thread_id * A_n_cols) / NUM_THREADS;
		finish = (long) ((thread_id+1) * A_n_cols) / NUM_THREADS;

		for (i = start, nnz = 0; i < finish; ++i) {

			out_col_ptr[i] = nnz;

			a_col_start = A_col_ptr[i];
			a_col_end = A_col_ptr[i+1];

			if (a_col_end > a_col_start) {

				local_values[nnz] = B_values[B_col_ptr[i]];
				nnz++;
			}
		}

		global_nnz[thread_id+1] = nnz;

		#pragma omp barrier

		#pragma omp master
		for (int i = 1; i <= NUM_THREADS; ++i) {
			global_nnz[i] += global_nnz[i-1];
		}

		#pragma omp barrier

		for (i = start; i < finish; ++i) {
			out_col_ptr[i] += global_nnz[thread_id];
		}

		start = global_nnz[thread_id];
		finish = global_nnz[thread_id+1];

		for (i = start, j = 0; i < finish; ++i, ++j) {
			out_values[i] = local_values[j];
		}

		_mm_free(local_values);
	}

	total_nnz = global_nnz[NUM_THREADS];
	out_col_ptr[A_n_cols] = A_nnz;

	#pragma omp parallel for schedule(static)
	for (int i = 0; i < total_nnz; ++i) {
		out_row_ind[i] = 0;
	}

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = A_nnz;
	*C_n_rows = 1;
	*C_n_cols = A_n_cols;

	_mm_free(global_nnz);
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_cols);
	assert(A_n_rows == 1);
	assert(B_n_rows == 1);
	assert(A_nnz == B_nnz);
	#endif

	long i, nnz;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	nnz = A_nnz > B_nnz ? A_nnz : B_nnz;

	out_values = (double*) _mm_malloc(nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((B_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_values, MEM_LINE_SIZE);

	__assume_aligned(B_values, MEM_LINE_SIZE);

	for (i = 0; i < A_nnz; ++i) {
		out_values[i] = A_values[i] * B_values[i];
	}

	for (i = 0; i < A_nnz; ++i) {
		out_row_ind[i] = 0;
	}

	for (i = 0; i <= A_n_cols; ++i) {
		out_col_ptr[i] = i;
	}

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = nnz;
	*C_n_rows = 1;
	*C_n_cols = A_n_cols;
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_cols);
	assert(A_n_rows == 1);
	assert(B_n_rows == 1);
	assert(A_nnz == B_nnz);
	#endif

	long nnz;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	nnz = A_nnz > B_nnz ? A_nnz : B_nnz;

	out_values = (double*) _mm_malloc(nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((B_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_values, MEM_LINE_SIZE);

	__assume_aligned(B_values, MEM_LINE_SIZE);

	#pragma omp parallel for schedule(static)
	for (long i = 0; i < A_nnz; ++i) {
		out_values[i] = A_values[i] * B_values[i];
	}

	#pragma omp parallel for schedule(static)
	for (long i = 0; i < A_nnz; ++i) {
		out_row_ind[i] = 0;
	}

	#pragma omp parallel for schedule(static)
	for (long i = 0; i <= A_n_cols; ++i) {
		out_col_ptr[i] = i;
	}

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = nnz;
	*C_n_rows = 1;
	*C_n_cols = A_n_cols;
}

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
    ) {


	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_cols);
	assert(A_n_rows == 1);
	assert(B_n_rows == 1);
	#endif

	long i, nnz, a_col_start, a_col_end, b_col_start, b_col_end;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	out_values = (double*) _mm_malloc(B_n_cols * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(B_n_cols * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((B_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);
	__assume_aligned(B_col_ptr, MEM_LINE_SIZE);

	for (i = 0, nnz = 0; i < B_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		a_col_start = A_col_ptr[i];
		a_col_end = A_col_ptr[i+1];

		if (a_col_end > a_col_start) {

			out_values[nnz] = 1.0;
			out_row_ind[nnz] = 0;
			nnz++;

		} else {

			b_col_start = B_col_ptr[i];
			b_col_end = B_col_ptr[i+1];

			if (b_col_end > b_col_start) {

				out_values[nnz] = 1.0;
				out_row_ind[nnz] = 0;
				nnz++;
			}
		}
	}

	out_col_ptr[i] = nnz;

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = nnz;
	*C_n_rows = 1;
	*C_n_cols = A_n_cols;
}


void sp_transpose(
	double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
	long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
	long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
	long A_nnz, long A_n_rows, long A_n_cols,
	double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
	long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
	long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
	long *B_nnz, long *B_n_rows, long *B_n_cols
	) {

	long i, j, end_col;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	__declspec(align(MEM_LINE_SIZE)) long *temp;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_rows+1) * sizeof(long), MEM_LINE_SIZE);

	temp = (long*) _mm_malloc((A_n_rows+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_values, MEM_LINE_SIZE);
	__assume_aligned(A_row_ind, MEM_LINE_SIZE);
	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	for (i = 0; i <= A_n_rows; ++i) {
		temp[i] = 0;
	}

	for (i = 0; i < A_n_cols; ++i) {
		end_col = A_col_ptr[i+1];
		for (j = A_col_ptr[i]; j < end_col; ++j)
			++temp[A_row_ind[j]+1];
	}

	out_col_ptr[0] = 0;

	for (i = 1; i <= A_n_rows; ++i) {
		out_col_ptr[i] = out_col_ptr[i-1] + temp[i];
	}

	for (i = 1; i <= A_n_rows; ++i) {
		temp[i] = out_col_ptr[i];
	}

	for (i = 0; i < A_n_cols; ++i) {
		end_col = A_col_ptr[i+1];
		for (j = A_col_ptr[i]; j < end_col; ++j) {
			out_row_ind[temp[A_row_ind[j]]] = i;
			out_values[temp[A_row_ind[j]]++] = A_values[j];
		}
	}

	*B_values = out_values;
	*B_row_ind = out_row_ind;
	*B_col_ptr = out_col_ptr;
	*B_nnz = A_nnz;
	*B_n_rows = A_n_cols;
	*B_n_cols = A_n_rows;

	_mm_free(temp);
}

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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_cols);
	assert(A_nnz <= A_n_cols);
	assert(B_nnz <= B_n_cols);
	assert(B_n_rows == 1);
	#endif

	long i, nnz, a_col_start, a_col_end;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_row_ind, MEM_LINE_SIZE);
	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	__assume_aligned(B_values, MEM_LINE_SIZE);

	for (i = 0, nnz = 0; i < A_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		a_col_start = A_col_ptr[i];
		a_col_end = A_col_ptr[i+1];

		if (a_col_end > a_col_start) {

			out_values[nnz] = B_values[i];
			out_row_ind[nnz] = A_row_ind[a_col_start];
			nnz++;
		}
	}

	out_col_ptr[i] = nnz;

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = nnz;
	*C_n_rows = A_n_rows;
	*C_n_cols = A_n_cols;
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == B_n_cols);
	assert(A_nnz <= A_n_cols);
	assert(B_nnz <= B_n_cols);
	assert(B_n_rows == 1);
	#endif

	long total_nnz;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	__declspec(align(MEM_LINE_SIZE)) long *global_nnz;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	global_nnz = (long*) _mm_malloc((NUM_THREADS+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_row_ind, MEM_LINE_SIZE);
	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	__assume_aligned(B_values, MEM_LINE_SIZE);

	global_nnz[0] = 0;

	#pragma omp parallel shared(out_col_ptr, out_row_ind, out_values, global_nnz)
	{
		long i, j, start, finish, nnz, a_col_start, a_col_end;
		int thread_id;

		__declspec(align(MEM_LINE_SIZE)) long *local_row_ind;
		__declspec(align(MEM_LINE_SIZE)) double *local_values;

		local_row_ind = (long*) _mm_malloc(((A_n_cols / NUM_THREADS) + (A_n_cols % NUM_THREADS)) * sizeof(long), MEM_LINE_SIZE);
		local_values = (double*) _mm_malloc(((A_n_cols / NUM_THREADS) + (A_n_cols % NUM_THREADS)) * sizeof(double), MEM_LINE_SIZE);

		thread_id = omp_get_thread_num();
		start = (long) (thread_id * A_n_cols) / NUM_THREADS;
		finish = (long) ((thread_id+1) * A_n_cols) / NUM_THREADS;

		for (i = start, nnz = 0; i < finish; ++i) {

			out_col_ptr[i] = nnz;

			a_col_start = A_col_ptr[i];
			a_col_end = A_col_ptr[i+1];

			if (a_col_end > a_col_start) {

				local_values[nnz] = B_values[i];
				local_row_ind[nnz] = A_row_ind[a_col_start];
				nnz++;
			}
		}

		global_nnz[thread_id+1] = nnz;

		#pragma omp barrier

		#pragma omp master
		for (int i = 1; i <= NUM_THREADS; ++i) {
			global_nnz[i] += global_nnz[i-1];
		}

		#pragma omp barrier

		for (i = start; i < finish; ++i) {
			out_col_ptr[i] += global_nnz[thread_id];
		}

		start = global_nnz[thread_id];
		finish = global_nnz[thread_id+1];

		for (i = start, j = 0; i < finish; ++i, ++j) {
			out_row_ind[i] = local_row_ind[j];
		}

		for (i = start, j = 0; i < finish; ++i, ++j) {
			out_values[i] = local_values[j];
		}

		_mm_free(local_row_ind);
		_mm_free(local_values);
	}

	total_nnz = global_nnz[NUM_THREADS];
	out_col_ptr[A_n_cols] = total_nnz;

	*C_values = out_values;
	*C_row_ind = out_row_ind;
	*C_col_ptr = out_col_ptr;
	*C_nnz = total_nnz;
	*C_n_rows = A_n_rows;
	*C_n_cols = A_n_cols;

	_mm_free(global_nnz);
}


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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == A_nnz);
	assert(A_n_rows == 1);
	#endif

	long i;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_values, MEM_LINE_SIZE);

	for (i = 0; i < A_nnz; ++i) {
		out_values[i] = A_values[i] * x;
	}

	for (i = 0; i < A_nnz; ++i) {
		out_row_ind[i] = 0;
	}

	for (i = 0; i <= A_n_cols; ++i) {
		out_col_ptr[i] = i;
	}

	*B_values = out_values;
	*B_row_ind = out_row_ind;
	*B_col_ptr = out_col_ptr;
	*B_nnz = A_nnz;
	*B_n_rows = 1;
	*B_n_cols = A_n_cols;
}

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
	) {

	#ifdef D_DEBUGGING
	assert(A_n_cols == A_nnz);
	assert(A_n_rows == 1);
	#endif

	long i;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	out_values = (double*) _mm_malloc(A_nnz * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(A_nnz * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_values, MEM_LINE_SIZE);

	for (i = 0; i < A_nnz; ++i) {
		out_values[i] = A_values[i] / x;
	}

	for (i = 0; i < A_nnz; ++i) {
		out_row_ind[i] = 0;
	}

	for (i = 0; i <= A_n_cols; ++i) {
		out_col_ptr[i] = i;
	}

	*B_values = out_values;
	*B_row_ind = out_row_ind;
	*B_col_ptr = out_col_ptr;
	*B_nnz = A_nnz;
	*B_n_rows = 1;
	*B_n_cols = A_n_cols;
}

void sp_bv_map_not(
	double * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_values,
	long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_row_ind,
	long * __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) A_col_ptr,
	long A_nnz, long A_n_rows, long A_n_cols,
	double ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_values,
	long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_row_ind,
	long ** __restrict__  __attribute__((aligned (MEM_LINE_SIZE))) B_col_ptr,
	long *B_nnz, long *B_n_rows, long *B_n_cols
	) {

	#ifdef D_DEBUGGING
	assert(A_n_rows == 1);
	#endif

	long i, nnz;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	out_col_ptr = (long*) _mm_malloc((A_n_cols+1) * sizeof(long), MEM_LINE_SIZE);

	__assume_aligned(A_col_ptr, MEM_LINE_SIZE);

	for (i = 0, nnz = 0; i < A_n_cols; ++i) {

		out_col_ptr[i] = nnz;

		if (A_col_ptr[i] == A_col_ptr[i+1]) {
			nnz++;
		}
	}

	out_col_ptr[i] = nnz;

	out_row_ind = (long*) _mm_malloc(nnz * sizeof(long), MEM_LINE_SIZE);
	out_values = (double*) _mm_malloc(nnz * sizeof(double), MEM_LINE_SIZE);

	for (i = 0; i < nnz; ++i) {
		out_row_ind[i] = 0;
	}

	for (i = 0; i < nnz; ++i) {
		out_values[i] = 1.0;
	}

	*B_values = out_values;
	*B_row_ind = out_row_ind;
	*B_col_ptr = out_col_ptr;
	*B_nnz = nnz;
	*B_n_rows = 1;
	*B_n_cols = A_n_cols;
}
