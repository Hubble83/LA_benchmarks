#include <stdio.h>
#include <glib.h>
#include <math.h>

#include "../common/timer.h"
#include "../common/tools.h"
#include "../common/strings.h"
#include "../common/operators.h"
#include "../common/altavl.h"

#ifdef D_PROFILE
struct _profile {
	double measurements[8];
	int curr_op;
};

typedef struct _profile profile;

profile prof;

#define START_PROFILE() { \
	int i; \
	for (i = 0; i < 8; ++i) \
		prof.measurements[i] = 0.0; \
	prof.curr_op = 0; \
}

#define START() { \
	GET_TIME(prof.measurements[prof.curr_op]); \
}

#define STOP() { \
	double finish; \
	GET_TIME(finish); \
	prof.measurements[prof.curr_op] = finish - prof.measurements[prof.curr_op]; \
	prof.curr_op++; \
}

#define STOP_PROFILE() { \
	int i; \
	for (i = 0; i < prof.curr_op; i++) \
		printf("%f\n", prof.measurements[i]); \
}
#endif

int main(int argc, char *argv[]) {

	double start, finish, elapsed;

	int dataset;

	#ifdef D_PROFILE
	START_PROFILE();
	#endif

	long num_elems[8][8] = {
							{  150000,   300000,   600000,  1200000,  2400000,   4800000,   9600000,  19200000 }, // customer
							{ 1500000,  3000000,  6000000, 12000000, 24000000,  48000000,  96000000, 192000000 }, // orders
							{ 6001215, 11997996, 23996604, 47989007, 95988640, 192000551, 384016850, 768046938 }, // lineitem
							{      25,       25,       25,       25,       25,        25,        25,        25 }, // nation
							{   10000,    20000,    40000,    80000,   160000,    320000,    640000,   1280000 }, // supplier
							{  800000,  1600000,  3200000,  6400000, 12800000,  25600000,  51200000, 102400000 }, // partsupp
							{  200000,   400000,   800000,  1600000,  3200000,   6400000,  12800000,  25600000 }, // part
							{       5,        5,        5,        5,        5,         5,         5,         5 }  // region
						   };

	char *dataset_path = getenv("LA_DATA_DIR");

	char nation_1[100], nation_2[100];
	char supplier_1[100], supplier_4[100];
	char partsupp_1[100], partsupp_2[100], partsupp_3[100], partsupp_4[100];
	char part_1[100];

	if (argc != 2) {
		fprintf(stderr, "Wrong number of arguments\n%s <dataset>\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	dataset = (int) log2 ((double) atoi(argv[1]));

	// Nation

	// n_nationkey
	strcpy(nation_1, dataset_path);
	strcat(nation_1, argv[1]);
	strcat(nation_1, "/nation_cols/nation_1.tbl");

	// n_name
	strcpy(nation_2, dataset_path);
	strcat(nation_2, argv[1]);
	strcat(nation_2, "/nation_cols/nation_2.tbl");

	// Supplier

	// s_suppkey
	strcpy(supplier_1, dataset_path);
	strcat(supplier_1, argv[1]);
	strcat(supplier_1, "/supplier_cols/supplier_1.tbl");

	// s_nationkey
	strcpy(supplier_4, dataset_path);
	strcat(supplier_4, argv[1]);
	strcat(supplier_4, "/supplier_cols/supplier_4.tbl");

	// PartSupp

	// ps_partkey
	strcpy(partsupp_1, dataset_path);
	strcat(partsupp_1, argv[1]);
	strcat(partsupp_1, "/partsupp_cols/partsupp_1.tbl");

	// ps_suppkey
	strcpy(partsupp_2, dataset_path);
	strcat(partsupp_2, argv[1]);
	strcat(partsupp_2, "/partsupp_cols/partsupp_2.tbl");

	// ps_availqty
	strcpy(partsupp_3, dataset_path);
	strcat(partsupp_3, argv[1]);
	strcat(partsupp_3, "/partsupp_cols/partsupp_3.tbl");

	// ps_supplycost
	strcpy(partsupp_4, dataset_path);
	strcat(partsupp_4, argv[1]);
	strcat(partsupp_4, "/partsupp_cols/partsupp_4.tbl");

	// Part

	// p_partkey
	strcpy(part_1, dataset_path);
	strcat(part_1, argv[1]);
	strcat(part_1, "/part_cols/part_1.tbl");

	// Declare Variables

	// Nation

	// n_nationkey
	__declspec(align(MEM_LINE_SIZE)) double *n_nationkey_values;
	__declspec(align(MEM_LINE_SIZE)) long *n_nationkey_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *n_nationkey_col_ptr;

	long n_nationkey_n_rows;
	long n_nationkey_n_cols;
	long n_nationkey_nnz;

	// n_name
	__declspec(align(MEM_LINE_SIZE)) double *n_name_values;
	__declspec(align(MEM_LINE_SIZE)) long *n_name_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *n_name_col_ptr;

	long n_name_n_rows;
	long n_name_n_cols;
	long n_name_nnz;

	// Supplier

	// s_suppkey
	__declspec(align(MEM_LINE_SIZE)) double *s_suppkey_values;
	__declspec(align(MEM_LINE_SIZE)) long *s_suppkey_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *s_suppkey_col_ptr;

	long s_suppkey_n_rows;
	long s_suppkey_n_cols;
	long s_suppkey_nnz;

	// s_nationkey
	__declspec(align(MEM_LINE_SIZE)) double *s_nationkey_values;
	__declspec(align(MEM_LINE_SIZE)) long *s_nationkey_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *s_nationkey_col_ptr;

	long s_nationkey_n_rows;
	long s_nationkey_n_cols;
	long s_nationkey_nnz;

	// PartSupp

	// ps_partkey
	__declspec(align(MEM_LINE_SIZE)) double *ps_partkey_values;
	__declspec(align(MEM_LINE_SIZE)) long *ps_partkey_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *ps_partkey_col_ptr;

	long ps_partkey_n_rows;
	long ps_partkey_n_cols;
	long ps_partkey_nnz;

	// ps_suppkey
	__declspec(align(MEM_LINE_SIZE)) double *ps_suppkey_values;
	__declspec(align(MEM_LINE_SIZE)) long *ps_suppkey_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *ps_suppkey_col_ptr;

	long ps_suppkey_n_rows;
	long ps_suppkey_n_cols;
	long ps_suppkey_nnz;

	// ps_availqty
	__declspec(align(MEM_LINE_SIZE)) double *ps_availqty_values;
	__declspec(align(MEM_LINE_SIZE)) long *ps_availqty_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *ps_availqty_col_ptr;

	long ps_availqty_n_rows;
	long ps_availqty_n_cols;
	long ps_availqty_nnz;

	// ps_supplycost
	__declspec(align(MEM_LINE_SIZE)) double *ps_supplycost_values;
	__declspec(align(MEM_LINE_SIZE)) long *ps_supplycost_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *ps_supplycost_col_ptr;

	long ps_supplycost_n_rows;
	long ps_supplycost_n_cols;
	long ps_supplycost_nnz;

	// Part

	// p_partkey
	__declspec(align(MEM_LINE_SIZE)) double *p_partkey_values;
	__declspec(align(MEM_LINE_SIZE)) long *p_partkey_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *p_partkey_col_ptr;

	long p_partkey_n_rows;
	long p_partkey_n_cols;
	long p_partkey_nnz;

	// Auxiliar Variables

	// A
	__declspec(align(MEM_LINE_SIZE)) double *A_values;
	__declspec(align(MEM_LINE_SIZE)) long *A_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *A_col_ptr;

	long A_n_rows;
	long A_n_cols;
	long A_nnz;

	// B
	__declspec(align(MEM_LINE_SIZE)) double *B_values;
	__declspec(align(MEM_LINE_SIZE)) long *B_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *B_col_ptr;

	long B_n_rows;
	long B_n_cols;
	long B_nnz;

	// C
	__declspec(align(MEM_LINE_SIZE)) double *C_values;
	__declspec(align(MEM_LINE_SIZE)) long *C_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *C_col_ptr;

	long C_n_rows;
	long C_n_cols;
	long C_nnz;

	// D
	__declspec(align(MEM_LINE_SIZE)) double *D_values;
	__declspec(align(MEM_LINE_SIZE)) long *D_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *D_col_ptr;

	long D_n_rows;
	long D_n_cols;
	long D_nnz;

	// E
	__declspec(align(MEM_LINE_SIZE)) double *E_values;
	__declspec(align(MEM_LINE_SIZE)) long *E_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *E_col_ptr;

	long E_n_rows;
	long E_n_cols;
	long E_nnz;

	// F
	Tree F = createSumTree();

	__declspec(align(MEM_LINE_SIZE)) double *F_values;
	__declspec(align(MEM_LINE_SIZE)) long *F_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *F_col_ptr;

	long F_n_rows;
	long F_n_cols;
	long F_nnz;

	// G
	__declspec(align(MEM_LINE_SIZE)) double *G_values;
	__declspec(align(MEM_LINE_SIZE)) long *G_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *G_col_ptr;

	long G_n_rows;
	long G_n_cols;
	long G_nnz;

	// H
	__declspec(align(MEM_LINE_SIZE)) double *H_values;
	__declspec(align(MEM_LINE_SIZE)) long *H_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *H_col_ptr;

	long H_n_rows;
	long H_n_cols;
	long H_nnz;

	// I
	__declspec(align(MEM_LINE_SIZE)) double *I_values;
	__declspec(align(MEM_LINE_SIZE)) long *I_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *I_col_ptr;

	long I_n_rows;
	long I_n_cols;
	long I_nnz;

	// J
	Tree J = createSumTree();

	__declspec(align(MEM_LINE_SIZE)) double *J_values;
	__declspec(align(MEM_LINE_SIZE)) long *J_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *J_col_ptr;

	long J_n_rows;
	long J_n_cols;
	long J_nnz;

	// L
	__declspec(align(MEM_LINE_SIZE)) double *L_values;
	__declspec(align(MEM_LINE_SIZE)) long *L_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *L_col_ptr;

	long L_n_rows;
	long L_n_cols;
	long L_nnz;

	// Labels

	// nationkey
	GHashTable *nationkey_id_label = NULL, *nationkey_label_id = NULL;
	long nationkey_next_id_label;

	// n_name
	GHashTable *n_name_id_label = NULL, *n_name_label_id = NULL;
	long n_name_next_id_label;

	// suppkey
	GHashTable *suppkey_id_label = NULL, *suppkey_label_id = NULL;
	long suppkey_next_id_label;

	// partkey
	GHashTable *partkey_id_label = NULL, *partkey_label_id = NULL;
	long partkey_next_id_label;

	// Load Dataset

	#ifdef D_VERBOSE
	printf("\nLoad Dataset\n\n");
	#endif

	// n_nationkey
	read_column(
		nation_1, num_elems[3][dataset],
		&n_nationkey_values, &n_nationkey_row_ind, &n_nationkey_col_ptr,
		&n_nationkey_nnz, &n_nationkey_n_rows, &n_nationkey_n_cols,
		&nationkey_id_label, &nationkey_label_id, &nationkey_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"n_nationkey",
    	n_nationkey_values, n_nationkey_row_ind, n_nationkey_col_ptr,
		n_nationkey_nnz, n_nationkey_n_rows, n_nationkey_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "n_nationkey", n_nationkey_n_rows, n_nationkey_n_cols, n_nationkey_nnz);
	#endif

	// free n_nationkey
	_mm_free(n_nationkey_col_ptr);
	_mm_free(n_nationkey_row_ind);
	_mm_free(n_nationkey_values);

	// n_name
	read_column(
		nation_2, num_elems[3][dataset],
		&n_name_values, &n_name_row_ind, &n_name_col_ptr,
		&n_name_nnz, &n_name_n_rows, &n_name_n_cols,
		&n_name_id_label, &n_name_label_id, &n_name_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"n_name",
    	n_name_values, n_name_row_ind, n_name_col_ptr,
		n_name_nnz, n_name_n_rows, n_name_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "n_name", n_name_n_rows, n_name_n_cols, n_name_nnz);
	#endif

	// s_suppkey
	read_column(
		supplier_1, num_elems[4][dataset],
		&s_suppkey_values, &s_suppkey_row_ind, &s_suppkey_col_ptr,
		&s_suppkey_nnz, &s_suppkey_n_rows, &s_suppkey_n_cols,
		&suppkey_id_label, &suppkey_label_id, &suppkey_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"s_suppkey",
    	s_suppkey_values, s_suppkey_row_ind, s_suppkey_col_ptr,
		s_suppkey_nnz, s_suppkey_n_rows, s_suppkey_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "s_suppkey", s_suppkey_n_rows, s_suppkey_n_cols, s_suppkey_nnz);
	#endif

	// free s_suppkey
	_mm_free(s_suppkey_col_ptr);
	_mm_free(s_suppkey_row_ind);
	_mm_free(s_suppkey_values);

	// s_nationkey
	read_column(
		supplier_4, num_elems[4][dataset],
		&s_nationkey_values, &s_nationkey_row_ind, &s_nationkey_col_ptr,
		&s_nationkey_nnz, &s_nationkey_n_rows, &s_nationkey_n_cols,
		&nationkey_id_label, &nationkey_label_id, &nationkey_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"s_nationkey",
    	s_nationkey_values, s_nationkey_row_ind, s_nationkey_col_ptr,
		s_nationkey_nnz, s_nationkey_n_rows, s_nationkey_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "s_nationkey", s_nationkey_n_rows, s_nationkey_n_cols, s_nationkey_nnz);
	#endif

	// p_partkey
	read_column(
		part_1, num_elems[6][dataset],
		&p_partkey_values, &p_partkey_row_ind, &p_partkey_col_ptr,
		&p_partkey_nnz, &p_partkey_n_rows, &p_partkey_n_cols,
		&partkey_id_label, &partkey_label_id, &partkey_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"p_partkey",
    	p_partkey_values, p_partkey_row_ind, p_partkey_col_ptr,
		p_partkey_nnz, p_partkey_n_rows, p_partkey_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "p_partkey", p_partkey_n_rows, p_partkey_n_cols, p_partkey_nnz);
	#endif

	// free p_partkey
	_mm_free(p_partkey_col_ptr);
	_mm_free(p_partkey_row_ind);
	_mm_free(p_partkey_values);

	// ps_partkey
	read_column(
		partsupp_1, num_elems[5][dataset],
		&ps_partkey_values, &ps_partkey_row_ind, &ps_partkey_col_ptr,
		&ps_partkey_nnz, &ps_partkey_n_rows, &ps_partkey_n_cols,
		&partkey_id_label, &partkey_label_id, &partkey_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"ps_partkey",
    	ps_partkey_values, ps_partkey_row_ind, ps_partkey_col_ptr,
		ps_partkey_nnz, ps_partkey_n_rows, ps_partkey_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "ps_partkey", ps_partkey_n_rows, ps_partkey_n_cols, ps_partkey_nnz);
	#endif

	// ps_suppkey
	read_column(
		partsupp_2, num_elems[5][dataset],
		&ps_suppkey_values, &ps_suppkey_row_ind, &ps_suppkey_col_ptr,
		&ps_suppkey_nnz, &ps_suppkey_n_rows, &ps_suppkey_n_cols,
		&suppkey_id_label, &suppkey_label_id, &suppkey_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"ps_suppkey",
    	ps_suppkey_values, ps_suppkey_row_ind, ps_suppkey_col_ptr,
		ps_suppkey_nnz, ps_suppkey_n_rows, ps_suppkey_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "ps_suppkey", ps_suppkey_n_rows, ps_suppkey_n_cols, ps_suppkey_nnz);
	#endif

	// ps_availqty
	read_column_measure(
		partsupp_3, num_elems[5][dataset],
		&ps_availqty_values, &ps_availqty_row_ind, &ps_availqty_col_ptr,
		&ps_availqty_nnz, &ps_availqty_n_rows, &ps_availqty_n_cols
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"ps_availqty",
    	ps_availqty_values, ps_availqty_row_ind, ps_availqty_col_ptr,
		ps_availqty_nnz, ps_availqty_n_rows, ps_availqty_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "ps_availqty", ps_availqty_n_rows, ps_availqty_n_cols, ps_availqty_nnz);
	#endif

	// ps_supplycost
	read_column_measure(
		partsupp_4, num_elems[5][dataset],
		&ps_supplycost_values, &ps_supplycost_row_ind, &ps_supplycost_col_ptr,
		&ps_supplycost_nnz, &ps_supplycost_n_rows, &ps_supplycost_n_cols
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"ps_supplycost",
    	ps_supplycost_values, ps_supplycost_row_ind, ps_supplycost_col_ptr,
		ps_supplycost_nnz, ps_supplycost_n_rows, ps_supplycost_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "ps_supplycost", ps_supplycost_n_rows, ps_supplycost_n_cols, ps_supplycost_nnz);
	#endif

	// Execute Query 11

	#ifdef D_VERBOSE
	printf("\nQuery 11\n\n");
	#endif

	GET_TIME(start);

	#ifdef D_PROFILE
	START();
	#endif

	// A = filter( n_name = 'GERMANY' )
	sp_bm_bv_filter_seq(
    	n_name_values, n_name_row_ind, n_name_col_ptr,
    	n_name_nnz, n_name_n_rows, n_name_n_cols,
    	n_name_id_label,
    	(int(*)(const void*,const void*)) strcmp,
    	EQUAL, "GERMANY",
    	&A_values, &A_row_ind, &A_col_ptr,
	    &A_nnz, &A_n_rows, &A_n_cols
	    );

	#ifdef D_PROFILE
	STOP();
	#endif

	// free n_name
	_mm_free(n_name_col_ptr);
	_mm_free(n_name_row_ind);
	_mm_free(n_name_values);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "A", A_n_rows, A_n_cols, A_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// B = dot( A, s_nationkey )
	sp_bvbm_dot_product_seq(
		A_values, A_row_ind, A_col_ptr,
    	A_nnz, A_n_rows, A_n_cols,
    	s_nationkey_values, s_nationkey_row_ind, s_nationkey_col_ptr,
    	s_nationkey_nnz, s_nationkey_n_rows, s_nationkey_n_cols,
	    &B_values, &B_row_ind, &B_col_ptr,
	    &B_nnz, &B_n_rows, &B_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free A
	_mm_free(A_col_ptr);
	_mm_free(A_row_ind);
	_mm_free(A_values);

	// free s_nationkey
	_mm_free(s_nationkey_col_ptr);
	_mm_free(s_nationkey_row_ind);
	_mm_free(s_nationkey_values);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "B", B_n_rows, B_n_cols, B_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// C = dot( B, ps_suppkey )
	sp_bvbm_dot_product_seq(
		B_values, B_row_ind, B_col_ptr,
    	B_nnz, B_n_rows, B_n_cols,
    	ps_suppkey_values, ps_suppkey_row_ind, ps_suppkey_col_ptr,
    	ps_suppkey_nnz, ps_suppkey_n_rows, ps_suppkey_n_cols,
	    &C_values, &C_row_ind, &C_col_ptr,
	    &C_nnz, &C_n_rows, &C_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free B
	_mm_free(B_values);
	_mm_free(B_row_ind);
	_mm_free(B_col_ptr);

	// free ps_suppkey
	_mm_free(ps_suppkey_values);
	_mm_free(ps_suppkey_row_ind);
	_mm_free(ps_suppkey_col_ptr);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "C", C_n_rows, C_n_cols, C_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// D = hadamard( ps_supplycost, ps_availqty )
	sp_vv_hadamard_seq(
		ps_supplycost_values, ps_supplycost_row_ind, ps_supplycost_col_ptr,
	    ps_supplycost_nnz, ps_supplycost_n_rows, ps_supplycost_n_cols,
	    ps_availqty_values, ps_availqty_row_ind, ps_availqty_col_ptr,
	    ps_availqty_nnz, ps_availqty_n_rows, ps_availqty_n_cols,
	    &D_values, &D_row_ind, &D_col_ptr,
	    &D_nnz, &D_n_rows, &D_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free ps_supplycost
	_mm_free(ps_supplycost_values);
	_mm_free(ps_supplycost_row_ind);
	_mm_free(ps_supplycost_col_ptr);

	// free ps_availqty
	_mm_free(ps_availqty_values);
	_mm_free(ps_availqty_row_ind);
	_mm_free(ps_availqty_col_ptr);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "D", D_n_rows, D_n_cols, D_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// E = hadamard( C, D )
	sp_bvv_hadamard_seq(
		C_values, C_row_ind, C_col_ptr,
	    C_nnz, C_n_rows, C_n_cols,
	    D_values, D_row_ind, D_col_ptr,
	    D_nnz, D_n_rows, D_n_cols,
	    &E_values, &E_row_ind, &E_col_ptr,
	    &E_nnz, &E_n_rows, &E_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "E", E_n_rows, E_n_cols, E_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// F = sum( E )
	F = fromCSCtoSumTree(F,
		E_col_ptr, E_row_ind, E_values,
	    E_n_cols, E_n_rows, E_nnz
		);

	fromSumTreeToCSC(F, E_n_rows,
		&F_col_ptr, &F_row_ind, &F_values,
		&F_n_cols, &F_n_rows, &F_nnz
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free E
	_mm_free(E_values);
	_mm_free(E_row_ind);
	_mm_free(E_col_ptr);

	// free F
	emptyTree(F);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "F", F_n_rows, F_n_cols, F_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// G = map( F * 0.0001 )
	sp_v_map_mult(
		0.0001,
	    F_values, F_row_ind, F_col_ptr,
	    F_nnz, F_n_rows, F_n_cols,
	    &G_values, &G_row_ind, &G_col_ptr,
	    &G_nnz, &G_n_rows, &G_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free F
	_mm_free(F_values);
	_mm_free(F_row_ind);
	_mm_free(F_col_ptr);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "G", G_n_rows, G_n_cols, G_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// H = krao( ps_partkey, C)
	sp_bmbv_krao_seq(
		ps_partkey_values, ps_partkey_row_ind, ps_partkey_col_ptr,
		ps_partkey_nnz, ps_partkey_n_rows, ps_partkey_n_cols,
		C_values, C_row_ind, C_col_ptr,
		C_nnz, C_n_rows, C_n_cols,
		&H_values, &H_row_ind, &H_col_ptr,
	    &H_nnz, &H_n_rows, &H_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free ps_partkey
	_mm_free(ps_partkey_values);
	_mm_free(ps_partkey_row_ind);
	_mm_free(ps_partkey_col_ptr);

	// free C
	_mm_free(C_values);
	_mm_free(C_row_ind);
	_mm_free(C_col_ptr);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "H", H_n_rows, H_n_cols, H_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// I = krao( H, D )
	sp_bmv_krao_seq(
		H_values, H_row_ind, H_col_ptr,
		H_nnz, H_n_rows, H_n_cols,
		D_values, D_row_ind, D_col_ptr,
		D_nnz, D_n_rows, D_n_cols,
	    &I_values, &I_row_ind, &I_col_ptr,
	    &I_nnz, &I_n_rows, &I_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free H
	_mm_free(H_values);
	_mm_free(H_row_ind);
	_mm_free(H_col_ptr);

	// free D
	_mm_free(D_values);
	_mm_free(D_row_ind);
	_mm_free(D_col_ptr);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "I", I_n_rows, I_n_cols, I_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// J = sum( I )
	J = fromCSCtoSumTree(J,
		I_col_ptr, I_row_ind, I_values,
	    I_n_cols, I_n_rows, I_nnz
		);

	fromSumTreeToCSC(J, I_n_rows,
		&J_col_ptr, &J_row_ind, &J_values,
		&J_n_cols, &J_n_rows, &J_nnz
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free J
	emptyTree(J);

	// free I
	_mm_free(I_values);
	_mm_free(I_row_ind);
	_mm_free(I_col_ptr);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "J", J_n_rows, J_n_cols, J_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// K = filter( J > G )
	// L = hadamard( J, K )
	filtro_vetor_coluna(
				J_values, J_row_ind, J_col_ptr,
			    J_nnz, J_n_rows, J_n_cols,
			    GREATER, G_values[0],
			    &L_values, &L_row_ind, &L_col_ptr,
			    &L_nnz, &L_n_rows, &L_n_cols
				);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free J
	_mm_free(J_values);
	_mm_free(J_row_ind);
	_mm_free(J_col_ptr);

	// free G
	_mm_free(G_values);
	_mm_free(G_row_ind);
	_mm_free(G_col_ptr);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "L", L_n_rows, L_n_cols, L_nnz);
	#endif

	double cenas = 0;
	for (int i = 0; i < L_nnz; ++i)
		cenas += L_values[i];
	printf("SUM = %f\n", cenas);

	GET_TIME(finish);
	elapsed = finish - start;
	printf("Execution Time:\t%f\nNNZ:\t%ld\n", elapsed, L_nnz);

	// free J
	_mm_free(L_values);
	_mm_free(L_row_ind);
	_mm_free(L_col_ptr);

	// Free Labels

	// nationkey
	g_hash_table_destroy(nationkey_id_label);
	g_hash_table_destroy(nationkey_label_id);

	// n_name
	g_hash_table_destroy(n_name_id_label);
	g_hash_table_destroy(n_name_label_id);

	// suppkey
	g_hash_table_destroy(suppkey_id_label);
	g_hash_table_destroy(suppkey_label_id);

	// partkey
	g_hash_table_destroy(partkey_id_label);
	g_hash_table_destroy(partkey_label_id);

	return EXIT_SUCCESS;
}
