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

	char part_1[100], part_5[100];
	char lineitem_2[100], lineitem_6[100], lineitem_7[100], lineitem_11[100];

	if (argc != 2) {
		fprintf(stderr, "Wrong number of arguments\n%s <dataset>\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	dataset = (int) log2 ((double) atoi(argv[1]));

	// Part

	// p_partkey
	strcpy(part_1, dataset_path);
	strcat(part_1, argv[1]);
	strcat(part_1, "/part_cols/part_1.tbl");

	// p_type
	strcpy(part_5, dataset_path);
	strcat(part_5, argv[1]);
	strcat(part_5, "/part_cols/part_5.tbl");

	// Lineitem

	// l_partkey
	strcpy(lineitem_2, dataset_path);
	strcat(lineitem_2, argv[1]);
	strcat(lineitem_2, "/lineitem_cols/lineitem_2.tbl");

	// l_extendedprice
	strcpy(lineitem_6, dataset_path);
	strcat(lineitem_6, argv[1]);
	strcat(lineitem_6, "/lineitem_cols/lineitem_6.tbl");

	// l_discount
	strcpy(lineitem_7, dataset_path);
	strcat(lineitem_7, argv[1]);
	strcat(lineitem_7, "/lineitem_cols/lineitem_7.tbl");

	// l_shipdate
	strcpy(lineitem_11, dataset_path);
	strcat(lineitem_11, argv[1]);
	strcat(lineitem_11, "/lineitem_cols/lineitem_11.tbl");

	// Declare Variables

	// Part

	// p_partkey
	__declspec(align(MEM_LINE_SIZE)) double *p_partkey_values;
	__declspec(align(MEM_LINE_SIZE)) long *p_partkey_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *p_partkey_col_ptr;

	long p_partkey_n_rows;
	long p_partkey_n_cols;
	long p_partkey_nnz;

	// p_type
	__declspec(align(MEM_LINE_SIZE)) double *p_type_values;
	__declspec(align(MEM_LINE_SIZE)) long *p_type_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *p_type_col_ptr;

	long p_type_n_rows;
	long p_type_n_cols;
	long p_type_nnz;

	// Lineitem

	// l_partkey
	__declspec(align(MEM_LINE_SIZE)) double *l_partkey_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_partkey_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_partkey_col_ptr;

	long l_partkey_n_rows;
	long l_partkey_n_cols;
	long l_partkey_nnz;

	// l_extendedprice
	__declspec(align(MEM_LINE_SIZE)) double *l_extendedprice_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_extendedprice_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_extendedprice_col_ptr;

	long l_extendedprice_n_rows;
	long l_extendedprice_n_cols;
	long l_extendedprice_nnz;

	// l_discount
	__declspec(align(MEM_LINE_SIZE)) double *l_discount_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_discount_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_discount_col_ptr;

	long l_discount_n_rows;
	long l_discount_n_cols;
	long l_discount_nnz;

	// l_shipdate
	__declspec(align(MEM_LINE_SIZE)) double *l_shipdate_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_shipdate_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_shipdate_col_ptr;

	long l_shipdate_n_rows;
	long l_shipdate_n_cols;
	long l_shipdate_nnz;

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
	__declspec(align(MEM_LINE_SIZE)) double *F_values;
	__declspec(align(MEM_LINE_SIZE)) long *F_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *F_col_ptr;

	long F_n_rows;
	long F_n_cols;
	long F_nnz;

	// aux
	__declspec(align(MEM_LINE_SIZE)) double *aux_values;
	__declspec(align(MEM_LINE_SIZE)) long *aux_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *aux_col_ptr;

	long aux_n_rows;
	long aux_n_cols;
	long aux_nnz;

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
	Tree I = createSumTree();

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

	// query_14
	double query_14;

	// Labels

	// p_type
	GHashTable *p_type_id_label = NULL, *p_type_label_id = NULL;
	long p_type_next_id_label;

	// partkey
	GHashTable *partkey_id_label = NULL, *partkey_label_id = NULL;
	long partkey_next_id_label;

	// l_shipdate
	GHashTable *l_shipdate_id_label = NULL, *l_shipdate_label_id = NULL;
	long l_shipdate_next_id_label;

	// Load Dataset

	#ifdef D_VERBOSE
	printf("\nLoad Dataset\n\n");
	#endif

	// p_type
	read_column(
		part_5, num_elems[6][dataset],
		&p_type_values, &p_type_row_ind, &p_type_col_ptr,
		&p_type_nnz, &p_type_n_rows, &p_type_n_cols,
		&p_type_id_label, &p_type_label_id, &p_type_next_id_label
		);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "p_type", p_type_n_rows, p_type_n_cols, p_type_nnz);
	#endif

	// p_partkey
	read_column(
		part_1, num_elems[6][dataset],
		&p_partkey_values, &p_partkey_row_ind, &p_partkey_col_ptr,
		&p_partkey_nnz, &p_partkey_n_rows, &p_partkey_n_cols,
		&partkey_id_label, &partkey_label_id, &partkey_next_id_label
		);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "p_partkey", p_partkey_n_rows, p_partkey_n_cols, p_partkey_nnz);
	#endif

	// free p_partkey
	_mm_free(p_partkey_values);
	_mm_free(p_partkey_row_ind);
	_mm_free(p_partkey_col_ptr);

	// l_partkey
	read_column(
		lineitem_2, num_elems[2][dataset],
		&l_partkey_values, &l_partkey_row_ind, &l_partkey_col_ptr,
		&l_partkey_nnz, &l_partkey_n_rows, &l_partkey_n_cols,
		&partkey_id_label, &partkey_label_id, &partkey_next_id_label
		);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "l_partkey", l_partkey_n_rows, l_partkey_n_cols, l_partkey_nnz);
	#endif

	// l_extendedprice
	read_column_measure(
		lineitem_6, num_elems[2][dataset],
		&l_extendedprice_values, &l_extendedprice_row_ind, &l_extendedprice_col_ptr,
		&l_extendedprice_nnz, &l_extendedprice_n_rows, &l_extendedprice_n_cols
		);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "l_extendedprice", l_extendedprice_n_rows, l_extendedprice_n_cols, l_extendedprice_nnz);
	#endif

	// l_discount
	read_column_measure(
		lineitem_7, num_elems[2][dataset],
		&l_discount_values, &l_discount_row_ind, &l_discount_col_ptr,
		&l_discount_nnz, &l_discount_n_rows, &l_discount_n_cols
		);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "l_discount", l_discount_n_rows, l_discount_n_cols, l_discount_nnz);
	#endif

	// l_shipdate
	read_column(
		lineitem_11, num_elems[2][dataset],
		&l_shipdate_values, &l_shipdate_row_ind, &l_shipdate_col_ptr,
		&l_shipdate_nnz, &l_shipdate_n_rows, &l_shipdate_n_cols,
		&l_shipdate_id_label, &l_shipdate_label_id, &l_shipdate_next_id_label
		);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "l_shipdate", l_shipdate_n_rows, l_shipdate_n_cols, l_shipdate_nnz);
	#endif

	// Execute Query 14

	#ifdef D_VERBOSE
	printf("\nQuery 14\n\n");
	#endif

	GET_TIME(start);

	#ifdef D_PROFILE
	START();
	#endif

	// A = filter( match( p_type , ".+PROMO.+" ) )
	sp_bm_bv_filter_seq(
    	p_type_values, p_type_row_ind, p_type_col_ptr,
    	p_type_nnz, p_type_n_rows, p_type_n_cols,
    	p_type_id_label,
    	(int(*)(const void*,const void*)) strstrcmp,
    	EQUAL, "PROMO",
    	&A_values, &A_row_ind, &A_col_ptr,
	    &A_nnz, &A_n_rows, &A_n_cols
	    );

	#ifdef D_PROFILE
	STOP();
	#endif

	// free p_type
	_mm_free(p_type_col_ptr);
	_mm_free(p_type_row_ind);
	_mm_free(p_type_values);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "A", A_n_rows, A_n_cols, A_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// B = dot( A, l_partkey )
	sp_bvbm_dot_product_seq(
		A_values, A_row_ind, A_col_ptr,
    	A_nnz, A_n_rows, A_n_cols,
    	l_partkey_values, l_partkey_row_ind, l_partkey_col_ptr,
    	l_partkey_nnz, l_partkey_n_rows, l_partkey_n_cols,
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

	// free l_partkey
	_mm_free(l_partkey_col_ptr);
	_mm_free(l_partkey_row_ind);
	_mm_free(l_partkey_values);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "B", B_n_rows, B_n_cols, B_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// C = filter( l_shipdate >= "1995-09-01" )
	sp_bm_bv_filter_seq(
		l_shipdate_values, l_shipdate_row_ind, l_shipdate_col_ptr,
	    l_shipdate_nnz, l_shipdate_n_rows, l_shipdate_n_cols,
		l_shipdate_id_label,
		(int(*)(const void*,const void*)) strcmp,
		GREATER_EQ, "1995-09-01",
	    &C_values, &C_row_ind, &C_col_ptr,
	    &C_nnz, &C_n_rows, &C_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "C", C_n_rows, C_n_cols, C_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// D = filter( l_shipdate >= "1995-09-01" )
	sp_bm_bv_filter_seq(
		l_shipdate_values, l_shipdate_row_ind, l_shipdate_col_ptr,
	    l_shipdate_nnz, l_shipdate_n_rows, l_shipdate_n_cols,
		l_shipdate_id_label,
		(int(*)(const void*,const void*)) strcmp,
		LESS, "1995-10-01",
	    &D_values, &D_row_ind, &D_col_ptr,
	    &D_nnz, &D_n_rows, &D_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free l_shipdate
	_mm_free(l_shipdate_values);
	_mm_free(l_shipdate_row_ind);
	_mm_free(l_shipdate_col_ptr);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "D", D_n_rows, D_n_cols, D_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// E = hadamard( C, D )
	sp_bvbv_hadamard_seq(
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

	// free C
	_mm_free(C_values);
	_mm_free(C_row_ind);
	_mm_free(C_col_ptr);

	// free D
	_mm_free(D_values);
	_mm_free(D_row_ind);
	_mm_free(D_col_ptr);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "E", E_n_rows, E_n_cols, E_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// F = map( l_extendedprice * ( 1 - l_discount ) )
	sp_v_bang_seq(
		l_discount_values, l_discount_row_ind, l_discount_col_ptr,
		l_discount_nnz, l_discount_n_rows, l_discount_n_cols,
		&aux_values, &aux_row_ind, &aux_col_ptr,
		&aux_nnz, &aux_n_rows, &aux_n_cols
		);

	sp_vv_hadamard_seq(
		l_extendedprice_values, l_extendedprice_row_ind, l_extendedprice_col_ptr,
		l_extendedprice_nnz, l_extendedprice_n_rows, l_extendedprice_n_cols,
		aux_values, aux_row_ind, aux_col_ptr,
		aux_nnz, aux_n_rows, aux_n_cols,
		&F_values, &F_row_ind, &F_col_ptr,
		&F_nnz, &F_n_rows, &F_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free l_extendedprice
	_mm_free(l_extendedprice_values);
	_mm_free(l_extendedprice_row_ind);
	_mm_free(l_extendedprice_col_ptr);

	// free l_discount
	_mm_free(l_discount_values);
	_mm_free(l_discount_row_ind);
	_mm_free(l_discount_col_ptr);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "F", F_n_rows, F_n_cols, F_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// G = hadamard( E, F )
	sp_bvv_hadamard_seq(
	    E_values, E_row_ind, E_col_ptr,
	    E_nnz, E_n_rows, E_n_cols,
	    F_values, F_row_ind, F_col_ptr,
	    F_nnz, F_n_rows, F_n_cols,
	    &G_values, &G_row_ind, &G_col_ptr,
	    &G_nnz, &G_n_rows, &G_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free E
	_mm_free(E_values);
	_mm_free(E_row_ind);
	_mm_free(E_col_ptr);

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

	// H = hadamard( B, G )
	sp_fdv_hadamard(
		B_values, B_row_ind, B_col_ptr,
		B_nnz, B_n_rows, B_n_cols,
		G_values, G_row_ind, G_col_ptr,
		G_nnz, G_n_rows, G_n_cols,
		&H_values, &H_row_ind, &H_col_ptr,
		&H_nnz, &H_n_rows, &H_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free B
	_mm_free(B_values);
	_mm_free(B_row_ind);
	_mm_free(B_col_ptr);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "H", H_n_rows, H_n_cols, H_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// I = sum( G )
	I = fromCSCtoSumTree(I,
		G_col_ptr, G_row_ind, G_values,
	    G_n_cols, G_n_rows, G_nnz
		);

	fromSumTreeToCSC(I, G_n_rows,
		&I_col_ptr, &I_row_ind, &I_values,
		&I_n_cols, &I_n_rows, &I_nnz
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free I
	emptyTree(I);

	// free G
	_mm_free(G_values);
	_mm_free(G_row_ind);
	_mm_free(G_col_ptr);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "I", I_n_rows, I_n_cols, I_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// J = sum( H )
	J = fromCSCtoSumTree(J,
		H_col_ptr, H_row_ind, H_values,
	    H_n_cols, H_n_rows, H_nnz
		);

	fromSumTreeToCSC(J, H_n_rows,
		&J_col_ptr, &J_row_ind, &J_values,
		&J_n_cols, &J_n_rows, &J_nnz
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free J
	emptyTree(J);

	// free H
	_mm_free(H_values);
	_mm_free(H_row_ind);
	_mm_free(H_col_ptr);

	#ifdef D_VERBOSE
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "J", J_n_rows, J_n_cols, J_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// query_14 = map( 100.00 * J / I )
	query_14 = (100.0 * J_values[0]) / I_values[0];

	#ifdef D_PROFILE
	STOP();
	#endif

	// free J
	_mm_free(J_values);
	_mm_free(J_row_ind);
	_mm_free(J_col_ptr);

	// free I
	_mm_free(I_values);
	_mm_free(I_row_ind);
	_mm_free(I_col_ptr);

	printf("%f\n", query_14);

	GET_TIME(finish);
	elapsed = finish - start;
	printf("Execution Time:\t%f\nNNZ:\t%ld\n", elapsed, (long) 1);

	// Free Labels

	// p_type
	g_hash_table_destroy(p_type_id_label);
	g_hash_table_destroy(p_type_label_id);

	// partkey
	g_hash_table_destroy(partkey_id_label);
	g_hash_table_destroy(partkey_label_id);

	// l_shipdate
	g_hash_table_destroy(l_shipdate_id_label);
	g_hash_table_destroy(l_shipdate_label_id);

	return EXIT_SUCCESS;
}
