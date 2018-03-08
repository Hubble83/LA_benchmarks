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

	long num_elems[9] = { 6001215, 11997996, 23996604, 47989007, 95988640, 192000551, 384016850, 768046938, 91 }; // lineitem

	char *dataset_path = getenv("LA_DATA_DIR");

	char lineitem_5[100], lineitem_6[100], lineitem_7[100], lineitem_11[100];

	if (argc != 2) {
		fprintf(stderr, "Wrong number of arguments\n%s <dataset>\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	dataset = (int) log2 ((double) atoi(argv[1]));
	//dataset = 8;

	// l_quantity
	strcpy(lineitem_5, dataset_path);
	strcat(lineitem_5, argv[1]);
	strcat(lineitem_5, "/lineitem_cols/lineitem_5.tbl");

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

	// l_quantity bitmap
	__declspec(align(MEM_LINE_SIZE)) double *l_quantity_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_quantity_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_quantity_col_ptr;

	long l_quantity_n_rows;
	long l_quantity_n_cols;
	long l_quantity_nnz;

	// l_extendedprice measure
	__declspec(align(MEM_LINE_SIZE)) double *l_extendedprice_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_extendedprice_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_extendedprice_col_ptr;

	long l_extendedprice_n_rows;
	long l_extendedprice_n_cols;
	long l_extendedprice_nnz;

	// l_discount bitmap
	__declspec(align(MEM_LINE_SIZE)) double *b_l_discount_values;
	__declspec(align(MEM_LINE_SIZE)) long *b_l_discount_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *b_l_discount_col_ptr;

	long b_l_discount_n_rows;
	long b_l_discount_n_cols;
	long b_l_discount_nnz;

	// l_discount measure
	__declspec(align(MEM_LINE_SIZE)) double *m_l_discount_values;
	__declspec(align(MEM_LINE_SIZE)) long *m_l_discount_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *m_l_discount_col_ptr;

	long m_l_discount_n_rows;
	long m_l_discount_n_cols;
	long m_l_discount_nnz;

	// l_shipdate bitmap
	__declspec(align(MEM_LINE_SIZE)) double *l_shipdate_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_shipdate_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_shipdate_col_ptr;

	long l_shipdate_n_rows;
	long l_shipdate_n_cols;
	long l_shipdate_nnz;

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

	// G
	__declspec(align(MEM_LINE_SIZE)) double *G_values;
	__declspec(align(MEM_LINE_SIZE)) long *G_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *G_col_ptr;

	long G_n_rows;
	long G_n_cols;
	long G_nnz;

	// Result query 6
	Tree query_6;

	// labels l_quantity
	GHashTable *l_quantity_id_label = NULL, *l_quantity_label_id = NULL;
	long l_quantity_next_id_label;

	// labels l_discount
	GHashTable *l_discount_id_label = NULL, *l_discount_label_id = NULL;
	long l_discount_next_id_label;

	// labels l_shipdate
	GHashTable *l_shipdate_id_label = NULL, *l_shipdate_label_id = NULL;
	long l_shipdate_next_id_label;

	// load dataset

	#ifdef D_VERBOSE
	printf("\nLoad Dataset\n\n");
	#endif

	// load l_quantity bitmap
	read_column(
		lineitem_5, num_elems[dataset],
		&l_quantity_values, &l_quantity_row_ind, &l_quantity_col_ptr,
		&l_quantity_nnz, &l_quantity_n_rows, &l_quantity_n_cols,
		&l_quantity_id_label, &l_quantity_label_id, &l_quantity_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"l_quantity",
    	l_quantity_values, l_quantity_row_ind, l_quantity_col_ptr,
		l_quantity_nnz, l_quantity_n_rows, l_quantity_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "l_quantity", l_quantity_n_rows, l_quantity_n_cols, l_quantity_nnz);
	#endif

	// load l_extendedprice measure
	read_column_measure(
		lineitem_6, num_elems[dataset],
		&l_extendedprice_values, &l_extendedprice_row_ind, &l_extendedprice_col_ptr,
		&l_extendedprice_nnz, &l_extendedprice_n_rows, &l_extendedprice_n_cols
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"l_extendedprice",
    	l_extendedprice_values, l_extendedprice_row_ind, l_extendedprice_col_ptr,
		l_extendedprice_nnz, l_extendedprice_n_rows, l_extendedprice_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "l_extendedprice", l_extendedprice_n_rows, l_extendedprice_n_cols, l_extendedprice_nnz);
	#endif

	// load l_discount bitmap
	read_column(
		lineitem_7, num_elems[dataset],
		&b_l_discount_values, &b_l_discount_row_ind, &b_l_discount_col_ptr,
		&b_l_discount_nnz, &b_l_discount_n_rows, &b_l_discount_n_cols,
		&l_discount_id_label, &l_discount_label_id, &l_discount_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"b_l_discount_values",
    	b_l_discount_values_values, b_l_discount_row_ind, b_l_discount_col_ptr,
		b_l_discount_nnz, b_l_discount_n_rows, b_l_discount_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "b_l_discount", b_l_discount_n_rows, b_l_discount_n_cols, b_l_discount_nnz);
	#endif

	// load l_discount measure
	read_column_measure(
		lineitem_7, num_elems[dataset],
		&m_l_discount_values, &m_l_discount_row_ind, &m_l_discount_col_ptr,
		&m_l_discount_nnz, &m_l_discount_n_rows, &m_l_discount_n_cols
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"m_l_discount",
    	m_l_discount_values, m_l_discount_row_ind, m_l_discount_col_ptr,
		m_l_discount_nnz, m_l_discount_n_rows, m_l_discount_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "m_l_discount", m_l_discount_n_rows, m_l_discount_n_cols, m_l_discount_nnz);
	#endif

	// load l_shipdate bitmap
	read_column(
		lineitem_11, num_elems[dataset],
		&l_shipdate_values, &l_shipdate_row_ind, &l_shipdate_col_ptr,
		&l_shipdate_nnz, &l_shipdate_n_rows, &l_shipdate_n_cols,
		&l_shipdate_id_label, &l_shipdate_label_id, &l_shipdate_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"l_shipdate",
    	l_shipdate_values, l_shipdate_row_ind, l_shipdate_col_ptr,
		l_shipdate_nnz, l_shipdate_n_rows, l_shipdate_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "l_shipdate", l_shipdate_n_rows, l_shipdate_n_cols, l_shipdate_nnz);
	#endif

	// execute query 6

	#ifdef D_VERBOSE
	printf("\nQuery 6\n\n");
	#endif

	GET_TIME(start);

/*
 * A: #l -> S -> 1
 * A = sp_bm_bv_filter_and( l_shipdate, >='1995-03-10', <'1995-03-10')
 * A : bitmap vector
 *
 */

	#ifdef D_PROFILE
	START();
	#endif

	sp_bm_bv_filter_and_seq(
		l_shipdate_values, l_shipdate_row_ind, l_shipdate_col_ptr,
    	l_shipdate_nnz, l_shipdate_n_rows, l_shipdate_n_cols,
    	l_shipdate_id_label,
    	(int(*)(const void*,const void*)) strcmp,
    	GREATER_EQ, "1995-03-10",
	    LESS, "1996-03-10",
	    &A_values, &A_row_ind, &A_col_ptr,
	    &A_nnz, &A_n_rows, &A_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free l_shipdate
	_mm_free(l_shipdate_values);
	_mm_free(l_shipdate_row_ind);
	_mm_free(l_shipdate_col_ptr);

	#ifdef D_VERBOSE
	/*
	print_column(
		"A",
		A_values, A_row_ind, A_col_ptr,
		A_nnz, A_n_rows, A_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "A", A_n_rows, A_n_cols, A_nnz);
	#endif

/*
 * B: #l -> D -> 1
 * B = sp_bm_bv_filter_and( l_discount, >='0.49', <='0.51')
 * B : bitmap vector
 *
 */

	#ifdef D_PROFILE
	START();
	#endif

	sp_bm_bv_filter_and_seq(
		b_l_discount_values, b_l_discount_row_ind, b_l_discount_col_ptr,
    	b_l_discount_nnz, b_l_discount_n_rows, b_l_discount_n_cols,
    	l_discount_id_label,
    	(int(*)(const void*,const void*)) dblcmp,
    	GREATER_EQ, "0.04",
	    LESS_EQ, "0.06",
	    &B_values, &B_row_ind, &B_col_ptr,
	    &B_nnz, &B_n_rows, &B_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free b_l_discount
	_mm_free(b_l_discount_values);
	_mm_free(b_l_discount_row_ind);
	_mm_free(b_l_discount_col_ptr);

	#ifdef D_VERBOSE
	/*
	print_column(
		"B",
		B_values, B_row_ind, B_col_ptr,
		B_nnz, B_n_rows, B_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "B", B_n_rows, B_n_cols, B_nnz);
	#endif

/*
 * C: #l -> Q -> 1
 * C = sp_bm_bv_filter( l_quantity, <='3')
 * C : bitmap vector
 *
 */

	#ifdef D_PROFILE
	START();
	#endif

	sp_bm_bv_filter_seq(
		l_quantity_values, l_quantity_row_ind, l_quantity_col_ptr,
    	l_quantity_nnz, l_quantity_n_rows, l_quantity_n_cols,
    	l_quantity_id_label,
    	(int(*)(const void*,const void*)) intcmp,
    	LESS, "3",
	    &C_values, &C_row_ind, &C_col_ptr,
	    &C_nnz, &C_n_rows, &C_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free l_quantity
	_mm_free(l_quantity_values);
	_mm_free(l_quantity_row_ind);
	_mm_free(l_quantity_col_ptr);

	#ifdef D_VERBOSE
	/*
	print_column(
		"C",
		C_values, C_row_ind, C_col_ptr,
		C_nnz, C_n_rows, C_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "C", C_n_rows, C_n_cols, C_nnz);
	#endif

/*
 * D: #l -> 1
 * D = sp_bvbv_hadamard( A , B )
 * D : bitmap vector
 *
 */

	#ifdef D_PROFILE
	START();
	#endif

	sp_bvbv_hadamard_seq(
		A_values, A_row_ind, A_col_ptr,
	    A_nnz, A_n_rows, A_n_cols,
		B_values, B_row_ind, B_col_ptr,
	    B_nnz, B_n_rows, B_n_cols,
	    &D_values, &D_row_ind, &D_col_ptr,
	    &D_nnz, &D_n_rows, &D_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free A
	_mm_free(A_values);
	_mm_free(A_row_ind);
	_mm_free(A_col_ptr);

	// free B
	_mm_free(B_values);
	_mm_free(B_row_ind);
	_mm_free(B_col_ptr);

	#ifdef D_VERBOSE
	/*
	print_column(
		"D",
		D_values, D_row_ind, D_col_ptr,
		D_nnz, D_n_rows, D_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "D", D_n_rows, D_n_cols, D_nnz);
	#endif

/*
 * E: #l -> 1
 * E = sp_bvbv_hadamard( C , D )
 * E : bitmap vector
 *
 */

	#ifdef D_PROFILE
	START();
	#endif

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
	/*
	print_column(
		"E",
		E_values, E_row_ind, E_col_ptr,
		E_nnz, E_n_rows, E_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "E", E_n_rows, E_n_cols, E_nnz);
	#endif

/*
 * F: #l -> 1
 * F = sp_vv_hadamard( l_extendedprice , l_discount )
 * F : bitmap vector
 *
 */

	#ifdef D_PROFILE
	START();
	#endif

	sp_vv_hadamard_seq(
		l_extendedprice_values, l_extendedprice_row_ind, l_extendedprice_col_ptr,
	    l_extendedprice_nnz, l_extendedprice_n_rows, l_extendedprice_n_cols,
		m_l_discount_values, m_l_discount_row_ind, m_l_discount_col_ptr,
	    m_l_discount_nnz, m_l_discount_n_rows, m_l_discount_n_cols,
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

	// free m_l_discount
	_mm_free(m_l_discount_values);
	_mm_free(m_l_discount_row_ind);
	_mm_free(m_l_discount_col_ptr);

	#ifdef D_VERBOSE
	/*
	print_column(
		"F",
		F_values, F_row_ind, F_col_ptr,
		F_nnz, F_n_rows, F_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "F", F_n_rows, F_n_cols, F_nnz);
	#endif

/*
 * G: #l -> 1
 * G = sp_bvv_hadamard( E , F )
 * G : vector not totally full of values
 *    E -> bitmap vector
 *    F -> vector full of values
 *
 */

	#ifdef D_PROFILE
	START();
	#endif

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
	/*
	print_column(
		"G",
		G_values, G_row_ind, G_col_ptr,
		G_nnz, G_n_rows, G_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "G", G_n_rows, G_n_cols, G_nnz);
	#endif

/*
 * Result of query 6
 */

	#ifdef D_PROFILE
	START();
	#endif

	query_6 = createSumTree();
	query_6 = fromCSCtoSumTree(query_6, G_col_ptr, G_row_ind, G_values, G_n_cols, G_n_rows, G_nnz);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free G
	_mm_free(G_values);
	_mm_free(G_row_ind);
	_mm_free(G_col_ptr);

	GET_TIME(finish);
	elapsed = finish - start;
	printf("Execution Time:\t%f\nNNZ:\t%ld\n", elapsed, G_nnz);

	#ifdef D_PROFILE
	STOP_PROFILE();
	#endif

	// free labels l_quantity
	g_hash_table_destroy(l_quantity_id_label);
	g_hash_table_destroy(l_quantity_label_id);

	// labels l_discount
	g_hash_table_destroy(l_discount_id_label);
	g_hash_table_destroy(l_discount_label_id);

	// labels l_shipdate
	g_hash_table_destroy(l_shipdate_id_label);
	g_hash_table_destroy(l_shipdate_label_id);

	return EXIT_SUCCESS;
}
