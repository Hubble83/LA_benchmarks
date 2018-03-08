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

	char orders_1[100], orders_6[100];
	char lineitem_1[100], lineitem_11[100], lineitem_12[100], lineitem_13[100], lineitem_15[100];

	if (argc != 2) {
		fprintf(stderr, "Wrong number of arguments\n%s <dataset>\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	dataset = (int) log2 ((double) atoi(argv[1]));

	// Orders

	// o_orderkey
	strcpy(orders_1, dataset_path);
	strcat(orders_1, argv[1]);
	strcat(orders_1, "/orders_cols/orders_1.tbl");

	// o_orderpriority
	strcpy(orders_6, dataset_path);
	strcat(orders_6, argv[1]);
	strcat(orders_6, "/orders_cols/orders_6.tbl");

	// Lineitem

	// l_orderkey
	strcpy(lineitem_1, dataset_path);
	strcat(lineitem_1, argv[1]);
	strcat(lineitem_1, "/lineitem_cols/lineitem_1.tbl");

	// l_shipdate
	strcpy(lineitem_11, dataset_path);
	strcat(lineitem_11, argv[1]);
	strcat(lineitem_11, "/lineitem_cols/lineitem_11.tbl");

	// l_commitdate
	strcpy(lineitem_12, dataset_path);
	strcat(lineitem_12, argv[1]);
	strcat(lineitem_12, "/lineitem_cols/lineitem_12.tbl");

	// l_receiptdate
	strcpy(lineitem_13, dataset_path);
	strcat(lineitem_13, argv[1]);
	strcat(lineitem_13, "/lineitem_cols/lineitem_13.tbl");

	// l_shipmode
	strcpy(lineitem_15, dataset_path);
	strcat(lineitem_15, argv[1]);
	strcat(lineitem_15, "/lineitem_cols/lineitem_15.tbl");

	// Declare Variables

	// Orders

	// o_orderkey
	__declspec(align(MEM_LINE_SIZE)) double *o_orderkey_values;
	__declspec(align(MEM_LINE_SIZE)) long *o_orderkey_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *o_orderkey_col_ptr;

	long o_orderkey_n_rows;
	long o_orderkey_n_cols;
	long o_orderkey_nnz;

	// o_orderpriority
	__declspec(align(MEM_LINE_SIZE)) double *o_orderpriority_values;
	__declspec(align(MEM_LINE_SIZE)) long *o_orderpriority_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *o_orderpriority_col_ptr;

	long o_orderpriority_n_rows;
	long o_orderpriority_n_cols;
	long o_orderpriority_nnz;

	// Lineitem

	// l_orderkey
	__declspec(align(MEM_LINE_SIZE)) double *l_orderkey_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_orderkey_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_orderkey_col_ptr;

	long l_orderkey_n_rows;
	long l_orderkey_n_cols;
	long l_orderkey_nnz;

	// l_shipdate
	__declspec(align(MEM_LINE_SIZE)) double *l_shipdate_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_shipdate_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_shipdate_col_ptr;

	long l_shipdate_n_rows;
	long l_shipdate_n_cols;
	long l_shipdate_nnz;

	// l_commitdate
	__declspec(align(MEM_LINE_SIZE)) double *l_commitdate_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_commitdate_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_commitdate_col_ptr;

	long l_commitdate_n_rows;
	long l_commitdate_n_cols;
	long l_commitdate_nnz;

	// l_receiptdate
	__declspec(align(MEM_LINE_SIZE)) double *l_receiptdate_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_receiptdate_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_receiptdate_col_ptr;

	long l_receiptdate_n_rows;
	long l_receiptdate_n_cols;
	long l_receiptdate_nnz;

	// l_shipmode
	__declspec(align(MEM_LINE_SIZE)) double *l_shipmode_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_shipmode_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_shipmode_col_ptr;

	long l_shipmode_n_rows;
	long l_shipmode_n_cols;
	long l_shipmode_nnz;

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
	__declspec(align(MEM_LINE_SIZE)) double *J_values;
	__declspec(align(MEM_LINE_SIZE)) long *J_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *J_col_ptr;

	long J_n_rows;
	long J_n_cols;
	long J_nnz;

	// K
	__declspec(align(MEM_LINE_SIZE)) double *K_values;
	__declspec(align(MEM_LINE_SIZE)) long *K_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *K_col_ptr;

	long K_n_rows;
	long K_n_cols;
	long K_nnz;

	// L
	__declspec(align(MEM_LINE_SIZE)) double *L_values;
	__declspec(align(MEM_LINE_SIZE)) long *L_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *L_col_ptr;

	long L_n_rows;
	long L_n_cols;
	long L_nnz;

	// M
	__declspec(align(MEM_LINE_SIZE)) double *M_values;
	__declspec(align(MEM_LINE_SIZE)) long *M_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *M_col_ptr;

	long M_n_rows;
	long M_n_cols;
	long M_nnz;

	// N
	__declspec(align(MEM_LINE_SIZE)) double *N_values;
	__declspec(align(MEM_LINE_SIZE)) long *N_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *N_col_ptr;

	long N_n_rows;
	long N_n_cols;
	long N_nnz;

	// O
	__declspec(align(MEM_LINE_SIZE)) double *O_values;
	__declspec(align(MEM_LINE_SIZE)) long *O_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *O_col_ptr;

	long O_n_rows;
	long O_n_cols;
	long O_nnz;

	// P
	__declspec(align(MEM_LINE_SIZE)) double *P_values;
	__declspec(align(MEM_LINE_SIZE)) long *P_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *P_col_ptr;

	long P_n_rows;
	long P_n_cols;
	long P_nnz;

	// Q
	__declspec(align(MEM_LINE_SIZE)) double *Q_values;
	__declspec(align(MEM_LINE_SIZE)) long *Q_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *Q_col_ptr;

	long Q_n_rows;
	long Q_n_cols;
	long Q_nnz;

	// R
	Tree R = createSumTree();

	// S
	Tree S = createSumTree();

	// T
	__declspec(align(MEM_LINE_SIZE)) double *T_values;
	__declspec(align(MEM_LINE_SIZE)) long *T_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *T_col_ptr;

	long T_n_rows;
	long T_n_cols;
	long T_nnz;

	// U
	__declspec(align(MEM_LINE_SIZE)) double *U_values;
	__declspec(align(MEM_LINE_SIZE)) long *U_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *U_col_ptr;

	long U_n_rows;
	long U_n_cols;
	long U_nnz;

	// Labels

	// orderkey
	GHashTable *orderkey_id_label = NULL, *orderkey_label_id = NULL;
	long orderkey_next_id_label;

	// o_orderpriority
	GHashTable *o_orderpriority_id_label = NULL, *o_orderpriority_label_id = NULL;
	long o_orderpriority_next_id_label;

	// l_shipdate
	GHashTable *l_shipdate_id_label = NULL, *l_shipdate_label_id = NULL;
	long l_shipdate_next_id_label;

	// l_commitdate
	GHashTable *l_commitdate_id_label = NULL, *l_commitdate_label_id = NULL;
	long l_commitdate_next_id_label;

	// l_receiptdate
	GHashTable *l_receiptdate_id_label = NULL, *l_receiptdate_label_id = NULL;
	long l_receiptdate_next_id_label;

	// l_shipmode
	GHashTable *l_shipmode_id_label = NULL, *l_shipmode_label_id = NULL;
	long l_shipmode_next_id_label;

	// Load Dataset

	#ifdef D_VERBOSE
	printf("\nLoad Dataset\n\n");
	#endif

	// o_orderkey
	read_column(
		orders_1, num_elems[1][dataset],
		&o_orderkey_values, &o_orderkey_row_ind, &o_orderkey_col_ptr,
		&o_orderkey_nnz, &o_orderkey_n_rows, &o_orderkey_n_cols,
		&orderkey_id_label, &orderkey_label_id, &orderkey_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"o_orderkey",
    	o_orderkey_values, o_orderkey_row_ind, o_orderkey_col_ptr,
		o_orderkey_nnz, o_orderkey_n_rows, o_orderkey_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "o_orderkey", o_orderkey_n_rows, o_orderkey_n_cols, o_orderkey_nnz);
	#endif

	_mm_free(o_orderkey_col_ptr);
	_mm_free(o_orderkey_row_ind);
	_mm_free(o_orderkey_values);

	// o_orderpriority
	read_column(
		orders_6, num_elems[1][dataset],
		&o_orderpriority_values, &o_orderpriority_row_ind, &o_orderpriority_col_ptr,
		&o_orderpriority_nnz, &o_orderpriority_n_rows, &o_orderpriority_n_cols,
		&o_orderpriority_id_label, &o_orderpriority_label_id, &o_orderpriority_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"o_orderpriority",
    	o_orderpriority_values, o_orderpriority_row_ind, o_orderpriority_col_ptr,
		o_orderpriority_nnz, o_orderpriority_n_rows, o_orderpriority_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "o_orderpriority", o_orderpriority_n_rows, o_orderpriority_n_cols, o_orderpriority_nnz);
	#endif

	// l_orderkey
	read_column(
		lineitem_1, num_elems[2][dataset],
		&l_orderkey_values, &l_orderkey_row_ind, &l_orderkey_col_ptr,
		&l_orderkey_nnz, &l_orderkey_n_rows, &l_orderkey_n_cols,
		&orderkey_id_label, &orderkey_label_id, &orderkey_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"l_orderkey",
    	l_orderkey_values, l_orderkey_row_ind, l_orderkey_col_ptr,
		l_orderkey_nnz, l_orderkey_n_rows, l_orderkey_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "l_orderkey", l_orderkey_n_rows, l_orderkey_n_cols, l_orderkey_nnz);
	#endif

	// l_shipdate
	read_column(
		lineitem_11, num_elems[2][dataset],
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

	// l_commitdate
	read_column(
		lineitem_12, num_elems[2][dataset],
		&l_commitdate_values, &l_commitdate_row_ind, &l_commitdate_col_ptr,
		&l_commitdate_nnz, &l_commitdate_n_rows, &l_commitdate_n_cols,
		&l_commitdate_id_label, &l_commitdate_label_id, &l_commitdate_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"l_commitdate",
    	l_commitdate_values, l_commitdate_row_ind, l_commitdate_col_ptr,
		l_commitdate_nnz, l_commitdate_n_rows, l_commitdate_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "l_commitdate", l_commitdate_n_rows, l_commitdate_n_cols, l_commitdate_nnz);
	#endif

	// l_receiptdate
	read_column(
		lineitem_13, num_elems[2][dataset],
		&l_receiptdate_values, &l_receiptdate_row_ind, &l_receiptdate_col_ptr,
		&l_receiptdate_nnz, &l_receiptdate_n_rows, &l_receiptdate_n_cols,
		&l_receiptdate_id_label, &l_receiptdate_label_id, &l_receiptdate_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"l_receiptdate",
    	l_receiptdate_values, l_receiptdate_row_ind, l_receiptdate_col_ptr,
		l_receiptdate_nnz, l_receiptdate_n_rows, l_receiptdate_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "l_receiptdate", l_receiptdate_n_rows, l_receiptdate_n_cols, l_receiptdate_nnz);
	#endif

	// l_shipmode
	read_column(
		lineitem_15, num_elems[2][dataset],
		&l_shipmode_values, &l_shipmode_row_ind, &l_shipmode_col_ptr,
		&l_shipmode_nnz, &l_shipmode_n_rows, &l_shipmode_n_cols,
		&l_shipmode_id_label, &l_shipmode_label_id, &l_shipmode_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"l_shipmode",
    	l_shipmode_values, l_shipmode_row_ind, l_shipmode_col_ptr,
		l_shipmode_nnz, l_shipmode_n_rows, l_shipmode_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "l_shipmode", l_shipmode_n_rows, l_shipmode_n_cols, l_shipmode_nnz);
	#endif

	// Execute Query 12

	#ifdef D_VERBOSE
	printf("\nQuery 12\n\n");
	#endif

	GET_TIME(start);

	#ifdef D_PROFILE
	START();
	#endif

	char **cond = (char**) malloc(2 * sizeof(char*));
	cond[0] = strdup("MAIL");
	cond[1] = strdup("SHIP");

	// A = filter( l_shipmode in {...} )
	sp_bm_bv_filter_in_seq(
    	l_shipmode_values, l_shipmode_row_ind, l_shipmode_col_ptr,
    	l_shipmode_nnz, l_shipmode_n_rows, l_shipmode_n_cols,
    	l_shipmode_id_label,
    	(int(*)(const void*,const void*)) strcmp,
    	cond, 2,
    	&A_values, &A_row_ind, &A_col_ptr,
	    &A_nnz, &A_n_rows, &A_n_cols
	    );

	#ifdef D_PROFILE
	STOP();
	#endif

	// free cond
	free(cond[0]);
	free(cond[1]);
	free(cond);

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

	#ifdef D_PROFILE
	START();
	#endif

	// B = filter( l_commitdate < l_receiptdate )
	sp_bmbm_bv_filter_seq(
		l_commitdate_values, l_commitdate_row_ind, l_commitdate_col_ptr,
    	l_commitdate_nnz, l_commitdate_n_rows, l_commitdate_n_cols,
    	l_commitdate_id_label,
    	l_receiptdate_values, l_receiptdate_row_ind, l_receiptdate_col_ptr,
    	l_receiptdate_nnz, l_receiptdate_n_rows, l_receiptdate_n_cols,
    	l_receiptdate_id_label,
    	(int(*)(const void*,const void*)) strcmp,
	    LESS,
	    &B_values, &B_row_ind, &B_col_ptr,
	    &B_nnz, &B_n_rows, &B_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

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

	#ifdef D_PROFILE
	START();
	#endif

	// C = filter( l_shipdate < l_commitdate )
	sp_bmbm_bv_filter_seq(
    	l_shipdate_values, l_shipdate_row_ind, l_shipdate_col_ptr,
    	l_shipdate_nnz, l_shipdate_n_rows, l_shipdate_n_cols,
    	l_shipdate_id_label,
    	l_commitdate_values, l_commitdate_row_ind, l_commitdate_col_ptr,
    	l_commitdate_nnz, l_commitdate_n_rows, l_commitdate_n_cols,
    	l_commitdate_id_label,
    	(int(*)(const void*,const void*)) strcmp,
	    LESS,
	    &C_values, &C_row_ind, &C_col_ptr,
	    &C_nnz, &C_n_rows, &C_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free l_shipdate
	_mm_free(l_shipdate_values);
	_mm_free(l_shipdate_row_ind);
	_mm_free(l_shipdate_col_ptr);

	// free l_commitdate
	_mm_free(l_commitdate_values);
	_mm_free(l_commitdate_row_ind);
	_mm_free(l_commitdate_col_ptr);

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

	#ifdef D_PROFILE
	START();
	#endif

	// D = filter( l_receiptdate >= '1994-01-01' )
	sp_bm_bv_filter_seq(
		l_receiptdate_values, l_receiptdate_row_ind, l_receiptdate_col_ptr,
	    l_receiptdate_nnz, l_receiptdate_n_rows, l_receiptdate_n_cols,
		l_receiptdate_id_label,
		(int(*)(const void*,const void*)) strcmp,
		GREATER_EQ, "1994-01-01",
	    &D_values, &D_row_ind, &D_col_ptr,
	    &D_nnz, &D_n_rows, &D_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

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

	#ifdef D_PROFILE
	START();
	#endif

	// E = filter( l_receiptdate < '1995-01-01' )
	sp_bm_bv_filter_seq(
		l_receiptdate_values, l_receiptdate_row_ind, l_receiptdate_col_ptr,
	    l_receiptdate_nnz, l_receiptdate_n_rows, l_receiptdate_n_cols,
		l_receiptdate_id_label,
		(int(*)(const void*,const void*)) strcmp,
		LESS, "1995-01-01",
	    &E_values, &E_row_ind, &E_col_ptr,
	    &E_nnz, &E_n_rows, &E_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free l_receiptdate
	_mm_free(l_receiptdate_values);
	_mm_free(l_receiptdate_row_ind);
	_mm_free(l_receiptdate_col_ptr);

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

	#ifdef D_PROFILE
	START();
	#endif

	// F = hadamard( D, E )
	sp_bvbv_hadamard_seq(
		D_values, D_row_ind, D_col_ptr,
	    D_nnz, D_n_rows, D_n_cols,
	    E_values, E_row_ind, E_col_ptr,
	    E_nnz, E_n_rows, E_n_cols,
	    &F_values, &F_row_ind, &F_col_ptr,
	    &F_nnz, &F_n_rows, &F_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free D
	_mm_free(D_values);
	_mm_free(D_row_ind);
	_mm_free(D_col_ptr);

	// free E
	_mm_free(E_values);
	_mm_free(E_row_ind);
	_mm_free(E_col_ptr);

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

	#ifdef D_PROFILE
	START();
	#endif

	// G = hadamard( C, F )
	sp_bvbv_hadamard_seq(
		C_values, C_row_ind, C_col_ptr,
	    C_nnz, C_n_rows, C_n_cols,
	    F_values, F_row_ind, F_col_ptr,
	    F_nnz, F_n_rows, F_n_cols,
	    &G_values, &G_row_ind, &G_col_ptr,
	    &G_nnz, &G_n_rows, &G_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free C
	_mm_free(C_values);
	_mm_free(C_row_ind);
	_mm_free(C_col_ptr);

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

	#ifdef D_PROFILE
	START();
	#endif

	// H = hadamard( B, G )
	sp_bvbv_hadamard_seq(
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

	// free G
	_mm_free(G_values);
	_mm_free(G_row_ind);
	_mm_free(G_col_ptr);

	#ifdef D_VERBOSE
	/*
	print_column(
		"H",
		H_values, H_row_ind, H_col_ptr,
		H_nnz, H_n_rows, H_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "H", H_n_rows, H_n_cols, H_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// I = hadamard( A, H )
	sp_bvbv_hadamard_seq(
		A_values, A_row_ind, A_col_ptr,
	    A_nnz, A_n_rows, A_n_cols,
		H_values, H_row_ind, H_col_ptr,
	    H_nnz, H_n_rows, H_n_cols,
	    &I_values, &I_row_ind, &I_col_ptr,
	    &I_nnz, &I_n_rows, &I_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free A
	_mm_free(A_values);
	_mm_free(A_row_ind);
	_mm_free(A_col_ptr);

	// free H
	_mm_free(H_values);
	_mm_free(H_row_ind);
	_mm_free(H_col_ptr);

	#ifdef D_VERBOSE
	/*
	print_column(
		"I",
		I_values, I_row_ind, I_col_ptr,
		I_nnz, I_n_rows, I_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "I", I_n_rows, I_n_cols, I_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// J = krao( l_shipmode, I )
	sp_bmbv_krao_seq(
		l_shipmode_values, l_shipmode_row_ind, l_shipmode_col_ptr,
    	l_shipmode_nnz, l_shipmode_n_rows, l_shipmode_n_cols,
    	I_values, I_row_ind, I_col_ptr,
	    I_nnz, I_n_rows, I_n_cols,
	    &J_values, &J_row_ind, &J_col_ptr,
	    &J_nnz, &J_n_rows, &J_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free l_shipmode
	_mm_free(l_shipmode_values);
	_mm_free(l_shipmode_row_ind);
	_mm_free(l_shipmode_col_ptr);

	// free I
	_mm_free(I_values);
	_mm_free(I_row_ind);
	_mm_free(I_col_ptr);

	#ifdef D_VERBOSE
	/*
	print_column(
		"J",
		J_values, J_row_ind, J_col_ptr,
		J_nnz, J_n_rows, J_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "J", J_n_rows, J_n_cols, J_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// K = dot( o_orderpriority, l_orderkey )
	sp_bmbm_dot_product_seq(
		o_orderpriority_values, o_orderpriority_row_ind, o_orderpriority_col_ptr,
		o_orderpriority_nnz, o_orderpriority_n_rows, o_orderpriority_n_cols,
		l_orderkey_values, l_orderkey_row_ind, l_orderkey_col_ptr,
		l_orderkey_nnz, l_orderkey_n_rows, l_orderkey_n_cols,
		&K_values, &K_row_ind, &K_col_ptr,
	    &K_nnz, &K_n_rows, &K_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free o_orderpriority
	_mm_free(o_orderpriority_values);
	_mm_free(o_orderpriority_row_ind);
	_mm_free(o_orderpriority_col_ptr);

	// free l_orderkey
	_mm_free(l_orderkey_values);
	_mm_free(l_orderkey_row_ind);
	_mm_free(l_orderkey_col_ptr);

	#ifdef D_VERBOSE
	/*
	print_column(
		"K",
		K_values, K_row_ind, K_col_ptr,
		K_nnz, K_n_rows, K_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "K", K_n_rows, K_n_cols, K_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// L = filter( K = '1-URGENT' )
	sp_bm_bv_filter_seq(
		K_values, K_row_ind, K_col_ptr,
	    K_nnz, K_n_rows, K_n_cols,
		o_orderpriority_id_label,
		(int(*)(const void*,const void*)) strcmp,
		EQUAL, "1-URGENT",
	    &L_values, &L_row_ind, &L_col_ptr,
	    &L_nnz, &L_n_rows, &L_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	#ifdef D_VERBOSE
	/*
	print_column(
		"L",
		L_values, L_row_ind, L_col_ptr,
		L_nnz, L_n_rows, L_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "L", L_n_rows, L_n_cols, L_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// M = filter( K = '2-HIGH' )
	sp_bm_bv_filter_seq(
		K_values, K_row_ind, K_col_ptr,
	    K_nnz, K_n_rows, K_n_cols,
		o_orderpriority_id_label,
		(int(*)(const void*,const void*)) strcmp,
		EQUAL, "2-HIGH",
	    &M_values, &M_row_ind, &M_col_ptr,
	    &M_nnz, &M_n_rows, &M_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free K
	_mm_free(K_values);
	_mm_free(K_row_ind);
	_mm_free(K_col_ptr);

	#ifdef D_VERBOSE
	/*
	print_column(
		"M",
		M_values, M_row_ind, M_col_ptr,
		M_nnz, M_n_rows, M_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "M", M_n_rows, M_n_cols, M_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// N = hadamard( L OR M )
	sp_bvbv_hadamard_or_seq(
		L_values, L_row_ind, L_col_ptr,
		L_nnz, L_n_rows, L_n_cols,
		M_values, M_row_ind, M_col_ptr,
		M_nnz, M_n_rows, M_n_cols,
		&N_values, &N_row_ind, &N_col_ptr,
	    &N_nnz, &N_n_rows, &N_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free L
	_mm_free(L_values);
	_mm_free(L_row_ind);
	_mm_free(L_col_ptr);

	// free M
	_mm_free(M_values);
	_mm_free(M_row_ind);
	_mm_free(M_col_ptr);

	#ifdef D_VERBOSE
	/*
	print_column(
		"N",
		N_values, N_row_ind, N_col_ptr,
		N_nnz, N_n_rows, N_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "N", N_n_rows, N_n_cols, N_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// O = map( !N )
	sp_bv_map_not(
		N_values, N_row_ind, N_col_ptr,
		N_nnz, N_n_rows, N_n_cols,
		&O_values, &O_row_ind, &O_col_ptr,
	    &O_nnz, &O_n_rows, &O_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	#ifdef D_VERBOSE
	/*
	print_column(
		"O",
		O_values, O_row_ind, O_col_ptr,
		O_nnz, O_n_rows, O_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "O", O_n_rows, O_n_cols, O_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// P = krao( J, N )
	sp_bmbv_krao_seq(
		J_values, J_row_ind, J_col_ptr,
		J_nnz, J_n_rows, J_n_cols,
		N_values, N_row_ind, N_col_ptr,
		N_nnz, N_n_rows, N_n_cols,
		&P_values, &P_row_ind, &P_col_ptr,
	    &P_nnz, &P_n_rows, &P_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free N
	_mm_free(N_values);
	_mm_free(N_row_ind);
	_mm_free(N_col_ptr);

	#ifdef D_VERBOSE
	/*
	print_column(
		"P",
		P_values, P_row_ind, P_col_ptr,
		P_nnz, P_n_rows, P_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "P", P_n_rows, P_n_cols, P_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// Q = krao( J, O )
	sp_bmbv_krao_seq(
		J_values, J_row_ind, J_col_ptr,
		J_nnz, J_n_rows, J_n_cols,
		O_values, O_row_ind, O_col_ptr,
		O_nnz, O_n_rows, O_n_cols,
		&Q_values, &Q_row_ind, &Q_col_ptr,
	    &Q_nnz, &Q_n_rows, &Q_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free J
	_mm_free(J_values);
	_mm_free(J_row_ind);
	_mm_free(J_col_ptr);

	// free O
	_mm_free(O_values);
	_mm_free(O_row_ind);
	_mm_free(O_col_ptr);

	#ifdef D_VERBOSE
	/*
	print_column(
		"Q",
		Q_values, Q_row_ind, Q_col_ptr,
		Q_nnz, Q_n_rows, Q_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "Q", Q_n_rows, Q_n_cols, Q_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// R = avl_sum( P )
	R = fromCSCtoSumTree(R,
		P_col_ptr, P_row_ind, P_values,
	    P_n_cols, P_n_rows, P_nnz
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free P
	_mm_free(P_values);
	_mm_free(P_row_ind);
	_mm_free(P_col_ptr);

	#ifdef D_VERBOSE
	/*
	print_column(
		"R",
		R_values, R_row_ind, R_col_ptr,
		R_nnz, R_n_rows, R_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "R", P_n_rows, (long) 1, (long) treeSize(R));
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// S = avl_sum( Q )
	S = fromCSCtoSumTree(S,
		Q_col_ptr, Q_row_ind, Q_values,
	    Q_n_cols, Q_n_rows, Q_nnz
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free Q
	_mm_free(Q_values);
	_mm_free(Q_row_ind);
	_mm_free(Q_col_ptr);

	#ifdef D_VERBOSE
	/*
	print_column(
		"S",
		S_values, S_row_ind, S_col_ptr,
		S_nnz, S_n_rows, S_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "S", Q_n_rows, (long) 1, (long) treeSize(S));
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// T = avl_to_vector( R )
	fromSumTreeToCSC(R, P_n_rows,
		&T_col_ptr, &T_row_ind, &T_values,
		&T_n_cols, &T_n_rows, &T_nnz
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free R
	emptyTree(R);

	#ifdef D_VERBOSE
	/*
	print_column(
		"T",
		T_values, T_row_ind, T_col_ptr,
		T_nnz, T_n_rows, T_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "T", T_n_rows, T_n_cols, T_nnz);
	#endif

	#ifdef D_PROFILE
	START();
	#endif

	// U = avl_to_vector( S )
	fromSumTreeToCSC(S, Q_n_rows,
		&U_col_ptr, &U_row_ind, &U_values,
		&U_n_cols, &U_n_rows, &U_nnz
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free S
	emptyTree(S);

	#ifdef D_VERBOSE
	/*
	print_column(
		"U",
		U_values, U_row_ind, U_col_ptr,
		U_nnz, U_n_rows, U_n_cols
		);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "U", U_n_rows, U_n_cols, U_nnz);
	#endif

	#ifdef D_PROFILE
	STOP_PROFILE();
	#endif

	for (int i = 0; i < U_nnz; ++i) {
		printf("%f\t%f\n", T_values[i], U_values[i]);
	}

	GET_TIME(finish);
	elapsed = finish - start;
	printf("Execution Time:\t%f\nNNZ:\t%ld\t%ld\n", elapsed, T_nnz, U_nnz);

	// Free Labels

	// orderkey
	g_hash_table_destroy(orderkey_id_label);
	g_hash_table_destroy(orderkey_label_id);

	// o_orderpriority
	g_hash_table_destroy(o_orderpriority_id_label);
	g_hash_table_destroy(o_orderpriority_label_id);

	// l_shipdate
	g_hash_table_destroy(l_shipdate_id_label);
	g_hash_table_destroy(l_shipdate_label_id);

	// l_commitdate
	g_hash_table_destroy(l_commitdate_id_label);
	g_hash_table_destroy(l_commitdate_label_id);

	// l_receiptdate
	g_hash_table_destroy(l_receiptdate_id_label);
	g_hash_table_destroy(l_receiptdate_label_id);

	// l_shipmode
	g_hash_table_destroy(l_shipmode_id_label);
	g_hash_table_destroy(l_shipmode_label_id);

	return EXIT_SUCCESS;
}
