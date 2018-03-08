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
	double measurements[13];
	int curr_op;
};

typedef struct _profile profile;

profile prof;

#define START_PROFILE() { \
	int i; \
	for (i = 0; i < 13; ++i) \
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

	long num_elems[3][9] = {
						   {  150000,   300000,   600000,  1200000,  2400000,   4800000,   9600000,  19200000, 25 }, // customer
						   { 1500000,  3000000,  6000000, 12000000, 24000000,  48000000,  96000000, 192000000, 51 }, // orders
						   { 6001215, 11997996, 23996604, 47989007, 95988640, 192000551, 384016850, 768046938, 91 }  // lineitem
						  };

	char *dataset_path = getenv("LA_DATA_DIR");

	char customer_1[100], customer_7[100];
	char orders_1[100], orders_2[100], orders_5[100], orders_8[100];
	char lineitem_1[100], lineitem_6[100], lineitem_7[100], lineitem_11[100];

	if (argc != 2) {
		fprintf(stderr, "Wrong number of arguments\n%s <dataset>\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	dataset = (int) log2 ((double) atoi(argv[1]));
	//dataset = 8;

	strcpy(customer_1, dataset_path);
	strcat(customer_1, argv[1]);
	strcat(customer_1, "/customer_cols/customer_1.tbl");

	strcpy(customer_7, dataset_path);
	strcat(customer_7, argv[1]);
	strcat(customer_7, "/customer_cols/customer_7.tbl");

	strcpy(orders_1, dataset_path);
	strcat(orders_1, argv[1]);
	strcat(orders_1, "/orders_cols/orders_1.tbl");

	strcpy(orders_2, dataset_path);
	strcat(orders_2, argv[1]);
	strcat(orders_2, "/orders_cols/orders_2.tbl");

	strcpy(orders_5, dataset_path);
	strcat(orders_5, argv[1]);
	strcat(orders_5, "/orders_cols/orders_5.tbl");

	strcpy(orders_8, dataset_path);
	strcat(orders_8, argv[1]);
	strcat(orders_8, "/orders_cols/orders_8.tbl");

	strcpy(lineitem_1, dataset_path);
	strcat(lineitem_1, argv[1]);
	strcat(lineitem_1, "/lineitem_cols/lineitem_1.tbl");

	strcpy(lineitem_6, dataset_path);
	strcat(lineitem_6, argv[1]);
	strcat(lineitem_6, "/lineitem_cols/lineitem_6.tbl");

	strcpy(lineitem_7, dataset_path);
	strcat(lineitem_7, argv[1]);
	strcat(lineitem_7, "/lineitem_cols/lineitem_7.tbl");

	strcpy(lineitem_11, dataset_path);
	strcat(lineitem_11, argv[1]);
	strcat(lineitem_11, "/lineitem_cols/lineitem_11.tbl");

	/* c_custkey */
	__declspec(align(MEM_LINE_SIZE)) double *c_custkey_values;
	__declspec(align(MEM_LINE_SIZE)) long *c_custkey_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *c_custkey_col_ptr;

	long c_custkey_n_rows;
	long c_custkey_n_cols;
	long c_custkey_nnz;

	/* c_mktsegment */
	__declspec(align(MEM_LINE_SIZE)) double *c_mktsegment_values;
	__declspec(align(MEM_LINE_SIZE)) long *c_mktsegment_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *c_mktsegment_col_ptr;

	long c_mktsegment_n_rows;
	long c_mktsegment_n_cols;
	long c_mktsegment_nnz;

	/* o_orderkey */
	__declspec(align(MEM_LINE_SIZE)) double *o_orderkey_values;
	__declspec(align(MEM_LINE_SIZE)) long *o_orderkey_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *o_orderkey_col_ptr;

	long o_orderkey_n_rows;
	long o_orderkey_n_cols;
	long o_orderkey_nnz;

	/* o_custkey */
	__declspec(align(MEM_LINE_SIZE)) double *o_custkey_values;
	__declspec(align(MEM_LINE_SIZE)) long *o_custkey_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *o_custkey_col_ptr;

	long o_custkey_n_rows;
	long o_custkey_n_cols;
	long o_custkey_nnz;

	/* o_orderdate */
	__declspec(align(MEM_LINE_SIZE)) double *o_orderdate_values;
	__declspec(align(MEM_LINE_SIZE)) long *o_orderdate_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *o_orderdate_col_ptr;

	long o_orderdate_n_rows;
	long o_orderdate_n_cols;
	long o_orderdate_nnz;

	/* o_shippriority */
	__declspec(align(MEM_LINE_SIZE)) double *o_shippriority_values;
	__declspec(align(MEM_LINE_SIZE)) long *o_shippriority_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *o_shippriority_col_ptr;

	long o_shippriority_n_rows;
	long o_shippriority_n_cols;
	long o_shippriority_nnz;

	/* l_orderkey */
	__declspec(align(MEM_LINE_SIZE)) double *l_orderkey_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_orderkey_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_orderkey_col_ptr;

	long l_orderkey_n_rows;
	long l_orderkey_n_cols;
	long l_orderkey_nnz;

	/* l_extendedprice */
	__declspec(align(MEM_LINE_SIZE)) double *l_extendedprice_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_extendedprice_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_extendedprice_col_ptr;

	long l_extendedprice_n_rows;
	long l_extendedprice_n_cols;
	long l_extendedprice_nnz;

	/* l_discount */
	__declspec(align(MEM_LINE_SIZE)) double *l_discount_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_discount_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_discount_col_ptr;

	long l_discount_n_rows;
	long l_discount_n_cols;
	long l_discount_nnz;

	/* l_shipdate */
	__declspec(align(MEM_LINE_SIZE)) double *l_shipdate_values;
	__declspec(align(MEM_LINE_SIZE)) long *l_shipdate_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *l_shipdate_col_ptr;

	long l_shipdate_n_rows;
	long l_shipdate_n_cols;
	long l_shipdate_nnz;

	/* A */
	__declspec(align(MEM_LINE_SIZE)) double *A_values;
	__declspec(align(MEM_LINE_SIZE)) long *A_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *A_col_ptr;

	long A_n_rows;
	long A_n_cols;
	long A_nnz;

	/* B */
	__declspec(align(MEM_LINE_SIZE)) double *B_values;
	__declspec(align(MEM_LINE_SIZE)) long *B_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *B_col_ptr;

	long B_n_rows;
	long B_n_cols;
	long B_nnz;

	/* C */
	__declspec(align(MEM_LINE_SIZE)) double *C_values;
	__declspec(align(MEM_LINE_SIZE)) long *C_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *C_col_ptr;

	long C_n_rows;
	long C_n_cols;
	long C_nnz;

	/* D */
	__declspec(align(MEM_LINE_SIZE)) double *D_values;
	__declspec(align(MEM_LINE_SIZE)) long *D_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *D_col_ptr;

	long D_n_rows;
	long D_n_cols;
	long D_nnz;

	/* E */
	__declspec(align(MEM_LINE_SIZE)) double *E_values;
	__declspec(align(MEM_LINE_SIZE)) long *E_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *E_col_ptr;

	long E_n_rows;
	long E_n_cols;
	long E_nnz;

	/* F */
	__declspec(align(MEM_LINE_SIZE)) double *F_values;
	__declspec(align(MEM_LINE_SIZE)) long *F_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *F_col_ptr;

	long F_n_rows;
	long F_n_cols;
	long F_nnz;

	/* G */
	__declspec(align(MEM_LINE_SIZE)) double *G_values;
	__declspec(align(MEM_LINE_SIZE)) long *G_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *G_col_ptr;

	long G_n_rows;
	long G_n_cols;
	long G_nnz;

	/* H */
	__declspec(align(MEM_LINE_SIZE)) double *H_values;
	__declspec(align(MEM_LINE_SIZE)) long *H_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *H_col_ptr;

	long H_n_rows;
	long H_n_cols;
	long H_nnz;

	/* I */
	__declspec(align(MEM_LINE_SIZE)) double *I_values;
	__declspec(align(MEM_LINE_SIZE)) long *I_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *I_col_ptr;

	long I_n_rows;
	long I_n_cols;
	long I_nnz;

	/* J */
	__declspec(align(MEM_LINE_SIZE)) double *J_values;
	__declspec(align(MEM_LINE_SIZE)) long *J_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *J_col_ptr;

	long J_n_rows;
	long J_n_cols;
	long J_nnz;

	/* K */
	__declspec(align(MEM_LINE_SIZE)) double *K_values;
	__declspec(align(MEM_LINE_SIZE)) long *K_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *K_col_ptr;

	long K_n_rows;
	long K_n_cols;
	long K_nnz;

	/* L */
	__declspec(align(MEM_LINE_SIZE)) double *L_values;
	__declspec(align(MEM_LINE_SIZE)) long *L_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *L_col_ptr;

	long L_n_rows;
	long L_n_cols;
	long L_nnz;

	/* Labels custkey */
	GHashTable *custkey_id_label = NULL, *custkey_label_id = NULL;
	long custkey_next_id_label;

	/* Labels mktsegment */
	GHashTable *mktsegment_id_label = NULL, *mktsegment_label_id = NULL;
	long mktsegment_next_id_label;

	/* Labels orderkey */
	GHashTable *orderkey_id_label = NULL, *orderkey_label_id = NULL;
	long orderkey_next_id_label;

	/* Labels orderdate */
	GHashTable *orderdate_id_label = NULL, *orderdate_label_id = NULL;
	long orderdate_next_id_label;

	/* Labels shippriority */
	GHashTable *shippriority_id_label = NULL, *shippriority_label_id = NULL;
	long shippriority_next_id_label;

	/* Labels shipdate */
	GHashTable *shipdate_id_label = NULL, *shipdate_label_id = NULL;
	long shipdate_next_id_label;

	#ifdef D_VERBOSE
	printf("\nLoad Dataset\n\n");
	#endif

	/* load c_custkey */
	read_column(
		customer_1, num_elems[0][dataset],
		&c_custkey_values, &c_custkey_row_ind, &c_custkey_col_ptr,
		&c_custkey_nnz, &c_custkey_n_rows, &c_custkey_n_cols,
		&custkey_id_label, &custkey_label_id, &custkey_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"c_custkey",
    	c_custkey_values, c_custkey_row_ind, c_custkey_col_ptr,
		c_custkey_nnz, c_custkey_n_rows, c_custkey_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "c_custkey", c_custkey_n_rows, c_custkey_n_cols, c_custkey_nnz);
	#endif

	/* load c_mktsegment */
	read_column(
		customer_7, num_elems[0][dataset],
		&c_mktsegment_values, &c_mktsegment_row_ind, &c_mktsegment_col_ptr,
		&c_mktsegment_nnz, &c_mktsegment_n_rows, &c_mktsegment_n_cols,
		&mktsegment_id_label, &mktsegment_label_id, &mktsegment_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"c_mktsegment",
    	c_mktsegment_values, c_mktsegment_row_ind, c_mktsegment_col_ptr,
		c_mktsegment_nnz, c_mktsegment_n_rows, c_mktsegment_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "c_mktsegment", c_mktsegment_n_rows, c_mktsegment_n_cols, c_mktsegment_nnz);
	#endif

	/* load o_orderkey */
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

	/* load o_custkey */
	read_column(
		orders_2, num_elems[1][dataset],
		&o_custkey_values, &o_custkey_row_ind, &o_custkey_col_ptr,
		&o_custkey_nnz, &o_custkey_n_rows, &o_custkey_n_cols,
		&custkey_id_label, &custkey_label_id, &custkey_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"o_custkey",
    	o_custkey_values, o_custkey_row_ind, o_custkey_col_ptr,
		o_custkey_nnz, o_custkey_n_rows, o_custkey_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "o_custkey", o_custkey_n_rows, o_custkey_n_cols, o_custkey_nnz);
	#endif

	/* load o_orderdate */
	read_column(
		orders_5, num_elems[1][dataset],
		&o_orderdate_values, &o_orderdate_row_ind, &o_orderdate_col_ptr,
		&o_orderdate_nnz, &o_orderdate_n_rows, &o_orderdate_n_cols,
		&orderdate_id_label, &orderdate_label_id, &orderdate_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"o_orderdate",
    	o_orderdate_values, o_orderdate_row_ind, o_orderdate_col_ptr,
		o_orderdate_nnz, o_orderdate_n_rows, o_orderdate_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "o_orderdate", o_orderdate_n_rows, o_orderdate_n_cols, o_orderdate_nnz);
	#endif

	/* load o_shippriority */
	read_column(
		orders_8, num_elems[1][dataset],
		&o_shippriority_values, &o_shippriority_row_ind, &o_shippriority_col_ptr,
		&o_shippriority_nnz, &o_shippriority_n_rows, &o_shippriority_n_cols,
		&shippriority_id_label, &shippriority_label_id, &shippriority_next_id_label
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"o_shippriority",
    	o_shippriority_values, o_shippriority_row_ind, o_shippriority_col_ptr,
		o_shippriority_nnz, o_shippriority_n_rows, o_shippriority_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "o_shippriority", o_shippriority_n_rows, o_shippriority_n_cols, o_shippriority_nnz);
	#endif

	/* load l_orderkey */
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

	/* load l_extendedprice */
	read_column_measure(
		lineitem_6, num_elems[2][dataset],
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

	/* load l_discount */
	read_column_measure(
		lineitem_7, num_elems[2][dataset],
		&l_discount_values, &l_discount_row_ind, &l_discount_col_ptr,
		&l_discount_nnz, &l_discount_n_rows, &l_discount_n_cols
		);

	#ifdef D_VERBOSE
	/*
	print_column(
		"l_discount",
    	l_discount_values, l_discount_row_ind, l_discount_col_ptr,
		l_discount_nnz, l_discount_n_rows, l_discount_n_cols
    	);
	*/
	printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "l_discount", l_discount_n_rows, l_discount_n_cols, l_discount_nnz);
	#endif

	/* load l_shipdate */
	read_column(
		lineitem_11, num_elems[2][dataset],
		&l_shipdate_values, &l_shipdate_row_ind, &l_shipdate_col_ptr,
		&l_shipdate_nnz, &l_shipdate_n_rows, &l_shipdate_n_cols,
		&shipdate_id_label, &shipdate_label_id, &shipdate_next_id_label
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

	#ifdef D_VERBOSE
	printf("\nInicio Query 3\n\n");
	#endif

	GET_TIME(start);

/*
 * A: D <- #o
 * A = matrix_to_matrix_filter( o_orderdate, <'1995-03-10' )
 * A : Matrix with at most one '1' by column
 *
 */

	#ifdef D_PROFILE
	START();
	#endif

	sp_bm_bm_filter_seq(
		o_orderdate_values, o_orderdate_row_ind, o_orderdate_col_ptr,
		o_orderdate_nnz, o_orderdate_n_rows, o_orderdate_n_cols,
		orderdate_id_label,
		(int(*)(const void*,const void*)) strcmp,
		LESS, "1995-03-10",
		&A_values, &A_row_ind, &A_col_ptr,
		&A_nnz, &A_n_rows, &A_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free o_orderdate
	_mm_free(o_orderdate_values);
	_mm_free(o_orderdate_row_ind);
	_mm_free(o_orderdate_col_ptr);

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

/* (1.1.3)
 *
 * B: 1 <- #c
 * B = filter( c_mktsegment, ='MACHINERY' )
 *
 * B : Vector with at most one '1' by column
 *
 * C: 1 <- #l
 * C = filter( l_shipdate, >'1995-03-10' )
 *
 * C : Vector with at most one '1' by column
 *
 */

	#ifdef D_PROFILE
	START();
	#endif

	sp_bm_bv_filter_seq(
		c_mktsegment_values, c_mktsegment_row_ind, c_mktsegment_col_ptr,
		c_mktsegment_nnz, c_mktsegment_n_rows, c_mktsegment_n_cols,
		mktsegment_id_label,
		(int(*)(const void*,const void*)) strcmp,
		EQUAL, "MACHINERY",
		&B_values, &B_row_ind, &B_col_ptr,
		&B_nnz, &B_n_rows, &B_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free c_mktsegment
	_mm_free(c_mktsegment_values);
	_mm_free(c_mktsegment_row_ind);
	_mm_free(c_mktsegment_col_ptr);

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

	sp_bm_bv_filter_seq(
		l_shipdate_values, l_shipdate_row_ind, l_shipdate_col_ptr,
		l_shipdate_nnz, l_shipdate_n_rows, l_shipdate_n_cols,
		shipdate_id_label,
		(int(*)(const void*,const void*)) strcmp,
		GREATER, "1995-03-10",
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

/* (4.1)
 *
 * H: 1 <- #o
 * H = dot( B, o_custkey )
 *
 * H : Vector with at most one '1' by column
 *     -> B         : Vector with at most one '1' by column
 *     -> o_custkey : Matrix with exactly one '1' by column
 */

	#ifdef D_PROFILE
	START();
	#endif

	sp_bvbm_dot_product_seq(
		B_values, B_row_ind, B_col_ptr,
		B_nnz, B_n_rows, B_n_cols,
		o_custkey_values, o_custkey_row_ind, o_custkey_col_ptr,
		o_custkey_nnz, o_custkey_n_rows, o_custkey_n_cols,
		&D_values, &D_row_ind, &D_col_ptr,
		&D_nnz, &D_n_rows, &D_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free B
	_mm_free(B_values);
	_mm_free(B_row_ind);
	_mm_free(B_col_ptr);

	// free o_custkey
	_mm_free(o_custkey_values);
	_mm_free(o_custkey_row_ind);
	_mm_free(o_custkey_col_ptr);

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

/* (4.2)
 *
 * E: K <- #l
 * E = krao_matrix_vector( l_orderkey, C )
 *
 * E : Matrix with at most one '1' by column
 *     -> l_orderkey : Matrix with exactly one '1' by column
 *     -> C          : Vector with at most one '1' by column
 *
 * F: D <- #o
 * F = krao_matrix_vector( A, D )
 *
 * F : Matrix with at most one '1' by column
 *     -> A : with at most one '1' by column
 *     -> D : with at most one '1' by column
 *
 * G: DxP <- #o
 * G = krao_matrix_matrix( F, o_shippriority )
 *
 * G : Matrix with at most one '1' by column
 *     -> F              : Matrix with at most one '1' by column
 *     -> o_shippriority : Matrix with exactly one '1' by column
 *
 */

	#ifdef D_PROFILE
	START();
	#endif

	sp_bmbv_krao_seq(
		l_orderkey_values, l_orderkey_row_ind, l_orderkey_col_ptr,
		l_orderkey_nnz, l_orderkey_n_rows, l_orderkey_n_cols,
		C_values, C_row_ind, C_col_ptr,
		C_nnz, C_n_rows, C_n_cols,
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

	sp_bmbv_krao_seq(
		A_values, A_row_ind, A_col_ptr,
		A_nnz, A_n_rows, A_n_cols,
		D_values, D_row_ind, D_col_ptr,
		D_nnz, D_n_rows, D_n_cols,
		&F_values, &F_row_ind, &F_col_ptr,
		&F_nnz, &F_n_rows, &F_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free A
	_mm_free(A_values);
	_mm_free(A_row_ind);
	_mm_free(A_col_ptr);

	// free D
	_mm_free(D_values);
	_mm_free(D_row_ind);
	_mm_free(D_col_ptr);

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

	sp_bmbm_krao_seq(
		F_values, F_row_ind, F_col_ptr,
		F_nnz, F_n_rows, F_n_cols,
		o_shippriority_values, o_shippriority_row_ind, o_shippriority_col_ptr,
		o_shippriority_nnz, o_shippriority_n_rows, o_shippriority_n_cols,
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

	// free o_shippriority
	_mm_free(o_shippriority_values);
	_mm_free(o_shippriority_row_ind);
	_mm_free(o_shippriority_col_ptr);

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

/* (4.1)
 *
 * H: DxP <- #l
 * H = dot( G, l_orderkey )
 *
 * H : Matrix with at most one '1' by column
 *     -> G          : Matrix with exactly one '1' by column
 *     -> l_orderkey : Matrix with at most one '1' by column
 */

	#ifdef D_PROFILE
	START();
	#endif

	sp_bmbm_dot_product_seq(
		G_values, G_row_ind, G_col_ptr,
		G_nnz, G_n_rows, G_n_cols,
		l_orderkey_values, l_orderkey_row_ind, l_orderkey_col_ptr,
		l_orderkey_nnz, l_orderkey_n_rows, l_orderkey_n_cols,
		&H_values, &H_row_ind, &H_col_ptr,
		&H_nnz, &H_n_rows, &H_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free G
	_mm_free(G_values);
	_mm_free(G_row_ind);
	_mm_free(G_col_ptr);

	// free l_orderkey
	_mm_free(l_orderkey_values);
	_mm_free(l_orderkey_row_ind);
	_mm_free(l_orderkey_col_ptr);

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

/* (4.2)
 *
 * I: KxDxP <- #l
 * I = krao_matrix_matrix( E, H )
 *
 * I : Matrix with at most one '1' by column
 *     -> E : Matrix with at most one '1' by column
 *     -> H : Matrix with at most one '1' by column
 *
 */

	#ifdef D_PROFILE
	START();
	#endif

	sp_bmbm_krao_seq(
		E_values, E_row_ind, E_col_ptr,
		E_nnz, E_n_rows, E_n_cols,
		H_values, H_row_ind, H_col_ptr,
		H_nnz, H_n_rows, H_n_cols,
		&I_values, &I_row_ind, &I_col_ptr,
		&I_nnz, &I_n_rows, &I_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free E
	_mm_free(E_values);
	_mm_free(E_row_ind);
	_mm_free(E_col_ptr);

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

/* (6)
 *
 * J: 1 - l_discount
 *    -> l_discount : horizontal vector full of values
 *
 * K: l_extendedprice >< J
 *    -> l_extendedprice : horizontal vector full of values
 *    -> J               : horizontal vector full of values
 *
 *
 *
 * Q : Vertical vector with zeros
 *     -> I : Matrix with at most one '1' by column
 *     -> P : Vertical vector full of values
 *
 */

	#ifdef D_PROFILE
	START();
	#endif

	sp_v_bang_seq(
		l_discount_values, l_discount_row_ind, l_discount_col_ptr,
		l_discount_nnz, l_discount_n_rows, l_discount_n_cols,
		&J_values, &J_row_ind, &J_col_ptr,
		&J_nnz, &J_n_rows, &J_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free l_discount
	_mm_free(l_discount_values);
	_mm_free(l_discount_row_ind);
	_mm_free(l_discount_col_ptr);

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

	sp_vv_hadamard_seq(
		l_extendedprice_values, l_extendedprice_row_ind, l_extendedprice_col_ptr,
		l_extendedprice_nnz, l_extendedprice_n_rows, l_extendedprice_n_cols,
		J_values, J_row_ind, J_col_ptr,
		J_nnz, J_n_rows, J_n_cols,
		&K_values, &K_row_ind, &K_col_ptr,
		&K_nnz, &K_n_rows, &K_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free l_extendedprice
	_mm_free(l_extendedprice_values);
	_mm_free(l_extendedprice_row_ind);
	_mm_free(l_extendedprice_col_ptr);

	// free J
	_mm_free(J_values);
	_mm_free(J_row_ind);
	_mm_free(J_col_ptr);

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

	sp_bmv_krao_seq(
		I_values, I_row_ind, I_col_ptr,
		I_nnz, I_n_rows, I_n_cols,
		K_values, K_row_ind, K_col_ptr,
		K_nnz, K_n_rows, K_n_cols,
		&L_values, &L_row_ind, &L_col_ptr,
		&L_nnz, &L_n_rows, &L_n_cols
		);

	#ifdef D_PROFILE
	STOP();
	#endif

	// free I
	_mm_free(I_values);
	_mm_free(I_row_ind);
	_mm_free(I_col_ptr);

	// free K
	_mm_free(K_values);
	_mm_free(K_row_ind);
	_mm_free(K_col_ptr);

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

	Tree t = createSumTree();
	t = fromCSCtoSumTree(t, L_col_ptr, L_row_ind, L_values, L_n_cols, L_n_rows, L_nnz);

	#ifdef D_PROFILE
	STOP();
	#endif

	GET_TIME(finish);
	elapsed = finish - start;
	printf("Execution Time:\t%f\nNNZ:\t%ld\n", elapsed, L_nnz);

	#ifdef D_PROFILE
	STOP_PROFILE();
	#endif

	//printTree(t);

	// free L
	_mm_free(L_values);
	_mm_free(L_row_ind);
	_mm_free(L_col_ptr);

	/* Labels custkey */
	g_hash_table_destroy(custkey_id_label);
	g_hash_table_destroy(custkey_label_id);

	/* Labels mktsegment */
	g_hash_table_destroy(mktsegment_id_label);
	g_hash_table_destroy(mktsegment_label_id);

	/* Labels orderkey */
	g_hash_table_destroy(orderkey_id_label);
	g_hash_table_destroy(orderkey_label_id);

	/* Labels orderdate */
	g_hash_table_destroy(orderdate_id_label);
	g_hash_table_destroy(orderdate_label_id);

	/* Labels shippriority */
	g_hash_table_destroy(shippriority_id_label);
	g_hash_table_destroy(shippriority_label_id);

	/* Labels shipdate */
	g_hash_table_destroy(shipdate_id_label);
	g_hash_table_destroy(shipdate_label_id);

	return EXIT_SUCCESS;
}
