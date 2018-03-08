#include <stdio.h>
#include <glib.h>
#include <math.h>

#include "../common/timer.h"
#include "../common/tools.h"
#include "../common/strings.h"
#include "../common/operators.h"
#include "../common/altavl.h"

int main(int argc, char *argv[]) {

        double start, finish, elapsed;

        int dataset;

        #ifdef D_PROFILE
        START_PROFILE();
        #endif

        long num_elems[4][9] = {
                                                   {  150000,   300000,   600000,  1200000,  2400000,   4800000,   9600000,  19200000, 25 }, // customer
                                                   { 1500000,  3000000,  6000000, 12000000, 24000000,  48000000,  96000000, 192000000, 51 }, // orders
                                                   { 6001215, 11997996, 23996604, 47989007, 95988640, 192000551, 384016850, 768046938, 91 }, // lineitem
                                                   {      25,       25,       25,       25,       25,        25,        25,        25, 25 }  // nation
                                                  };

        char *dataset_path = getenv("LA_DATA_DIR");

        char orders_1[100], orders_5[100], orders_6[100];
        char lineitem_1[100], lineitem_12[100], lineitem_13[100];

        if (argc != 2) {
                fprintf(stderr, "Wrong number of arguments\n%s <dataset>\n", argv[0]);
                exit(EXIT_FAILURE);
        }

        dataset = (int) log2 ((double) atoi(argv[1]));
        //dataset = 8;

        // Orders

        // o_orderkey
        strcpy(orders_1, dataset_path);
        strcat(orders_1, argv[1]);
        strcat(orders_1, "/orders_cols/orders_1.tbl");

        // o_orderdate
        strcpy(orders_5, dataset_path);
        strcat(orders_5, argv[1]);
        strcat(orders_5, "/orders_cols/orders_5.tbl");

        // o_orderpriority
        strcpy(orders_6, dataset_path);
        strcat(orders_6, argv[1]);
        strcat(orders_6, "/orders_cols/orders_6.tbl");

        // Lineitem

        // l_orderkey
        strcpy(lineitem_1, dataset_path);
        strcat(lineitem_1, argv[1]);
        strcat(lineitem_1, "/lineitem_cols/lineitem_1.tbl");

        // l_commitdate
        strcpy(lineitem_12, dataset_path);
        strcat(lineitem_12, argv[1]);
        strcat(lineitem_12, "/lineitem_cols/lineitem_12.tbl");

        // l_receiptdate
        strcpy(lineitem_13, dataset_path);
        strcat(lineitem_13, argv[1]);
        strcat(lineitem_13, "/lineitem_cols/lineitem_13.tbl");


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

        // o_orderdate
        __declspec(align(MEM_LINE_SIZE)) double *o_orderdate_values;
        __declspec(align(MEM_LINE_SIZE)) long *o_orderdate_row_ind;
        __declspec(align(MEM_LINE_SIZE)) long *o_orderdate_col_ptr;

        long o_orderdate_n_rows;
        long o_orderdate_n_cols;
        long o_orderdate_nnz;

        // Lineitem

        // l_orderkey
        __declspec(align(MEM_LINE_SIZE)) double *l_orderkey_values;
        __declspec(align(MEM_LINE_SIZE)) long *l_orderkey_row_ind;
        __declspec(align(MEM_LINE_SIZE)) long *l_orderkey_col_ptr;

        long l_orderkey_n_rows;
        long l_orderkey_n_cols;
        long l_orderkey_nnz;

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

        // Iaux
        Tree Iaux = createExistsTree();

        // I
        __declspec(align(MEM_LINE_SIZE)) double *I_values;
        __declspec(align(MEM_LINE_SIZE)) long *I_row_ind;
        __declspec(align(MEM_LINE_SIZE)) long *I_col_ptr;

        long I_n_rows;
        long I_n_cols;
        long I_nnz;

        //J
        __declspec(align(MEM_LINE_SIZE)) double *J_values;
        __declspec(align(MEM_LINE_SIZE)) long *J_row_ind;
        __declspec(align(MEM_LINE_SIZE)) long *J_col_ptr;

        long J_n_rows;
        long J_n_cols;
        long J_nnz;

        //Kaux
        Tree Kaux = createExistsTree();

        //K
        __declspec(align(MEM_LINE_SIZE)) double *K_values;
        __declspec(align(MEM_LINE_SIZE)) long *K_row_ind;
        __declspec(align(MEM_LINE_SIZE)) long *K_col_ptr;

        long K_n_rows;
        long K_n_cols;
        long K_nnz;


        // Labels

        // orderkey
        GHashTable *orderkey_id_label = NULL, *orderkey_label_id = NULL;
        long orderkey_next_id_label;

        // o_orderdate
        GHashTable *o_orderdate_id_label = NULL, *o_orderdate_label_id = NULL;
        long o_orderdate_next_id_label;

        // o_orderpriority
        GHashTable *o_orderpriority_id_label = NULL, *o_orderpriority_label_id = NULL;
        long o_orderpriority_next_id_label;

        // l_commitdate
        GHashTable *l_commitdate_id_label = NULL, *l_commitdate_label_id = NULL;
        long l_commitdate_next_id_label;

        // l_receiptdate
        GHashTable *l_receiptdate_id_label = NULL, *l_receiptdate_label_id = NULL;
        long l_receiptdate_next_id_label;

        // Load Dataset

        // o_orderkey
        read_column(
                orders_1, num_elems[1][dataset],
                &o_orderkey_values, &o_orderkey_row_ind, &o_orderkey_col_ptr,
                &o_orderkey_nnz, &o_orderkey_n_rows, &o_orderkey_n_cols,
                &orderkey_id_label, &orderkey_label_id, &orderkey_next_id_label
                );

        _mm_free(o_orderkey_col_ptr);
        _mm_free(o_orderkey_row_ind);
        _mm_free(o_orderkey_values);

        #ifdef D_VERBOSE
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "o_orderkey", o_orderkey_n_rows, o_orderkey_n_cols, o_orderkey_nnz);
        #endif

        // o_orderdate
        read_column(
                orders_5, num_elems[1][dataset],
                &o_orderdate_values, &o_orderdate_row_ind, &o_orderdate_col_ptr,
                &o_orderdate_nnz, &o_orderdate_n_rows, &o_orderdate_n_cols,
                &o_orderdate_id_label, &o_orderdate_label_id, &o_orderdate_next_id_label
                );

        #ifdef D_VERBOSE
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "o_orderdate", o_orderdate_n_rows, o_orderdate_n_cols, o_orderdate_nnz);
        #endif

        // o_orderpriority
        read_column(
                orders_6, num_elems[1][dataset],
                &o_orderpriority_values, &o_orderpriority_row_ind, &o_orderpriority_col_ptr,
                &o_orderpriority_nnz, &o_orderpriority_n_rows, &o_orderpriority_n_cols,
                &o_orderpriority_id_label, &o_orderpriority_label_id, &o_orderpriority_next_id_label
                );

        #ifdef D_VERBOSE
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
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "l_orderkey", l_orderkey_n_rows, l_orderkey_n_cols, l_orderkey_nnz);
        #endif

        // l_commitdate
        read_column(
                lineitem_12, num_elems[2][dataset],
                &l_commitdate_values, &l_commitdate_row_ind, &l_commitdate_col_ptr,
                &l_commitdate_nnz, &l_commitdate_n_rows, &l_commitdate_n_cols,
                &l_commitdate_id_label, &l_commitdate_label_id, &l_commitdate_next_id_label
                );

        #ifdef D_VERBOSE
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
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "l_receiptdate", l_receiptdate_n_rows, l_receiptdate_n_cols, l_receiptdate_nnz);
        #endif


        // Execute Query 4

        GET_TIME(start);

        // A = filter( l_commitdate < l_receiptdate )
        sp_bmbm_bv_filter_seq(
                l_commitdate_values, l_commitdate_row_ind, l_commitdate_col_ptr,
        l_commitdate_nnz, l_commitdate_n_rows, l_commitdate_n_cols,
        l_commitdate_id_label,
        l_receiptdate_values, l_receiptdate_row_ind, l_receiptdate_col_ptr,
        l_receiptdate_nnz, l_receiptdate_n_rows, l_receiptdate_n_cols,
        l_receiptdate_id_label,
        (int(*)(const void*,const void*)) strcmp,
            LESS,
            &A_values, &A_row_ind, &A_col_ptr,
            &A_nnz, &A_n_rows, &A_n_cols
                );

        // free l_commitdate
        _mm_free(l_commitdate_values);
        _mm_free(l_commitdate_row_ind);
        _mm_free(l_commitdate_col_ptr);

        // free l_receiptdate
        _mm_free(l_receiptdate_values);
        _mm_free(l_receiptdate_row_ind);
        _mm_free(l_receiptdate_col_ptr);

        #ifdef D_VERBOSE
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "A", A_n_rows, A_n_cols, A_nnz);
        #endif

        #ifdef D_PROFILE
        START();
        #endif

        // B = krao( l_orderkey, A )
        sp_bmbv_krao_seq(
                l_orderkey_values, l_orderkey_row_ind, l_orderkey_col_ptr,
        l_orderkey_nnz, l_orderkey_n_rows, l_orderkey_n_cols,
        A_values, A_row_ind, A_col_ptr,
        A_nnz, A_n_rows, A_n_cols,
            &B_values, &B_row_ind, &B_col_ptr,
            &B_nnz, &B_n_rows, &B_n_cols
                );

        #ifdef D_PROFILE
        STOP();
        #endif

        // free l_orderkey
        _mm_free(l_orderkey_values);
        _mm_free(l_orderkey_row_ind);
        _mm_free(l_orderkey_col_ptr);

        // free A
        _mm_free(A_values);
        _mm_free(A_row_ind);
        _mm_free(A_col_ptr);

        #ifdef D_VERBOSE
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "B", B_n_rows, B_n_cols, B_nnz);
        #endif


        // C = filter( o_orderdate < '1993-07-01' )
        sp_bm_bv_filter_seq(
                o_orderdate_values, o_orderdate_row_ind, o_orderdate_col_ptr,
            o_orderdate_nnz, o_orderdate_n_rows, o_orderdate_n_cols,
                o_orderdate_id_label,
                (int(*)(const void*,const void*)) strcmp,
                LESS, "1993-10-01",
            &C_values, &C_row_ind, &C_col_ptr,
            &C_nnz, &C_n_rows, &C_n_cols
                );

        #ifdef D_VERBOSE
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "C", C_n_rows, C_n_cols, C_nnz);
        #endif


        // D = filter( o_orderdate >= '1993-07-01' )
        sp_bm_bv_filter_seq(
                o_orderdate_values, o_orderdate_row_ind, o_orderdate_col_ptr,
            o_orderdate_nnz, o_orderdate_n_rows, o_orderdate_n_cols,
                o_orderdate_id_label,
                (int(*)(const void*,const void*)) strcmp,
                GREATER_EQ, "1993-07-01",
            &D_values, &D_row_ind, &D_col_ptr,
            &D_nnz, &D_n_rows, &D_n_cols
                );

        // free o_orderdate
        _mm_free(o_orderdate_values);
        _mm_free(o_orderdate_row_ind);
        _mm_free(o_orderdate_col_ptr);

        #ifdef D_VERBOSE
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "D", D_n_rows, D_n_cols, D_nnz);
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

        // F = krao( o_orderpriority, E )
        sp_bmbv_krao_seq(
                o_orderpriority_values, o_orderpriority_row_ind, o_orderpriority_col_ptr,
            o_orderpriority_nnz, o_orderpriority_n_rows, o_orderpriority_n_cols,
            E_values, E_row_ind, E_col_ptr,
            E_nnz, E_n_rows, E_n_cols,
            &F_values, &F_row_ind, &F_col_ptr,
            &F_nnz, &F_n_rows, &F_n_cols
                );

        // free o_orderpriority
        _mm_free(o_orderpriority_values);
        _mm_free(o_orderpriority_row_ind);
        _mm_free(o_orderpriority_col_ptr);

        // free E
        _mm_free(E_values);
        _mm_free(E_row_ind);
        _mm_free(E_col_ptr);

        #ifdef D_VERBOSE
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "F", F_n_rows, F_n_cols, F_nnz);
        #endif

        // G = dot ( F, B )
        sp_bmbm_dot_product_seq(
            F_values, F_row_ind, F_col_ptr,
            F_nnz, F_n_rows, F_n_cols,
            B_values, B_row_ind, B_col_ptr,
            B_nnz, B_n_rows, B_n_cols,
            &G_values, &G_row_ind, &G_col_ptr,
            &G_nnz, &G_n_rows, &G_n_cols
            );

        // free F
        _mm_free(F_values);
        _mm_free(F_row_ind);
        _mm_free(F_col_ptr);

        #ifdef D_VERBOSE
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "G", G_n_rows, G_n_cols, G_nnz);
        #endif

        // H = krao ( B , G )
        sp_bmbm_krao_seq(
            B_values, B_row_ind, B_col_ptr,
            B_nnz, B_n_rows, B_n_cols,
            G_values, G_row_ind, G_col_ptr,
            G_nnz, G_n_rows, G_n_cols,
            &H_values, &H_row_ind, &H_col_ptr,
            &H_nnz, &H_n_rows, &H_n_cols
            );

        // free B
        _mm_free(B_values);
        _mm_free(B_row_ind);
        _mm_free(B_col_ptr);

        // free G
        _mm_free(G_values);
        _mm_free(G_row_ind);
        _mm_free(G_col_ptr);

        #ifdef D_VERBOSE
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "H", H_n_rows, H_n_cols, H_nnz);
        #endif

        // Iaux = avl_exists( H )
        Iaux = fromCSCtoExistsTree(Iaux,
                H_col_ptr, H_row_ind, H_values,
                H_n_cols, H_n_rows, H_nnz
                );

        // free H
        _mm_free(H_values);
        _mm_free(H_row_ind);
        _mm_free(H_col_ptr);

        #ifdef D_VERBOSE
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "Iaux", H_n_rows, (long) 1, (long) treeSize(Iaux));
        #endif

        // I = avl_to_vector( Iaux )
        fromExistsTreeToCSC_Col(Iaux, H_n_rows,
                &I_col_ptr, &I_row_ind, &I_values,
                &I_n_cols, &I_n_rows, &I_nnz
                );

        // free Iaux
        emptyTree(Iaux);

        #ifdef D_VERBOSE
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "I", I_n_rows, I_n_cols, I_nnz);
        #endif

        // J = unvec ( I )
        unvec_v_col(
            // input:
                // #rows of output matrix
                G_n_rows,
                //columnar vector I
                I_values, I_row_ind, I_col_ptr,
                I_nnz, I_n_rows, I_n_cols,
            // output:
                //matrix J
                &J_values, &J_row_ind, &J_col_ptr,
                &J_nnz, &J_n_rows, &J_n_cols
            );

        #ifdef D_VERBOSE
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "J", J_n_rows, J_n_cols, J_nnz);
        #endif

        // Kaux = avl_count( J )
        Kaux = fromCSCtoCountTree(Kaux,
            J_col_ptr, J_row_ind, J_values,
            J_n_cols, J_n_rows, J_nnz
                );

        // free J
        _mm_free(J_values);
        _mm_free(J_row_ind);
        _mm_free(J_col_ptr);

        #ifdef D_VERBOSE
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "K_avl", J_n_rows, (long) 1, (long) treeSize(Kaux));
        #endif


        // K = avl_to_vector( Kaux )
        fromCountTreeToCSC(Kaux, J_n_rows,
            &K_col_ptr, &K_row_ind, &K_values,
            &K_n_cols, &K_n_rows, &K_nnz
                );

        // free Kaux
        emptyTree(Kaux);

        #ifdef D_VERBOSE
        printf("%20s :: %10ld >< %10ld\tNNZ = %10ld\n", "K", K_n_rows, K_n_cols, K_nnz);
        #endif

        double cenas = 0;
        for (int i = 0; i < K_nnz; ++i)
                cenas += K_values[i];
        printf("SUM = %f\n", cenas);

        GET_TIME(finish);
        elapsed = finish - start;
        printf("Execution Time:\t%f\nNNZ:\t%ld\n", elapsed, K_nnz);

        // free K
        _mm_free(K_values);
        _mm_free(K_row_ind);
        _mm_free(K_col_ptr);

        // Free Labels

        // orderkey
        g_hash_table_destroy(orderkey_id_label);
        g_hash_table_destroy(orderkey_label_id);

        // o_orderdate
        g_hash_table_destroy(o_orderdate_id_label);
        g_hash_table_destroy(o_orderdate_label_id);

        // o_orderpriority
        g_hash_table_destroy(o_orderpriority_id_label);
        g_hash_table_destroy(o_orderpriority_label_id);

        // l_commitdate
        g_hash_table_destroy(l_commitdate_id_label);
        g_hash_table_destroy(l_commitdate_label_id);

        // l_receiptdate
        g_hash_table_destroy(l_receiptdate_id_label);
        g_hash_table_destroy(l_receiptdate_label_id);

        return EXIT_SUCCESS;
}

