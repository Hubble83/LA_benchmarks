#include "altavl.h"

int compareByKey(void* a, void* b)
{
	long _a, _b;

	_a = ((struct exists_pair*) a)->key;
	_b = ((struct exists_pair*) b)->key;

	return (_a > _b) - (_a < _b);
}

// SUM

Tree createSumTree()
{
	return createTree();
}

Tree insertPairSumTree(Tree t, long key, double value)
{
	int bf;
	SumPair *aux;

	aux = (SumPair*) malloc(sizeof(SumPair));
	aux->key = key;
	aux = searchEntry(t, aux, compareByKey);

	if(!aux){
		aux = (SumPair*) malloc(sizeof(SumPair));
		aux->key = key;
		aux->value = value;
		t = insertTree(t, aux, &bf, compareByKey);
	}
	else{
		aux->value += value;
	}

	return t;
}

Tree fromCSCtoSumTree(Tree t, long *col_ptr, long *row_ind, double *values, long n_cols, long n_rows, long nnz)
{
	long i;

	for (i = 0; i < nnz; i++) {
		t = insertPairSumTree(t, row_ind[i], values[i]);
	}

	return t;
}

void getSumTreeEntries(Tree t, long *pos, SumPair** entries)
{
	if (t == NULL)
		return;

	getSumTreeEntries(getLeft(t), pos, entries);

	entries[(*pos)++] = (SumPair*) getEntry(t);

	getSumTreeEntries(getRight(t), pos, entries);
}

// needs to receive the total number of labels
void fromSumTreeToCSC(Tree t, long num_labels, long **col_ptr, long **row_ind, double **values, long *n_cols, long *n_rows, long *nnz)
{
	long i, size, pos;
	SumPair **entries;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	size = treeSize(t);
	pos = 0;

	out_values = (double*) _mm_malloc(size * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(size * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc(2 * sizeof(long), MEM_LINE_SIZE);

	entries = (SumPair**) malloc(size * sizeof(SumPair*));

	getSumTreeEntries(t, &pos, entries);

	for (i = 0; i < size; ++i)
	{
		out_row_ind[i] = entries[i]->key;
		out_values[i] = entries[i]->value;
	}

	out_col_ptr[0] = 0;
	out_col_ptr[1] = size;

	*values = out_values;
	*row_ind = out_row_ind;
	*col_ptr = out_col_ptr;
	*nnz = size;
	*n_rows = num_labels;
	*n_cols = 1;

	free(entries);
}


// AVG

Tree createAvgTree()
{
	return createTree();
}

Tree insertPairAvgTree(Tree t, long key, double value)
{
	int bf;
	AvgPair *aux;

	aux = (AvgPair*) malloc(sizeof(AvgPair));
	aux->key = key;
	aux = searchEntry(t, aux, compareByKey);

	if(!aux){
		aux = (AvgPair*) malloc(sizeof(AvgPair));
		aux->key = key;
		aux->value = value;
		aux->num = 1;
		t = insertTree(t, aux, &bf, compareByKey);
	}
	else{
		aux->value += value;
		aux->num++;
	}

	return t;
}

Tree fromCSCtoAvgTree(Tree t, long *col_ptr, long *row_ind, double *values, long n_cols, long n_rows, long nnz)
{
	long i;

	for(i = 0; i < nnz; i++) {
		t = insertPairAvgTree(t, row_ind[i], values[i]);
	}

	return t;
}

void getAvgTreeEntries(Tree t, long *pos, AvgPair** entries)
{
	if (t == NULL)
		return;

	getAvgTreeEntries(getLeft(t), pos, entries);

	entries[(*pos)++] = (AvgPair*) getEntry(t);

	getAvgTreeEntries(getRight(t), pos, entries);
}

// needs to receive the total number of labels
void fromAvgTreeToCSC(Tree t, long num_labels, long **col_ptr, long **row_ind, double **values, long *n_cols, long *n_rows, long *nnz)
{
	long i, size, pos;
	AvgPair **entries;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	size = treeSize(t);
	pos = 0;

	out_values = (double*) _mm_malloc(size * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(size * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc(2 * sizeof(long), MEM_LINE_SIZE);

	entries = (AvgPair**) malloc(size * sizeof(AvgPair*));

	getAvgTreeEntries(t, &pos, entries);

	for (i = 0; i < size; ++i)
	{
		out_row_ind[i] = entries[i]->key;
		out_values[i] = entries[i]->value / entries[i]->num;
	}

	out_col_ptr[0] = 0;
	out_col_ptr[1] = size;

	*values = out_values;
	*row_ind = out_row_ind;
	*col_ptr = out_col_ptr;
	*nnz = size;
	*n_rows = num_labels;
	*n_cols = 1;

	free(entries);
}


// EXISTS

Tree createExistsTree()
{
	return createTree();
}

Tree insertPairExistsTree(Tree t, long key)
{
	int bf;
	ExistsPair *aux;

	aux = (ExistsPair*) malloc(sizeof(ExistsPair));
	aux->key = key;
	aux = searchEntry(t, aux, compareByKey);

	if(!aux){
		aux = (ExistsPair*) malloc(sizeof(ExistsPair));
		aux->key = key;
		t = insertTree(t, aux, &bf, compareByKey);
	}

	return t;
}

Tree fromCSCtoExistsTree(Tree t, long *col_ptr, long *row_ind, double *values, long n_cols, long n_rows, long nnz)
{
	long i;

	for (i = 0; i < nnz; i++) {
		t = insertPairExistsTree(t, row_ind[i]);
	}

	return t;
}

void getExistsTreeEntries(Tree t, long *pos, ExistsPair** entries)
{
	if (t == NULL)
		return;

	getExistsTreeEntries(getLeft(t), pos, entries);

	entries[(*pos)++] = (ExistsPair*) getEntry(t);

	getExistsTreeEntries(getRight(t), pos, entries);
}

// needs to receive the total number of labels
void fromExistsTreeToCSC_Col(Tree t, long num_labels, long **col_ptr, long **row_ind, double **values, long *n_cols, long *n_rows, long *nnz)
{
	long i, size, pos;
	ExistsPair **entries;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	size = treeSize(t);
	pos = 0;

	out_values = (double*) _mm_malloc(size * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(size * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc(2 * sizeof(long), MEM_LINE_SIZE);

	entries = (ExistsPair**) malloc(size * sizeof(ExistsPair*));

	getExistsTreeEntries(t, &pos, entries);

	for (i = 0; i < size; ++i)
	{
		out_row_ind[i] = entries[i]->key;
		out_values[i] = 1;
	}

	out_col_ptr[0] = 0;
	out_col_ptr[1] = size;

	*values = out_values;
	*row_ind = out_row_ind;
	*col_ptr = out_col_ptr;
	*nnz = size;
	*n_rows = num_labels;
	*n_cols = 1;

	free(entries);
}

void fromExistsTreeToCSC_Row(Tree t, long num_labels, long **col_ptr, long **row_ind, double **values, long *n_cols, long *n_rows, long *nnz)
{
	long i, size, pos, aux;
	ExistsPair **entries;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	size = treeSize(t);
	pos = 0;

	out_values = (double*) _mm_malloc(size * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(size * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc((num_labels+1) * sizeof(long), MEM_LINE_SIZE);

	entries = (ExistsPair**) malloc(size * sizeof(ExistsPair*));

	getExistsTreeEntries(t, &pos, entries);

	for (i = 0, aux = 0; i < num_labels; ++i)
	{
		out_col_ptr[i] = aux;

		if ( i == entries[aux]->key ) {
			out_row_ind[aux] = 0;
			out_values[aux] = 1;
			aux++;
		}
	}

	out_col_ptr[i] = aux;

	*values = out_values;
	*row_ind = out_row_ind;
	*col_ptr = out_col_ptr;
	*nnz = aux;
	*n_rows = 1;
	*n_cols = num_labels;

	free(entries);
}

// COUNT

Tree createCountTree()
{
	return createTree();
}

Tree insertPairCountTree(Tree t, long key)
{
	int bf;
	CountPair *aux;

	aux = (CountPair*) malloc(sizeof(CountPair));
	aux->key = key;
	aux = searchEntry(t, aux, compareByKey);

	if(!aux) {
		aux = (CountPair*) malloc(sizeof(CountPair));
		aux->key = key;
		aux->value = 1;
		t = insertTree(t, aux, &bf, compareByKey);
	}
	else {
		aux->value++;
	}

	return t;
}

Tree fromCSCtoCountTree(Tree t, long *col_ptr, long *row_ind, double *values, long n_cols, long n_rows, long nnz)
{
	long i;

	for (i = 0; i < nnz; i++) {
		t = insertPairCountTree(t, row_ind[i]);
	}

	return t;
}

void getCountTreeEntries(Tree t, long *pos, CountPair **entries)
{
	if (t == NULL)
		return;

	getCountTreeEntries(getLeft(t), pos, entries);

	entries[(*pos)++] = (CountPair*) getEntry(t);

	getCountTreeEntries(getRight(t), pos, entries);
}

// needs to receive the total number of labels
void fromCountTreeToCSC(Tree t, long num_labels, long **col_ptr, long **row_ind, double **values, long *n_cols, long *n_rows, long *nnz)
{
	long i, size, pos;
	CountPair **entries;

	__declspec(align(MEM_LINE_SIZE)) double *out_values;
	__declspec(align(MEM_LINE_SIZE)) long *out_row_ind;
	__declspec(align(MEM_LINE_SIZE)) long *out_col_ptr;

	size = treeSize(t);
	pos = 0;

	out_values = (double*) _mm_malloc(size * sizeof(double), MEM_LINE_SIZE);
	out_row_ind = (long*) _mm_malloc(size * sizeof(long), MEM_LINE_SIZE);
	out_col_ptr = (long*) _mm_malloc(2 * sizeof(long), MEM_LINE_SIZE);

	entries = (CountPair**) malloc(size * sizeof(CountPair*));

	getCountTreeEntries(t, &pos, entries);

	for (i = 0; i < size; ++i)
	{
		out_row_ind[i] = entries[i]->key;
		out_values[i] = entries[i]->value;
	}

	out_col_ptr[0] = 0;
	out_col_ptr[1] = size;

	*values = out_values;
	*row_ind = out_row_ind;
	*col_ptr = out_col_ptr;
	*nnz = size;
	*n_rows = num_labels;
	*n_cols = 1;

	free(entries);
}
