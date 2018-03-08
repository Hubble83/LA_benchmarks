#ifndef AVL_H
#define AVL_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum balancefactor { LH , EH , RH } BalanceFactor;

typedef struct treenode *Tree;
typedef struct linear *OutputLinear;

/** Cria uma Tree 
@return Tree
*/
Tree createTree(void);

/** Esvazia uma Tree 
@param Tree
*/
void emptyTree(Tree t);

/** Calcula a altura de uma Tree
@param Tree
@return Altura
*/
int treeHeight(Tree t);

/** Calcula o tamanho (número de nodos) de uma Tree
@param Tree
@return Tamanho
*/
int treeSize(Tree t);

/** Verifica se uma Tree é vazia
@param Tree
@return 1 se está vazia, 0 caso contrário
*/
int isEmpty(Tree t);

/** Compara por ordem lexicográfica duas strings, recebendo os apontadores para elas
@param s1 String 1
@param s2 String 2 
@return 1 se s1 é maior que s2, 0 caso sejam iguais, -1 caso s1 é menor que s2
*/
int compareStrings(void *s1 , void *s2);

/** Dado uma Tree, um endereço de um data e uma função que compara, diz se existe um data igual na Tree
@param t Tree
@param e Apontador para data
@param f Função que compara
@return 1 se Tree contem um data igual, 0 caso contrário
*/
int searchKey(Tree t, void* e, int (*f)(void* , void*));

/** Dado uma Tree, um endereço de um data e uma função que compara, devolve o apontador para o data igual ao recebido
@param t Tree
@param e Apontador para data
@param f Função que compara
@return Apontador para o data igual ao recebido
*/
void* searchEntry(Tree t, void* e, int (*f)(void* , void*));

/** Dado uma Tree, um endereço de um data, um enderenço de um inteiro (necessario apenas para manter a correta inserção), e uma função uma Tree com o data recebido inserido
@param t Tree
@param e Apontador para data
@param cresceu Inteiro para gerir a inserção (pode vir com qualquer valor)
@param f Função que compara
@return Apontador para o data igual ao recebido
*/
Tree insertTree(Tree t, void* e, int *cresceu, int (*f)(void* , void*));


/** Dado uma Tree, e um OutputLinear, devolve um OutputLinear que corresponde à Tree recebida mas linearizada (ou seja, apenas é uma representação da Tree mas que dá para ser percorrida linearmente, qualquer mudança feita irá afetar a Tree)
@param t Tree
@param out OutputLinear
@return OutputLinear
*/
OutputLinear lineariza(Tree t, OutputLinear out);

/** Devolve um OutputLinear inicializado
@return OutputLinear
*/
OutputLinear initOutput();

/** Recebendo um OutputLinear, devolve um array de endereços de dados (que vieram de uma Tree)
@param out OutputLinear
@return Array de endereços de dados
*/
void** getArray(OutputLinear b);
/** Recebendo um OutputLinear, devolve quantos dados o OutputLinear tem
@param out OutputLinear
@return Total de dados
*/
int getContador(OutputLinear b);


void *getEntry(Tree t);

Tree getLeft(Tree t);

Tree getRight(Tree t);


#endif