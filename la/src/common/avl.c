#include "avl.h"

typedef struct treenode{
  BalanceFactor bf;
  void* entry;
  struct treenode *left;
  struct treenode *right;
} TreeNode;

struct linear{
  void** array;
  int contador; 
};

Tree createTree(void){
  return NULL;
}

int compareStrings(void *s1 , void *s2){
  int i=0;
  while(((char*)s1)[i] != '\0' && ((char*)s2)[i] != '\0' && ((char*)s1)[i]==((char*)s2)[i]){
	i++;
  }
  if (((char*)s1)[i]>((char*)s2)[i]) return 1;
  else{
	if (((char*)s1)[i]<((char*)s2)[i]) return -1;
	else return 0;
  }
}

void emptyTree(Tree t){
  if(t==NULL) return;
  emptyTree(t->left);
  emptyTree(t->right);
  free(t->entry);
  t->entry = NULL;
  free(t);
  t = NULL;
}

int treeSize(Tree t){
  if(t==NULL)return 0;
  return(treeSize(t->left)+treeSize(t->right)+1);
}

int treeHeight(Tree t){
  if (!t) return 0;
  else{
	if(t->bf==LH) return 1 + treeHeight(t->left);
	else return 1 + treeHeight(t->right);
  } 
}

int isEmpty(Tree t){
  if (t==NULL) return 1;
  else return 0;
}

int searchKey(Tree t, void* e, int (*f)(void* , void*)){
  if(t==NULL) return 0;
  if(f(t->entry,e)==0) return 1;
  else if(f(t->entry,e)==1) return(searchKey(t->left, e, f));
  else return(searchKey(t->right, e, f));
}

void* searchEntry(Tree t, void* e, int (*f)(void* , void*)){
  if(t==NULL) return NULL;
  if(f(t->entry,e)==0) return t->entry;
  else if(f(t->entry,e)==1) return(searchEntry(t->left, e, f));
  else return(searchEntry(t->right, e, f));
}

Tree rotateRight(Tree t){
  Tree aux;
  aux = createTree();
  if ((! t)||(! t->left)){
  }
  else{
	aux=t->left;
	t->left=aux->right;
	aux->right=t;
	t=aux;
  }
  return t;
}

Tree rotateLeft(Tree t){
  Tree aux;
  aux = createTree();
  if((! t)||(! t->right)){
  }
  else{
	aux=t->right;
	t->right=aux->left;
	aux->left=t;
	t=aux;
  }
  return t;
}

Tree balanceRight(Tree t){
  if(t->right->bf==RH){
	t=rotateLeft(t);
	t->bf=EH;
	t->left->bf=EH;
  }
  else{
	t->right=rotateRight(t->right);
	t=rotateLeft(t);
	switch (t->bf) {
	case EH:
	  t->left->bf=EH;
	  t->right->bf=EH;
	  break;
	case LH:
	  t->left->bf=EH;
	  t->right->bf=RH;
	  break;
	case RH:
	  t->left->bf=LH;
	  t->right->bf=EH;
	}
	t->bf=EH;
  }
  return t;
}

Tree balanceLeft(Tree t){
  if (t->left->bf==LH){
	t=rotateRight(t);
	t->bf=EH;
	t->right->bf=EH;
  }
  else{
	t->left=rotateLeft(t->left);
	t=rotateRight(t);
	switch (t->bf) {
	case EH:
	  t->left->bf=EH;
	  t->right->bf=EH;
	  break;
	case RH:
	  t->left->bf=LH;
	  t->right->bf=EH;
	  break;
	case LH:
	  t->left->bf=EH;
	  t->right->bf=RH;
	}
	t->bf=EH;
  }
  return t;
}

Tree insertTree(Tree t, void* e, int *cresceu, int (*f)(void* , void*));

Tree insertRight(Tree t, void* e, int *cresceu, int (*f)(void* , void*)){
  t->right=insertTree(t->right, e, cresceu, f);
  if(*cresceu){
	switch (t->bf) {
	case LH:
	  t->bf=EH;
	  *cresceu=0;
	  break;
	case EH:
	  t->bf=RH;
	  *cresceu=1;
	  break;
	case RH:
	  t=balanceRight(t);
	  *cresceu=0;
	}
  }
  return t;
}

Tree insertLeft(Tree t, void* e, int *cresceu, int (*f)(void* , void*)){
  t->left=insertTree(t->left, e, cresceu, f);
  if (*cresceu) {
	switch (t->bf) {
	case RH:
	  t->bf=EH;
	  *cresceu=0;
	  break;
	case EH:
	  t->bf=LH;
	  *cresceu=1;
	  break;
	case LH:
	  t=balanceLeft(t);
	  *cresceu=0;
	}
  }
  return t;
}

Tree insertTree(Tree t, void* e, int *cresceu, int (*f)(void* , void*)){
  if(t==NULL){
	t=(Tree)malloc(sizeof(struct treenode));
	t->entry=e;
	t->right=NULL;
	t->left=NULL;
	t->bf=EH;
	*cresceu=1;
  }
  else if(f(e,t->entry)==1){
	t=insertRight(t,e,cresceu, f);
  }
  else{
	t=insertLeft(t,e,cresceu, f);
  }
  return(t);
}

OutputLinear initOutput(){
  OutputLinear out;
  out = (OutputLinear)malloc(sizeof(struct linear));
  out->array = NULL;
  out->contador = 0;
  return out;
}

OutputLinear linearizaaux(Tree t, OutputLinear b){
  if(t){
	linearizaaux(t->left, b);
	linearizaaux(t->right, b);
		b->array = (void**)realloc(b->array, (b->contador+1)*(sizeof(void*)));
		b->array[b->contador] = t->entry;
		b->contador++;  
  }
  return b;
}

OutputLinear lineariza(Tree t, OutputLinear out){
  if(!out->array){
	out->array = malloc(sizeof(void*)*10);
	out = linearizaaux(t, out);
  }
  else{
	out = linearizaaux(t, out);
  }
  return out;
}

void** getArray(OutputLinear b){
  return b->array;
}

int getContador(OutputLinear b){
  int res;
  res = b->contador;
  return res;
}

void *getEntry(Tree t) {
  return t->entry;
}

Tree getLeft(Tree t) {
	return t->left;
}

Tree getRight(Tree t) {
	return t->right;
}