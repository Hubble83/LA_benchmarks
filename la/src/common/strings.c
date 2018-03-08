#include "strings.h"

char *strdup(const char *str) {
    char *new_str;
    
    new_str = (char*) malloc((strlen(str) + 1) * sizeof(char));
    if (new_str != NULL)
        strcpy(new_str, str);

    return new_str;
}
