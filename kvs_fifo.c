#include "kvs_fifo.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Pair Tooltip
struct pair {
  char* key;
  char* value;
  int type;  // 0 is GET, 1 is SET
} pair;

typedef struct pair kvs_pair;

kvs_pair* kvs_pair_new(const char* key, const char* value, const int type) {
  kvs_pair* out = malloc(sizeof(kvs_pair));
  if (out) {
    out->key = malloc(strlen(key) + 1);
    out->value = malloc(strlen(value) + 1);
    out->type = type;
    strcpy(out->key, key);
    strcpy(out->value, value);
  }
  return out;
}

void kvs_pair_free(kvs_pair** ps) {
  if (*ps) {
    free((*ps)->key);
    free((*ps)->value);
    free(*ps);
    *ps = NULL;
  }
}

///////////////////////////////////////////////////////

struct kvs_fifo {
  // TODO: add necessary variables
  kvs_base_t* kvs_base;
  int capacity;
  int cursorcap;
  int cursor;
  kvs_pair** list;
};

// Debug tool
void kvs_fifo_print(kvs_fifo_t* kvs_fifo) {
  for (int i = 0; i < kvs_fifo->capacity; i++) {
    if (kvs_fifo->list[i]) {
      printf("%d: %s %s %d\n", i, kvs_fifo->list[i]->key,
             kvs_fifo->list[i]->value, kvs_fifo->list[i]->type);
    }
  }
  printf("\n");
}

kvs_fifo_t* kvs_fifo_new(kvs_base_t* kvs, int capacity) {
  kvs_fifo_t* kvs_fifo = malloc(sizeof(kvs_fifo_t));
  kvs_fifo->kvs_base = kvs;
  kvs_fifo->capacity = capacity;
  kvs_fifo->cursorcap = 0;
  kvs_fifo->cursor = 0;
  kvs_fifo->list = calloc(capacity, sizeof(kvs_pair));
  // TODO: initialize other variables

  return kvs_fifo;
}

void kvs_fifo_free(kvs_fifo_t** ptr) {
  // TODO: free dynamically allocated memory
  for (int i = 0; i < (*ptr)->capacity; i++) {
    if ((*ptr)->list[i]) kvs_pair_free(&((*ptr)->list[i]));
  }
  free((*ptr)->list);
  free(*ptr);
  *ptr = NULL;
}

int kvs_fifo_set(kvs_fifo_t* kvs_fifo, const char* key, const char* value) {
  // make sure to check if the key exists in the lists first before doing
  for (int i = 0; i < kvs_fifo->capacity; i++) {
    if (kvs_fifo->list[i] && strcmp(key, kvs_fifo->list[i]->key) == 0) {
      free(kvs_fifo->list[i]->value);
      kvs_fifo->list[i]->value = malloc(strlen(value) + 1);
      strcpy(kvs_fifo->list[i]->value, value);
      kvs_fifo->list[i]->type =
          1;  // overwrite the GET to SET (experimental, might change)
      // kvs_fifo_print(kvs_fifo);
      return 0;
    }
  }
  if (kvs_fifo->capacity == 0 &&
      kvs_base_set(kvs_fifo->kvs_base, key, value) != 0) {
    return FAILURE;
  } else if (kvs_fifo->capacity > 0 &&
             kvs_fifo->cursorcap < kvs_fifo->capacity) {
    kvs_fifo->list[kvs_fifo->cursorcap++] =
        kvs_pair_new(key, value, 1);  // set to 1 cause it's a SET operation
    // kvs_fifo_print(kvs_fifo);
  } else if (kvs_fifo->capacity > 0) {
    if (kvs_fifo->list[kvs_fifo->cursor]->type &&
        kvs_base_set(kvs_fifo->kvs_base, kvs_fifo->list[kvs_fifo->cursor]->key,
                     kvs_fifo->list[kvs_fifo->cursor]->value) != 0) {
      return FAILURE;
    }
    kvs_pair_free(&(kvs_fifo->list[kvs_fifo->cursor]));
    kvs_fifo->list[kvs_fifo->cursor] =
        kvs_pair_new(key, value, 1);  // set to 1 cause it's a SET operation
    kvs_fifo->cursor = (kvs_fifo->cursor + 1) % (kvs_fifo->capacity);
    // kvs_fifo_print(kvs_fifo);
  }

  return 0;
}

int kvs_fifo_get(kvs_fifo_t* kvs_fifo, const char* key, char* value) {
  // TODO: implement this function
  // If the key is in memory, it should write the value from memory to the given
  // pointer.
  for (int i = 0; i < kvs_fifo->capacity; i++) {
    if (kvs_fifo->list[i] && strcmp(key, kvs_fifo->list[i]->key) == 0) {
      strcpy(value, kvs_fifo->list[i]->value);
      // kvs_fifo_print(kvs_fifo);
      return 0;
    }
  }
  // If the key is not in memory, it calls kvs_base_get to read the value from
  // the disk. In addition to writing the value to the given pointer, it should
  // cache the key-value pair (employing the specified replacement strategy if
  // need be).
  if (kvs_base_get(kvs_fifo->kvs_base, key, value) != 0) return FAILURE;
  if (kvs_fifo->capacity > 0 && kvs_fifo->cursorcap < kvs_fifo->capacity) {
    kvs_fifo->list[kvs_fifo->cursorcap++] = kvs_pair_new(key, value, 0);
    // kvs_fifo_print(kvs_fifo);
  } else if (kvs_fifo->capacity > 0) {
    // might change this later
    if (kvs_fifo->list[kvs_fifo->cursor]->type &&
        kvs_base_set(kvs_fifo->kvs_base, kvs_fifo->list[kvs_fifo->cursor]->key,
                     kvs_fifo->list[kvs_fifo->cursor]->value) != 0) {
      return FAILURE;
    }
    kvs_pair_free(&(kvs_fifo->list[kvs_fifo->cursor]));
    kvs_fifo->list[kvs_fifo->cursor] = kvs_pair_new(key, value, 0);
    kvs_fifo->cursor = (kvs_fifo->cursor + 1) % (kvs_fifo->capacity);
    // kvs_fifo_print(kvs_fifo);
  }
  return 0;
}

int kvs_fifo_flush(kvs_fifo_t* kvs_fifo) {
  // TODO: implement this function
  for (int i = 0; i < kvs_fifo->capacity; i++) {
    if (kvs_fifo->list[i] && kvs_fifo->list[i]->type) {
      if (kvs_base_set(kvs_fifo->kvs_base, kvs_fifo->list[i]->key,
                       kvs_fifo->list[i]->value) != 0) {
        return FAILURE;
      }
    }
  }
  return 0;
}
