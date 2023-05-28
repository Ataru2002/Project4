#include "kvs_clock.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Pair Tooltip
struct pairc {
  char* key;
  char* value;
  int type;  // 0 for GET, and 1 for SET
  int reference;
} pairc;

typedef struct pairc kvs_pair;

kvs_pair* kvs_pair_newc(const char* key, const char* value, const int type,
                        const int reference) {
  kvs_pair* out = malloc(sizeof(kvs_pair));
  if (out) {
    out->key = malloc(strlen(key) + 1);
    out->value = malloc(strlen(value) + 1);
    out->type = type;
    out->reference = reference;
    strcpy(out->key, key);
    strcpy(out->value, value);
  }
  return out;
}

void kvs_pair_freec(kvs_pair** ps) {
  if (*ps) {
    free((*ps)->key);
    free((*ps)->value);
    free(*ps);
    *ps = NULL;
  }
}

struct kvs_clock {
  // TODO: add necessary variables
  kvs_base_t* kvs_base;
  int capacity;
  int cursorcap;
  int cursor;
  kvs_pair** list;
};

// Debug tool
void kvs_clock_print(kvs_clock_t* kvs_clock) {
  for (int i = 0; i < kvs_clock->capacity; i++) {
    if (kvs_clock->list[i]) {
      printf("%d: %s %s, type: %d, reference: %d\n", i, kvs_clock->list[i]->key,
             kvs_clock->list[i]->value, kvs_clock->list[i]->type,
             kvs_clock->list[i]->reference);
    }
  }
  printf("\n");
}

kvs_clock_t* kvs_clock_new(kvs_base_t* kvs, int capacity) {
  kvs_clock_t* kvs_clock = malloc(sizeof(kvs_clock_t));
  kvs_clock->kvs_base = kvs;
  kvs_clock->capacity = capacity;
  kvs_clock->cursorcap = 0;
  kvs_clock->cursor = 0;
  kvs_clock->list = calloc(capacity, sizeof(kvs_pair));

  // TODO: initialize other variables

  return kvs_clock;
}

void kvs_clock_free(kvs_clock_t** ptr) {
  // TODO: free dynamically allocated memory
  for (int i = 0; i < (*ptr)->capacity; i++) {
    if ((*ptr)->list[i]) kvs_pair_freec(&((*ptr)->list[i]));
  }
  free((*ptr)->list);
  free(*ptr);
  *ptr = NULL;
}

int kvs_clock_set(kvs_clock_t* kvs_clock, const char* key, const char* value) {
  // TODO: implement this function
  // If found, update value and also set the reference to 1
  for (int i = 0; i < kvs_clock->capacity; i++) {
    if (kvs_clock->list[i] && strcmp(key, kvs_clock->list[i]->key) == 0) {
      free(kvs_clock->list[i]->value);
      kvs_clock->list[i]->value = malloc(strlen(value) + 1);
      strcpy(kvs_clock->list[i]->value, value);
      kvs_clock->list[i]->reference = 1;
      kvs_clock->list[i]->type = 1;  // Change from GET to SET
      // kvs_clock_print(kvs_clock);
      kvs_clock_print(kvs_clock);
      return 0;
    }
  }
  // If not found
  // If we didn't fill up the buffer yet
  if (kvs_clock->cursorcap < kvs_clock->capacity) {
    kvs_clock->list[kvs_clock->cursorcap++] = kvs_pair_newc(key, value, 1, 1);
    kvs_clock_print(kvs_clock);
  } else {
    // We need to evict here
    while (kvs_clock->list[kvs_clock->cursor] &&
           kvs_clock->list[kvs_clock->cursor]->reference) {
      kvs_clock->list[kvs_clock->cursor]->reference = 0;
      kvs_clock->cursor = (kvs_clock->cursor + 1) % (kvs_clock->capacity);
    }
    if (kvs_clock->list[kvs_clock->cursor]->type &&
        kvs_base_set(kvs_clock->kvs_base,
                     kvs_clock->list[kvs_clock->cursor]->key,
                     kvs_clock->list[kvs_clock->cursor]->value) != 0) {
      return FAILURE;
    }
    kvs_pair_freec(&(kvs_clock->list[kvs_clock->cursor]));
    kvs_clock->list[kvs_clock->cursor] = kvs_pair_newc(key, value, 1, 1);
    kvs_clock_print(kvs_clock);
  }

  return 0;
}

int kvs_clock_get(kvs_clock_t* kvs_clock, const char* key, char* value) {
  // TODO: implement this function
  // If the key is in memory, it should write the value from memory to the given
  // pointer.
  for (int i = 0; i < kvs_clock->capacity; i++) {
    if (kvs_clock->list[i] && strcmp(key, kvs_clock->list[i]->key) == 0) {
      strcpy(value, kvs_clock->list[i]->value);
      kvs_clock_print(kvs_clock);
      return 0;
    }
  }
  if (kvs_base_get(kvs_clock->kvs_base, key, value) != 0) return FAILURE;
  // When there's no value in the queue, we add it to the queue
  if (kvs_clock->cursorcap < kvs_clock->capacity) {
    kvs_clock->list[kvs_clock->cursorcap++] = kvs_pair_newc(key, value, 0, 1);
    kvs_clock_print(kvs_clock);
  } else {
    // We need to evict something
    // Cyclic rotation
    while (kvs_clock->list[kvs_clock->cursor] &&
           kvs_clock->list[kvs_clock->cursor]->reference) {
      kvs_clock->list[kvs_clock->cursor]->reference = 0;
      kvs_clock->cursor = (kvs_clock->cursor + 1) % (kvs_clock->capacity);
    }
    // save the changes on disk if the evicted operation is SET
    if (kvs_clock->list[kvs_clock->cursor]->type &&
        kvs_base_set(kvs_clock->kvs_base,
                     kvs_clock->list[kvs_clock->cursor]->key,
                     kvs_clock->list[kvs_clock->cursor]->value) != 0) {
      return FAILURE;
    }
    // delete the old one and insert a new one in
    kvs_pair_freec(&(kvs_clock->list[kvs_clock->cursor]));
    kvs_clock->list[kvs_clock->cursor] = kvs_pair_newc(key, value, 0, 1);
    kvs_clock_print(kvs_clock);
  }
  return 0;
}

int kvs_clock_flush(kvs_clock_t* kvs_clock) {
  // TODO: implement this function
  for (int i = 0; i < kvs_clock->capacity; i++) {
    if (kvs_clock->list[i] && kvs_clock->list[i]->type) {
      if (kvs_base_set(kvs_clock->kvs_base, kvs_clock->list[i]->key,
                       kvs_clock->list[i]->value) != 0) {
        return FAILURE;
      }
    }
  }
  return 0;
}
