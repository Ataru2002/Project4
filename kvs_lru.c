#include "kvs_lru.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Linked List Tools
struct node {
  struct node* prev;
  char* key;
  char* value;
  int type;  // 0 is GET, 1 is SET
  struct node* next;
} node;

typedef struct node kvs_node;

struct list {
  kvs_node* front;
  kvs_node* back;
  int length;
} list;

typedef struct list kvs_list;

kvs_node* new_node(const char* key, const char* value, const int type) {
  kvs_node* out = malloc(sizeof(kvs_node));
  if (out) {
    out->prev = out->next = NULL;
    out->key = malloc(strlen(key) + 1);
    out->value = malloc(strlen(value) + 1);
    out->type = type;
    strcpy(out->key, key);
    strcpy(out->value, value);
  }
  return out;
}

void free_node(kvs_node** ps) {
  if (*ps) {
    free((*ps)->key);
    free((*ps)->value);
    free(*ps);
    *ps = NULL;
  }
}

kvs_list* new_list(void) {
  kvs_list* l = malloc(sizeof(kvs_list));
  if (l) {
    l->front = l->back = NULL;
    l->length = 0;
  }
  return l;
}

void deleteBack(kvs_list* list) {
  if (list == NULL || list->length <= 0) return;
  kvs_node* del = list->back;
  if (list->length == 1)
    list->front = list->back = NULL;
  else {
    list->back->prev->next = NULL;
    list->back = list->back->prev;
  }
  free_node(&del);
  list->length--;
}

void free_list(kvs_list** list) {
  if (*list && list) {
    int oriLength = (*list)->length;
    for (int i = 0; i < oriLength; i++) {
      deleteBack(*list);
    }
    free(*list);
    *list = NULL;
  }
}

void prepend(kvs_list* list, kvs_node* in) {
  if (list == NULL || in == NULL) return;
  if (list->length == 0) {
    list->front = list->back = in;
    list->length = 1;
    return;
  }
  in->next = list->front;
  list->front->prev = in;
  list->front = in;
  list->length++;
}

void priority(kvs_list* list, const char* target) {
  kvs_node* start = list->front;
  for (int i = 0; i < list->length; i++) {
    if (strcmp(target, start->key) == 0) break;
    start = start->next;
  }
  // If the target is at the end of the list

  if (list->length > 1 && start == list->back) {
    start->prev->next = NULL;
    list->back = start->prev;
    start->prev = NULL;
    list->front->prev = start;
    start->next = list->front;
    list->front = start;
  } else if (list->length > 1 && start != list->front) {
    // when the target is in the middle
    start->prev->next = start->next;
    start->next->prev = start->prev;
    start->prev = NULL;
    start->next = list->front;
    list->front->prev = start;
    list->front = start;
  }
}

void print_list(kvs_list* list) {
  kvs_node* start = list->front;
  printf("list length: %d\n", list->length);
  for (int i = 0; i < list->length; i++) {
    printf("%d: %s data: %s, Type: %d\n", i, start->key, start->value,
           start->type);
    start = start->next;
  }
  printf("\n");
}

//////////////////////////////////////////////////////////////////

struct kvs_lru {
  // TODO: add necessary variables
  kvs_base_t* kvs_base;
  int capacity;
  kvs_list* list;
};

kvs_lru_t* kvs_lru_new(kvs_base_t* kvs, int capacity) {
  kvs_lru_t* kvs_lru = malloc(sizeof(kvs_lru_t));
  kvs_lru->kvs_base = kvs;
  kvs_lru->capacity = capacity;
  kvs_lru->list = new_list();
  // TODO: initialize other variables
  return kvs_lru;
}

void kvs_lru_free(kvs_lru_t** ptr) {
  // TODO: free dynamically allocated memory
  free_list(&((*ptr)->list));
  free(*ptr);
  *ptr = NULL;
}

int kvs_lru_set(kvs_lru_t* kvs_lru, const char* key, const char* value) {
  // TODO: implement this function
  // check if there's an entry in the cache
  kvs_node* start = kvs_lru->list->front;
  for (int i = 0; i < kvs_lru->list->length; i++) {
    if (strcmp(key, start->key) == 0) {
      free(start->value);
      start->value = malloc(strlen(value) + 1);
      strcpy(start->value, value);
      start->type = 1;  // change the GET to the SET
      priority(kvs_lru->list, start->key);
      print_list(kvs_lru->list);
      return 0;
    }
    start = start->next;
  }
  // If the key is not in memory, it calls kvs_base_get to read the value from
  // the disk. In addition to writing the value to the given pointer, it should
  // cache the key-value pair (employing the specified replacement strategy if
  // need be).
  if (kvs_lru->capacity == 0 &&
      kvs_base_set(kvs_lru->kvs_base, key, value) != 0) {
    return FAILURE;
  } else if (kvs_lru->capacity > 0 &&
             kvs_lru->list->length < kvs_lru->capacity) {
    // not fully filled yet
    prepend(kvs_lru->list, new_node(key, value, 1));
    print_list(kvs_lru->list);
  } else if (kvs_lru->capacity > 0) {
    // need to evict something
    kvs_node* evict = kvs_lru->list->back;
    if (evict->type &&
        kvs_base_set(kvs_lru->kvs_base, evict->key, evict->value) != 0) {
      return FAILURE;
    }
    deleteBack(kvs_lru->list);
    prepend(kvs_lru->list, new_node(key, value, 1));
    print_list(kvs_lru->list);
  }
  return 0;
}

int kvs_lru_get(kvs_lru_t* kvs_lru, const char* key, char* value) {
  // TODO: implement this function
  // When the entry is found in the cache
  kvs_node* start = kvs_lru->list->front;
  for (int i = 0; i < kvs_lru->list->length; i++) {
    printf("debug keys: %s %s\n", key, start->key);
    if (strcmp(key, start->key) == 0) {
      strcpy(value, start->value);
      priority(kvs_lru->list, start->key);
      print_list(kvs_lru->list);
      return 0;
    }
    start = start->next;
  }
  if (kvs_base_get(kvs_lru->kvs_base, key, value) != 0) return FAILURE;
  if (kvs_lru->capacity > 0 && kvs_lru->list->length < kvs_lru->capacity) {
    // not fully filled yet
    prepend(kvs_lru->list, new_node(key, value, 0));
    print_list(kvs_lru->list);
  } else if (kvs_lru->capacity > 0) {
    // need to evict something
    kvs_node* evict = kvs_lru->list->back;
    if (evict->type &&
        kvs_base_set(kvs_lru->kvs_base, evict->key, evict->value) != 0) {
      return FAILURE;
    }
    deleteBack(kvs_lru->list);
    prepend(kvs_lru->list, new_node(key, value, 0));
    print_list(kvs_lru->list);
  }
  return 0;
}

int kvs_lru_flush(kvs_lru_t* kvs_lru) {
  // TODO: implement this function
  kvs_node* start = kvs_lru->list->front;
  for (int i = 0; i < kvs_lru->list->length; i++) {
    if (start->type &&
        kvs_base_set(kvs_lru->kvs_base, start->key, start->value) != 0) {
      return FAILURE;
    }
    start = start->next;
  }
  return 0;
}
