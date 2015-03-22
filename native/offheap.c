/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <jni.h>

//#define DEBUG 1

struct nodeValue{
  jsize valueLen;
  jbyte *value;
};

struct node{
  struct node *left;
  struct node *right;
  struct nodeValue *value;
};

struct trie{
    struct node *root;
};

inline int GETBIT(unsigned int addr, int depth){
    return (addr >> (31-depth)) & 0x1;
}

#define GETBIT(addr, depth) ((addr >> (31-depth)) & 0x1)


void
insert_recursive(struct node * n, unsigned int addr, int mask, int depth, struct nodeValue *value){
    if(mask == depth){
      if(n->value != NULL){
	// NOTE: invariant, node and value allocated in one go
	free(n->value);
      }
      n->value = value;
      return;
    }
    int direction = GETBIT(addr, depth);

    struct node **next;

    if(direction == 0){
        next = &(n->left);
    }
    else{
        next = &(n->right);
    }
#ifdef DEBUG
    printf("inserting [%d] at depth [%d]\n", direction, depth);
#endif

    if((*next) == NULL){
        *next = (struct node *) malloc(sizeof(struct node));
        (*next)->left = NULL;
        (*next)->right = NULL;
        (*next)->value = NULL;
    }
    insert_recursive((*next), addr, mask, depth+1, value);
}

void
insert(struct trie *t, unsigned int addr, int mask, struct nodeValue *value){
    if(t->root == NULL){
        t->root = (struct node *)malloc(sizeof(struct node));
        t->root->left = NULL;
        t->root->right = NULL;
        t->root->value = NULL;
    }
    insert_recursive(t->root, addr, mask, 0, value);
}


struct nodeValue *
lookup_recursive(struct node * n, struct nodeValue *last, unsigned int addr, int depth){

    int direction = GETBIT(addr, depth);

    struct node *next;

    if(direction == 0){
        next = n->left;
    }
    else{
        next = n->right;
    }
#ifdef DEBUG
    printf("looking [%d] at depth [%d]\n", direction, depth);
#endif
    if(n->value != NULL){
      last = n->value;
    }

    if(next == NULL){
        return last;
    }
    return lookup_recursive(next, last, addr, depth+1);
}

struct nodeValue *
lookup(struct trie *t, unsigned int addr){
    if(t->root == NULL){
       return NULL;
    }
    return lookup_recursive(t->root, NULL, addr, 0);
}

JNIEXPORT jlong JNICALL Java_org_apache_nifi_util_lookup_OffHeapLookup_getTrie(JNIEnv *env, jclass cls){
    struct trie * t = (struct trie *)malloc(sizeof(struct trie));
    if(t == NULL){
        // throw exception?
        return 0;
    }
    t->root = NULL;
    return (jlong)t;
}

JNIEXPORT void JNICALL Java_org_apache_nifi_util_lookup_OffHeapLookup_trieInsert(JNIEnv *env, jclass cls, jlong pointer, jint address, jint mask, jbyteArray bytes){

    struct trie *t = (struct trie *)pointer;

    // TODO: check errors
    jbyte* bufferPtr = (*env)->GetByteArrayElements(env, bytes, NULL);
    jsize lengthOfArray = (*env)->GetArrayLength(env, bytes);

    struct nodeValue * nv = (struct nodeValue *)malloc(lengthOfArray + sizeof(struct nodeValue) + 1);
    nv->valueLen = lengthOfArray;
    nv->value = (jbyte *)nv + sizeof(struct nodeValue);

    // TODO: ADD null check
    memcpy(nv->value, bufferPtr, lengthOfArray);

    (*env)->ReleaseByteArrayElements(env, bytes, bufferPtr, 0);
    
    insert(t, address, mask, nv);
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_nifi_util_lookup_OffHeapLookup_trieLookup(JNIEnv *env, jclass cls, jlong pointer, jint address){
  struct trie *t = (struct trie *)pointer;
  struct nodeValue *nv = lookup(t, address);

  if(nv == NULL){
    return NULL;
  }

  jbyteArray arr = (*env)->NewByteArray(env, nv->valueLen);
  // TODO: ADD null check

  (*env)->SetByteArrayRegion(env, arr, 0, nv->valueLen, (jbyte*)nv->value);
  return arr;

}


