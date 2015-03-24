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

/**
 * @brief Generic length/value tuple
 *
 */
struct nodeValue{
  jsize valueLen;
  jbyte *value;
};

/**
 * @brief Trie node 
 *
 */
struct node{
  struct node *left;
  struct node *right;
  struct nodeValue *value;
};

/**
 * @brief Trie base struct 
 *
 */
struct trie{
    struct node *root;
};

/**
 * @brief Used for retrieving the appropriate bit for a trie depth from a 32 bit unsigned in 
 */
static inline int GETBIT(unsigned int addr, int depth){
    return (addr >> (31-depth)) & 0x1;
}

//#define GETBIT(addr, depth) ((addr >> (31-depth)) & 0x1)


/**
 * @brief Utility function for throwing an OutOfMemory error
 * 
 * @note Invariant: values passed in must be valid, no error checking
 * @return Will return -1 if class not found, value returned by JNI funtion ThrowNew otherwise
 */
static jint 
throwOutOfMemoryError( JNIEnv *env, char *message ){
    jclass jc;
    jc = (*env)->FindClass( env, "java/lang/OutOfMemoryError" );
    if(jc == NULL) {
      // NOTE: Shouldn't happen. Should get NoClassDefFound 
      return -1;
    }
    return (*env)->ThrowNew(env, jc, message );
}

/**
 * @brief Utility function for throwing an Error
 * 
 * @note Invariant: values passed in must be valid, no error checking
 * @return Will return -1 if class not found, value returned by JNI funtion ThrowNew otherwise
 */
static jint 
throwError(JNIEnv *env, char *message){
  jclass jc;
  jc = (*env)->FindClass( env, "java/lang/Error" );
  if(jc == NULL) {
    // NOTE: Shouldn't happen. Should get NoClassDefFound 
    return -1;
  }
  return (*env)->ThrowNew(env, jc, message );
}


/**
 * @brief Recursive function for adding value to trie
 * 
 * @note Invariant: values passed in must be valid, no error checking
 * @throws OutOfMemoryError if memory cannot be allocated while inserting
 */
static void
insert_recursive(JNIEnv *env, struct node * n, unsigned int addr, int mask, int depth, struct nodeValue *value){
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
    if(*next == NULL){
      throwOutOfMemoryError(env, "Unable to allocate offheap storage for trie");
    }
    (*next)->left = NULL;
    (*next)->right = NULL;
    (*next)->value = NULL;
  }
  insert_recursive(env, (*next), addr, mask, depth+1, value);
}

/**
 * @brief Function for adding value to trie
 * 
 * @note Invariant: values passed in must be valid, no error checking
 * @throws OutOfMemoryError if memory cannot be allocated while inserting
 */
static void
insert(JNIEnv *env, struct trie *t, unsigned int addr, int mask, struct nodeValue *value){
  if(t->root == NULL){
    t->root = (struct node *)malloc(sizeof(struct node));
    if(t->root == NULL){
      throwOutOfMemoryError(env, "Unable to allocate offheap storage for trie");
    }

    t->root->left = NULL;
    t->root->right = NULL;
    t->root->value = NULL;
  }
  insert_recursive(env, t->root, addr, mask, 0, value);
}

/**
 * @brief Recursive function for looking up value in trie
 * 
 * @note Invariant: values passed in must be valid, no error checking
 * @return nodeValue at longest prefix match in trie, NULL if not found
 */
static struct nodeValue *
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

/**
 * @brief Function for looking up value in trie
 * 
 * @note Invariant: values passed in must be valid, no error checking
 * @return nodeValue at longest prefix match in trie, NULL if not found
 */

static struct nodeValue *
lookup(struct trie *t, unsigned int addr){
  if(t->root == NULL){
    return NULL;
  }
  return lookup_recursive(t->root, NULL, addr, 0);
}


/**
 * @brief JNI call to allocate a new trie. 
 * 
 * @note Invariant: values passed in must be valid, no error checking. Appropriate for use as static method (jclass not referenced)
 * @throws OutOfMemoryError if memory cannot be allocated while creating trie
 * @return jlong used for referencing trie
 */
JNIEXPORT jlong JNICALL 
Java_org_apache_nifi_util_lookup_OffHeapLookup_newTrie(JNIEnv *env, jclass cls){
  struct trie * t = (struct trie *)malloc(sizeof(struct trie));
  if(t == NULL){
    throwOutOfMemoryError(env, "Unable to allocate offheap storage for trie");
    return 0;
  }
  t->root = NULL;
  return (jlong)t;
}

/**
 * @brief JNI call to insert into trie. 
 * 
 * @note Invariant: values passed in must be valid, no error checking. Appropriate for use as static method (jclass not referenced)
 * @throws OutOfMemoryError if memory cannot be allocated while inserting value
 * @throws Error on operations expected to not fail
 */
JNIEXPORT void JNICALL 
Java_org_apache_nifi_util_lookup_OffHeapLookup_trieInsert(JNIEnv *env, jclass cls, jlong pointer, jint address, jint mask, jbyteArray bytes){

    struct trie *t = (struct trie *)pointer;

    jbyte* bufferPtr = (*env)->GetByteArrayElements(env, bytes, NULL);
    if(bufferPtr == NULL){
      throwError(env, "Operation failed when calling GetByteArrayElements");
    }

    jsize lengthOfArray = (*env)->GetArrayLength(env, bytes);
    
    struct nodeValue * nv = (struct nodeValue *)malloc(lengthOfArray + sizeof(struct nodeValue) + 1);
    if(nv == NULL){
      throwOutOfMemoryError(env, "Unable to allocate offheap storage for value");
    }
    
    nv->valueLen = lengthOfArray;
    nv->value = (jbyte *)nv + sizeof(struct nodeValue);

    memcpy(nv->value, bufferPtr, lengthOfArray);

    (*env)->ReleaseByteArrayElements(env, bytes, bufferPtr, 0);
    
    insert(env, t, address, mask, nv);
}

/**
 * @brief JNI call to lookup address in trie, using longest prefix match
 * 
 * @note Invariant: values passed in must be valid, no error checking. Appropriate for use as static method (jclass not referenced)
 * @throws OutOfMemoryError if memory cannot be allocated for return value
 * @throws Error on operations expected to not fail
 */
JNIEXPORT jbyteArray JNICALL 
Java_org_apache_nifi_util_lookup_OffHeapLookup_trieLookup(JNIEnv *env, jclass cls, jlong pointer, jint address){
  struct trie *t = (struct trie *)pointer;
  struct nodeValue *nv = lookup(t, address);

  if(nv == NULL){
    return NULL;
  }

  jbyteArray arr = (*env)->NewByteArray(env, nv->valueLen);
  if(arr == NULL){
     throwError(env, "Return byte array cannot be constructed");
  }
  // NOTE: Throws ArrayIndexOutOfBounds exception, should not occur
  (*env)->SetByteArrayRegion(env, arr, 0, nv->valueLen, (jbyte*)nv->value);
  return arr;
}


