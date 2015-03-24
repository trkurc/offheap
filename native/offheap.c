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
      return;
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
      return;
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
 * @brief Recursive function for DFS deletion of trie
 * 
 * @note Invariant: values passed in must be valid, no error checking
 */
static void
delete_recursive(struct node *n){
  if(n->left != NULL){
    delete_recursive(n->left);
  }
  if(n->right != NULL){
    delete_recursive(n->right);
  }
  if(n->value != NULL){
    // NOTE: INVARIANT! value must have been allocated in one large block
    free(n->value);
  }
  free(n);
}

/**
 * @brief Deallocate trie
 * 
 * @note Invariant: values passed in must be valid, no error checking
 */
static void
delete(struct trie *t){
  if(t->root != NULL){
    delete_recursive(t->root);
  }
  free(t);
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
 * @brief JNI call to tear down a trie
 * 
 * @note Invariant: values passed in must be valid, no error checking. Appropriate for use as static method (jclass not referenced)
 * @throws OutOfMemoryError if memory cannot be allocated while creating trie
 * @return jlong used for referencing trie
 */
JNIEXPORT void JNICALL 
Java_org_apache_nifi_util_lookup_OffHeapLookup_deleteTrie(JNIEnv *env, jclass cls, jlong pointer){
    struct trie *t = (struct trie *)pointer;
    delete(t);
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
      return;
    }

    jsize lengthOfArray = (*env)->GetArrayLength(env, bytes);
    
    struct nodeValue * nv = (struct nodeValue *)malloc(lengthOfArray + sizeof(struct nodeValue));
    if(nv == NULL){
      (*env)->ReleaseByteArrayElements(env, bytes, bufferPtr, 0);
      throwOutOfMemoryError(env, "Unable to allocate offheap storage for value");
      return;
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
     throwOutOfMemoryError(env, "Return byte array cannot be constructed");
     return NULL;
  }
  // NOTE: Throws ArrayIndexOutOfBounds exception, should not occur
  (*env)->SetByteArrayRegion(env, arr, 0, nv->valueLen, (jbyte*)nv->value);
  return arr;
}


struct hnode {
  struct nodeValue *key;
  struct nodeValue *value;
  unsigned int hashCode;
  struct hnode * next;
};

struct htable {
  struct hnode **table;
  jint tableLen;
};

static unsigned int
hashNodeValue(struct nodeValue *nv){
  unsigned int result = 17;
  
  jint len = nv->valueLen;
  jint i;
 
  for(i=0; i < len; i++){
    result = 31 * result + nv->value[i];
  }	
  return result;
}


static int
nodeValueEquality(struct nodeValue *left, struct nodeValue *right){
  if(left->valueLen == right->valueLen){
    int match = left->valueLen;
    int i=0;
    jbyte *lv = left->value;
    jbyte *rv = right->value;
    for(i=0; i<match; i++){
      if(*lv != *rv){
	break;
      }
    }
    if(i == match){
      return 1;
    }
  }
  return 0;
}

void
htable_insert(JNIEnv *env, struct htable *h, struct nodeValue *key, struct nodeValue *value){
#ifdef DEBUG
  printf("Entering htable insert\n");
#endif
  unsigned int hashcode = hashNodeValue(key);
  unsigned int index = hashcode % (h->tableLen);
  struct hnode * newNode = (struct hnode *)malloc(sizeof(struct hnode));
  if(newNode == NULL){
    throwOutOfMemoryError(env, "Unable to allocate offheap storage for trie");
    return;
  }
  newNode->next = h->table[index];
  newNode->key = key;
  newNode->value = value;
  newNode->hashCode = hashcode;
  h->table[index] = newNode;
#ifdef DEBUG
  printf("Exiting htable insert\n");
#endif

}

struct nodeValue *
htable_lookup(struct htable *h, struct nodeValue *key){
  unsigned int hashcode = hashNodeValue(key);
  unsigned int index = hashcode % (h->tableLen);
  
  struct hnode *current = h->table[index];
  for(current = h->table[index]; current != NULL; current = current->next){
    if(current->hashCode == hashcode && nodeValueEquality(key, current->key)){
      return current->value;      
    }
  }
  return NULL;
}

/**
 * @brief JNI call to allocate a new htable. 
 * 
 * @note Invariant: values passed in must be valid, no error checking. Appropriate for use as static method (jclass not referenced)
 * @throws OutOfMemoryError if memory cannot be allocated while creating htable
 * @return jlong used for referencing htable
 */
JNIEXPORT jlong JNICALL 
Java_org_apache_nifi_util_lookup_OffHeapLookup_newHtable(JNIEnv *env, jclass cls, jint size){
  // TODO: Error check size?
  struct htable * h = (struct htable *)malloc(sizeof(struct htable));
  if(h == NULL){
    throwOutOfMemoryError(env, "Unable to allocate offheap storage for trie");
    return 0;
  }
  h->tableLen = size;

  h->table = (struct hnode **)calloc(size, sizeof(struct hnode *));
  if(h->table == NULL){
    free(h);
    throwOutOfMemoryError(env, "Unable to allocate offheap storage for trie");
    return 0;
  }
  return (jlong)h;
}

/**
 * @brief JNI call to tear down an htable
 * 
 * @note Invariant: values passed in must be valid, no error checking. Appropriate for use as static method (jclass not referenced)
 */
JNIEXPORT void JNICALL 
Java_org_apache_nifi_util_lookup_OffHeapLookup_deleteHtable(JNIEnv *env, jclass cls, jlong pointer){
    struct htable *h = (struct htable *)pointer;
    jint i;
    for(i=0; i< h->tableLen; i++){
      struct hnode *current;
      for(current = h->table[i]; current != NULL; ){
	// NOTE: invariant, the nodeValue value and struct allocated in one blob
	free(current->value);
	free(current->key);
	
	struct hnode *removeMe = current;
	current = current->next;
	free(removeMe);
      }	
    }
    free(h->table);
    free(h);
}


/**
 * @brief JNI call to insert into htable. 
 * 
 * @note Invariant: values passed in must be valid, no error checking. Appropriate for use as static method (jclass not referenced)
 * @throws OutOfMemoryError if memory cannot be allocated while inserting value
 * @throws Error on operations expected to not fail
 */
JNIEXPORT void JNICALL 
Java_org_apache_nifi_util_lookup_OffHeapLookup_htableInsert(JNIEnv *env, jclass cls, jlong pointer, jbyteArray key, jbyteArray value){

    struct htable *h = (struct htable *)pointer;

    // Make Key

    jbyte* bufferPtr = (*env)->GetByteArrayElements(env, key, NULL);
    if(bufferPtr == NULL){
      throwError(env, "Operation failed when calling GetByteArrayElements");
    }

    jsize lengthOfArray = (*env)->GetArrayLength(env, key);
    
    struct nodeValue * nvKey = (struct nodeValue *)malloc(lengthOfArray + sizeof(struct nodeValue));
    if(nvKey == NULL){
      (*env)->ReleaseByteArrayElements(env, key, bufferPtr, 0);
      throwOutOfMemoryError(env, "Unable to allocate offheap storage for value");
      return;
    }
    
    nvKey->valueLen = lengthOfArray;
    nvKey->value = (jbyte *)nvKey + sizeof(struct nodeValue);

    memcpy(nvKey->value, bufferPtr, lengthOfArray);

    (*env)->ReleaseByteArrayElements(env, key, bufferPtr, 0);
    
    // Make Value

    bufferPtr = (*env)->GetByteArrayElements(env, value, NULL);
    if(bufferPtr == NULL){
      free(nvKey);
      throwError(env, "Operation failed when calling GetByteArrayElements");
      return;
    }

    lengthOfArray = (*env)->GetArrayLength(env, value);
    
    struct nodeValue * nvValue = (struct nodeValue *)malloc(lengthOfArray + sizeof(struct nodeValue));
    if(nvValue == NULL){
      // clean up
      free(nvKey);
      (*env)->ReleaseByteArrayElements(env, value, bufferPtr, 0);
      throwOutOfMemoryError(env, "Unable to allocate offheap storage for value");
      return;
    }
    
    nvValue->valueLen = lengthOfArray;
    nvValue->value = (jbyte *)nvValue + sizeof(struct nodeValue);

    memcpy(nvValue->value, bufferPtr, lengthOfArray);

    (*env)->ReleaseByteArrayElements(env, value, bufferPtr, 0);
    
    htable_insert(env, h, nvKey, nvValue);
}

/**
 * @brief JNI call to lookup key in htable
 * 
 * @note Invariant: values passed in must be valid, no error checking. Appropriate for use as static method (jclass not referenced)
 * @throws OutOfMemoryError if memory cannot be allocated for return value
 * @throws Error on operations expected to not fail
 */
JNIEXPORT jbyteArray JNICALL 
Java_org_apache_nifi_util_lookup_OffHeapLookup_htableLookup(JNIEnv *env, jclass cls, jlong pointer, jbyteArray key){
  struct htable *h = (struct htable *)pointer;
  struct nodeValue nvKey;
  
  jbyte *bufferPtr = (*env)->GetByteArrayElements(env, key, NULL);
  if(bufferPtr == NULL){
    throwError(env, "Operation failed when calling GetByteArrayElements");
    return NULL;
  }
  nvKey.value = bufferPtr;
  nvKey.valueLen = (*env)->GetArrayLength(env, key);

  struct nodeValue *nv = htable_lookup(h, &nvKey);
  (*env)->ReleaseByteArrayElements(env, key, bufferPtr, 0);

  if(nv == NULL){
    return NULL;
  }

  jbyteArray arr = (*env)->NewByteArray(env, nv->valueLen);
  if(arr == NULL){
     throwOutOfMemoryError(env, "Return byte array cannot be constructed");
  }
  //NOTE: Throws ArrayIndexOutOfBounds exception, should not occur
  (*env)->SetByteArrayRegion(env, arr, 0, nv->valueLen, (jbyte*)nv->value);
  return arr;

}
