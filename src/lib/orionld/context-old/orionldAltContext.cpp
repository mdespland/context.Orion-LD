/*
*
* Copyright 2019 Telefonica Investigacion y Desarrollo, S.A.U
*
* This file is part of Orion Context Broker.
*
* Orion Context Broker is free software: you can redistribute it and/or
* modify it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* Orion Context Broker is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero
* General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with Orion Context Broker. If not, see http://www.gnu.org/licenses/.
*
* For those usages not covered by this license please contact with
* iot_support at tid dot es
*
* Author: Ken Zangelin
*/
#ifdef DEBUG
#include <sys/types.h>                                           // DIR, dirent
#include <fcntl.h>                                               // O_RDONLY
#include <dirent.h>                                              // opendir(), readdir(), closedir()
#include <sys/stat.h>                                            // statbuf
#include <unistd.h>                                              // stat()
#endif

#include <semaphore.h>                                           // sem_t, sem_init, sem_wait, sem_post

extern "C"
{
#include "kbase/kMacros.h"                                       // K_FT
#include "kalloc/kaStrdup.h"                                     // kaStrdup
#include "kalloc/kaAlloc.h"                                      // kaAlloc
#include "khash/khash.h"                                         // KHashTable
#include "kjson/KjNode.h"                                        // KjNode
#include "kjson/kjParse.h"                                       // kjParse
#include "kjson/kjBuilder.h"                                     // kjString, ...
#include "kjson/kjClone.h"                                       // kjClone
#include "kjson/kjLookup.h"                                      // kjLookup
}

#include "logMsg/logMsg.h"                                       // LM_*
#include "logMsg/traceLevels.h"                                  // Lmt*

#include "orionld/common/OrionldProblemDetails.h"                // OrionldProblemDetails
#include "orionld/common/SCOMPARE.h"                             // SCOMPAREx
#include "orionld/common/orionldState.h"                         // kalloc
#include "orionld/common/urlCheck.h"                             // urlCheck
#include "orionld/common/OrionldResponseBuffer.h"                // OrionldResponseBuffer
#include "orionld/common/orionldRequestSend.h"                   // orionldRequestSend
#include "orionld/common/uuidGenerate.h"                         // uuidGenerate
#include "orionld/context/orionldAltContext.h"                   // Own interface



// -----------------------------------------------------------------------------
//
// ORIONLD_CORE_CONTEXT_URL -
//
#define ORIONLD_CORE_CONTEXT_URL  (char*) "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"



// -----------------------------------------------------------------------------
//
// hashCode -
//
int hashCode(const char* name)
{
  int code = 0;

  while (*name != 0)
  {
    code += *name;
    ++name;
  }

  return code;
}



// -----------------------------------------------------------------------------
//
// nameCompareFunction -
//
int nameCompareFunction(const char* name, void* itemP)
{
  OrionldContextItem* cItemP = (OrionldContextItem*) itemP;

  return strcmp(name, cItemP->name);
}



// ----------------------------------------------------------------------------
//
// valueCompareFunction -
//
int valueCompareFunction(const char* longname, void* itemP)
{
  OrionldContextItem* cItemP = (OrionldContextItem*) itemP;

  LM_TMP(("ALT: Looking for '%s', comparing with '%s'", longname, cItemP->id));
  return strcmp(longname, cItemP->id);
}



// -----------------------------------------------------------------------------
//
// orionldContextUrlGenerate -
//
// The size used in the call to kaAlloc:
//   - strlen("http://HOSTNAME:PORT"): 12
//   - strlen("/ngsi-ld/contexts/"):   18
//   - uuidGenerate:                   37
//   - zero termination:                1
//   - orionldHostNameLen
//
//  => 68 + orionldHostNameLen
//
char* orionldContextUrlGenerate(char** contextIdP)
{
  char* url = (char*) kaAlloc(&kalloc, 68 + orionldHostNameLen);

  snprintf(url, 68 + orionldHostNameLen, "http://%s:%d/ngsi-ld/contexts/", orionldHostName, portNo);
  uuidGenerate(&url[30 + orionldHostNameLen]);

  *contextIdP = &url[30 + orionldHostNameLen];

  return url;
}



// -----------------------------------------------------------------------------
//
// orionldAltContextCreate -
//
OrionldAltContext* orionldAltContextCreate(const char* url, const char* nickname, const char* id, KjNode* tree, bool keyValues, bool local)
{
  const char* treeType = (tree == NULL)? "unknown" : kjValueType(tree->type);

  if (tree->type == KjString)
    LM_TMP(("NCTX: Creating a context for url '%s'. Real URL is: %s", url, treeType, tree->value.s));
  else
    LM_TMP(("NCTX: Creating a context for url '%s'. Its type is: %s", url, treeType));

  OrionldAltContext* contextP = (OrionldAltContext*) kaAlloc(&kalloc, sizeof(OrionldAltContext));

  contextP->nickname  = (char*) nickname;
  contextP->url       = (char*) url;
  contextP->id        = (char*) id;
  contextP->tree      = kjClone(tree);
  contextP->keyValues = keyValues;
  contextP->local     = local;

  return contextP;
}



// -----------------------------------------------------------------------------
//
// orionldContextHashTablesFill -
//
// Fill both the name hashtable and the value hashtable.
//
// The values in the key-value list can be either a string or an object
//
static void orionldContextHashTablesFill(OrionldAltContextHashTables* hashP, KjNode* keyValueTree)
{
  KHashTable* nameHashTableP  = hashP->nameHashTable;
  KHashTable* valueHashTableP = hashP->valueHashTable;

  LM_TMP(("HASH: nameHashTableP  at %p", nameHashTableP));
  LM_TMP(("HASH: valueHashTableP at %p", valueHashTableP));

  for (KjNode* kvP = keyValueTree->value.firstChildP; kvP != NULL; kvP = kvP->next)
  {
    OrionldContextItem* hiP = (OrionldContextItem*) kaAlloc(&kalloc, sizeof(OrionldContextItem));

    hiP->name = kaStrdup(&kalloc, kvP->name);
    hiP->type = NULL;

    if (kvP->type == KjString)
    {
      LM_TMP(("ALT: context key-value '%s': a string", kvP->name));
      hiP->id = kaStrdup(&kalloc, kvP->value.s);
    }
    else if (kvP->type == KjObject)
    {
      hiP->id = NULL;
      LM_TMP(("ALT: context key-value '%s': an object", kvP->name));
      //
      // Find @id, @type
      //
      for (KjNode* itemP = kvP->value.firstChildP; itemP != NULL; itemP = itemP->next)
      {
        if (strcmp(itemP->name, "@id") == 0)
          hiP->id =	kaStrdup(&kalloc, itemP->value.s);
        else if (strcmp(itemP->name, "@type") == 0)
          hiP->type = kaStrdup(&kalloc, itemP->value.s);
      }
    }
    else
    {
      LM_TMP(("ALT: WARNING: context key-value '%s': %s", kvP->name, kjValueType(kvP->type)));
      continue;
    }

    if (hiP->id == NULL)
    {
      LM_TMP(("ALT: WARNING: NULL value for key '%s'", kvP->name));
      continue;
    }

    if (hiP->type != NULL)
      LM_TMP(("ALT: Inserting key-value %s -> %s (type: %s)", kvP->name, hiP->id, hiP->type));
    else
      LM_TMP(("ALT: Inserting key-value %s -> %s", kvP->name, hiP->id));

    khashItemAdd(nameHashTableP,  hiP->name, hiP);
    khashItemAdd(valueHashTableP, hiP->id,   hiP);
  }
}



// -----------------------------------------------------------------------------
//
// httpResponse -
//
static __thread OrionldResponseBuffer  httpResponse;



// -----------------------------------------------------------------------------
//
// orionldAltContextDownload -
//
static char* orionldAltContextDownload(const char* url, bool* downloadFailedP, OrionldProblemDetails* pdP)
{
  bool ok = false;

  *downloadFailedP = false;

  LM_TMP(("NCTX: downloading context '%s'", url));
  for (int tries = 0; tries < contextDownloadAttempts; tries++)
  {
    httpResponse.buf       = NULL;
    httpResponse.size      = 0;
    httpResponse.used      = 0;
    httpResponse.allocated = false;

    LM_TMP(("ALT: Downloading context '%s'", url));

    //
    // detailsPP is filled in by orionldRequestSend()
    // httpResponse.buf freed by orionldRequestSend() in case of error
    //
    bool tryAgain = false;
    bool reqOk;

    LM_TMP(("CURL: Calling orionldRequestSend"));
    reqOk = orionldRequestSend(&httpResponse, url, contextDownloadTimeout, &pdP->detail, &tryAgain, downloadFailedP, "Accept: application/ld+json");
    LM_TMP(("CURL: orionldRequestSend returned %s", K_FT(reqOk)));
    if (reqOk == true)
    {
      ok = true;
      break;
    }
    else
      LM_E(("orionldRequestSend failed (try number %d out of %d. Timeout is: %dms): %s", tries + 1, contextDownloadAttempts, contextDownloadTimeout, pdP->detail));

    if (tryAgain == false)
      break;
  }

  if (ok == false)
  {
    LM_E(("orionldRequestSend failed - downloadFailed set to TRUE"));

    pdP->type   = OrionldBadRequestData;
    pdP->title  = (char*) "Unable to download context";
    pdP->detail = (char*) url;
    pdP->status = 409;  // ??

    return NULL;
  }

  LM_TMP(("ALT: Downloaded context"));
  return httpResponse.buf;
}



// -----------------------------------------------------------------------------
//
// contextMemberGet -
//
static KjNode* contextMemberGet(KjNode* tree, OrionldProblemDetails* pdP)
{
  //
  // Supposedly, the tree is an object, and inside that object, there is a "@context" member 
  //
  if (tree->type != KjObject)
    return NULL;

  for (KjNode* nodeP = tree->value.firstChildP; nodeP != NULL; nodeP = nodeP->next)
  {
    if (strcmp(nodeP->name, "@context") == 0)
      return nodeP;
  }

  return NULL;
}



// -----------------------------------------------------------------------------
//
// orionldAltContextList
//
static sem_t                orionldAltContextListSem;
static OrionldAltContext*   orionldAltContextListArray[100];  // When 100 is not enough, a realloc is done
static OrionldAltContext**  orionldAltContextList         = orionldAltContextListArray;
static int                  orionldAltContextListSlots    = 100;
static int                  orionldAltContextListSlotIx   = 0;



// -----------------------------------------------------------------------------
//
// orionldAltContextPresentTree -
//
void orionldAltContextPresentTree(const char* prefix, KjNode* contextNodeP)
{
  if (contextNodeP->type == KjString)
    LM_TMP(("%s: the context is a String: %s", prefix, contextNodeP->value.s));
  else if (contextNodeP->type == KjArray)
  {
    LM_TMP(("%s: the context is an Array:", prefix));
    for (KjNode* aItemP = contextNodeP->value.firstChildP; aItemP != NULL; aItemP = aItemP->next)
      orionldAltContextPresentTree(prefix, aItemP);
  }
  else if (contextNodeP->type == KjObject)
  {
    int items = 0;
    LM_TMP(("%s: the context is an Object:", prefix));
    for	(KjNode* itemP = contextNodeP->value.firstChildP; itemP != NULL; itemP = itemP->next)
    {
      if (itemP->type == KjString)
        LM_TMP(("%s: %s -> %s", prefix, itemP->name, itemP->value));
      else
        LM_TMP(("%s: %s (%s)", prefix, itemP->name, kjValueType(itemP->type)));
      ++items;
      if (items > 3)
        break;
    }
  }
  else
    LM_TMP(("%s: Invalid type for context: %s", prefix, kjValueType(contextNodeP->type)));
  LM_TMP(("%s", prefix));
}



// -----------------------------------------------------------------------------
//
// orionldAltContextPresent -
//
void orionldAltContextPresent(const char* prefix, OrionldAltContext* contextP)
{
  LM_TMP(("%s: Context '%s' (%s)", prefix, contextP->url, contextP->keyValues? "Key-Values" : "Array"));
  LM_TMP(("%s: ----------------------------------------------------------------------------", prefix));

  if (contextP->keyValues == true)
  {
    int          noOfItems = 0;
    KHashTable*  htP       = contextP->context.hash.nameHashTable;

    for (int slot = 0; slot < 1024; slot++)
    {
      KHashListItem* itemP = htP->array[slot];

      while (itemP != 0)
      {
        OrionldContextItem* hiP = (OrionldContextItem*) itemP->data;

        LM_TMP(("%s: key-value[slot %d]: %s -> %s (type: %s)", prefix, slot, hiP->name, hiP->id, hiP->type));
        itemP = itemP->next;
        ++noOfItems;

        if (noOfItems >= 3)
          break;
      }

      if (noOfItems >= 3)
        break;
    }
  }
  else
  {
    for (int iIx = 0; iIx < contextP->context.array.items; iIx++)
    {
      LM_TMP(("%s:   Array Item %d: %s (%s)",
              prefix,
              iIx,
              contextP->context.array.vector[iIx]->url,
              contextP->context.array.vector[iIx]->keyValues? "Key-Values" : "Array"));
    }
  }
}



// -----------------------------------------------------------------------------
//
// orionldAltContextListPresent -
//
void orionldAltContextListPresent(const char* prefix, const char* info)
{
  LM_TMP(("%s: %s: %d Contexts", prefix, info, orionldAltContextListSlotIx));
  LM_TMP(("%s: ========================================================================", prefix));
  for (int ix = 0; ix < orionldAltContextListSlotIx; ix++)
  {
    orionldAltContextPresent(prefix, orionldAltContextList[ix]);
    LM_TMP(("%s:", prefix));
  }
  LM_TMP(("%s: ========================================================================", prefix));
}



// -----------------------------------------------------------------------------
//
// orionldAltContextLookup -
//
OrionldAltContext* orionldAltContextLookup(const char* url)
{
  LM_TMP(("CLIST: Looking up context '%s'", url));
  for (int ix = 0; ix < orionldAltContextListSlotIx; ix++)
  {
    LM_TMP(("CLIST:   Looking for '%s', comparing with '%s'", url, orionldAltContextList[ix]->url));
    if (strcmp(url, orionldAltContextList[ix]->url) == 0)
    {
      LM_TMP(("CLIST:   Found context '%s'", url));
      return orionldAltContextList[ix];
    }

    if (orionldAltContextList[ix]->id != NULL)
    {
      LM_TMP(("CLIST:   Looking for '%s', comparing with '%s'", url, orionldAltContextList[ix]->id));
      if (strcmp(url, orionldAltContextList[ix]->id) == 0)
      {
        LM_TMP(("CLIST:   Found context '%s'", url));
        return orionldAltContextList[ix];
      }
    }
  }

  LM_TMP(("ALT:   Did not find context '%s'", url));
  return NULL;
}



// -----------------------------------------------------------------------------
//
// orionldAltContextListInsert -
//
void orionldAltContextListInsert(OrionldAltContext* contextP)
{
  LM_TMP(("CLIST-INSERT: inserting context %s", contextP->url));

  sem_wait(&orionldAltContextListSem);

  //
  // Reallocation necessary?
  //

  if (orionldAltContextListSlotIx >= orionldAltContextListSlots)
  {
    char* newArray = (char*) kaAlloc(&kalloc, sizeof(OrionldAltContext*) * (orionldAltContextListSlots + 50));

    memcpy(newArray, (char*) orionldAltContextList, sizeof(OrionldAltContext*) * orionldAltContextListSlots);
    bzero(&newArray[sizeof(OrionldAltContext*) * orionldAltContextListSlots], 50 * sizeof(OrionldAltContext*));

    orionldAltContextListSlots += 50;
    orionldAltContextList = (OrionldAltContext**) newArray;
  }

  orionldAltContextList[orionldAltContextListSlotIx] = contextP;
  ++orionldAltContextListSlotIx;

  sem_post(&orionldAltContextListSem);

  orionldAltContextListPresent("CLIST-INSERT", "AFTER INSERTION");
}



// -----------------------------------------------------------------------------
//
// orionldAltCoreContextP - The Core Context
//
OrionldAltContext* orionldAltCoreContextP   = NULL;
char*              orionldAltDefaultUrl     = NULL;
int                orionldAltDefaultUrlLen  = 1000;  // To provoke errors is not properly set



// -----------------------------------------------------------------------------
//
// orionldAltContextCreateFromKeyValueObject -
//
OrionldAltContext* orionldAltContextCreateFromKeyValueObject(KjNode* keyValueObjectP)
{
  char*              id;
  char*              url      = orionldContextUrlGenerate(&id);
  OrionldAltContext* contextP = orionldAltContextCreate(url, "inline-object", id, keyValueObjectP, true, true);

  contextP->context.hash.nameHashTable  = khashTableCreate(&kalloc, hashCode, nameCompareFunction,  1024);
  contextP->context.hash.valueHashTable = khashTableCreate(&kalloc, hashCode, valueCompareFunction, 1024);

  LM_TMP(("HASH: nameHashTable  at %p", contextP->context.hash.nameHashTable));
  LM_TMP(("HASH: valueHashTable at %p", contextP->context.hash.valueHashTable));
  LM_TMP(("HASH: Calling orionldContextHashTablesFill"));
  orionldContextHashTablesFill(&contextP->context.hash, keyValueObjectP);

  return contextP;
}


OrionldAltContext* orionldAltContextCreateFromArray(KjNode* contextArrayP)
{
  char*              url      = orionldContextUrlGenerate(NULL);
  OrionldAltContext* contextP = orionldAltContextCreate(url, "array", NULL, contextArrayP, false, true);
  
  for (KjNode* arrayItemP = contextArrayP->value.firstChildP; arrayItemP != NULL; arrayItemP = arrayItemP->next)
  {
    if (arrayItemP->type == KjString)
    {
      if (orionldAltContextLookup(arrayItemP->value.s) == NULL)
      {
      }
    }
    else if (arrayItemP->type == KjObject)
    {
    }
    else
    {
      LM_W(("Bad Input (invalid type of context array item: %s)", kjValueType(arrayItemP->type)));
    }
  }
}



// -----------------------------------------------------------------------------
//
// orionldContextArraySimplify - simplify array if possible
//
KjNode* orionldContextArraySimplify(KjNode* contextArrayP, const char* url)
{
  int items   = 0;
  int removed = 0;

  LM_TMP(("ARRFIX: Fixing context array '%s'", url));

  //
  // In this loop, items may be removed from the tree, so, we need to save the next pointer before removing
  // and then use it to point to the next item.
  //
  KjNode* arrayItemP = contextArrayP->value.firstChildP;

  while (arrayItemP != NULL)
  {
    KjNode* next = arrayItemP->next;

    if ((arrayItemP->type == KjString) && (strcmp(arrayItemP->value.s, ORIONLD_CORE_CONTEXT_URL) == 0))
    {
      //
      // Found the Core Context - remove it !!!
      //
      ++removed;
      kjChildRemove(contextArrayP, arrayItemP);
      LM_TMP(("ARRFIX: removed a Core Context from the context array"));
    }
    else if (arrayItemP->type == KjObject)
    {
      //
      // Found an inline object inside the array -
      // - Remove the object from the array
      // - create a separate context with the Object
      // - invent a URL for it (done by orionldAltContextCreateFromKeyValueObject)
      // - insert the new context into the context cache
      // - insert a String KjNode referencing the new context of the object
      //
      // [ The variable 'removed' is not modified as one item is replaced by another ]
      //
      kjChildRemove(contextArrayP, arrayItemP);

      OrionldAltContext* contextP    = orionldAltContextCreateFromKeyValueObject(arrayItemP);
      KjNode*            stringNodeP = kjString(&kjson, contextP->url);

      orionldAltContextListInsert(contextP);
      kjChildAdd(contextArrayP, stringNodeP);
    }
    
    ++items;

    arrayItemP = next;
  }

  if (items - removed == 0)
  {
    LM_TMP(("ARRFIX: Array is empty after fixes - returning NULL"));
    return NULL;
  }

  if (items - removed == 1)
  {
    LM_TMP(("ARRFIX: Only one item left in array - returning an %s", kjValueType(contextArrayP->value.firstChildP->type)));
    return contextArrayP->value.firstChildP;
  }

  LM_TMP(("ARRFIX: after removing %d items, returning the same array (now with %d items)", removed, items - removed));
  return contextArrayP;
}



// -----------------------------------------------------------------------------
//
// orionldAltContextCreateFromTree -
//
OrionldAltContext* orionldAltContextCreateFromTree(const char* url, KjNode* contextNodeP, OrionldProblemDetails* pdP)
{
  OrionldAltContext* contextP;

  LM_TMP(("NCTX: ***** In orionldAltContextCreateFromTree (url: %s)", url));

  //
  // If the context is an Array, we might get to simplify it a little bit
  // - If an array of only one item, the array will be removed and the item used directly
  // - If the core context is an item, then it is removed
  //
  // This must be done first, as an Array may be converted into a String
  //
  if (contextNodeP->type == KjArray)
  {
    LM_TMP(("NCTX: Fixing the array"));
    contextNodeP = orionldContextArraySimplify(contextNodeP, url);
    if (contextNodeP == NULL)  // Nothing left in the context after removing core context
    {
      LM_TMP(("NCTX: Nothing left after fixing the array"));      
      return orionldAltCoreContextP;
    }
    orionldAltContextListPresent("ARRFIX", "After orionldContextArraySimplify");

    if (contextNodeP->type == KjString)
    {
      LM_TMP(("NCTX: Array -> String. The URL of the String is: %s", contextNodeP->value.s));

      OrionldAltContext* contextP = orionldAltContextCreate(url, "arr-to-string", NULL, contextNodeP, false, true);
      orionldAltContextListInsert(contextP);
      return contextP;
    }
  }

  if (contextNodeP->type == KjString)
  {
    if ((url != NULL) && (strcmp(url, ORIONLD_CORE_CONTEXT_URL) == 0))
    {
      LM_TMP(("the context is a string and it's the core context"));
      return orionldAltCoreContextP;
    }
    else if ((contextP = orionldAltContextLookup(contextNodeP->value.s)) != NULL)
    {
      LM_TMP(("NCTX: Found context %s", contextNodeP->value.s));
      return contextP;
    }
    else
    {
      LM_TMP(("NCTX: Here possibly a new Context should be created: a String context '%s' value: %s", url, contextNodeP->value.s));
      
      LM_TMP(("the context is a string '%s', calling orionldAltContextCreateFromUrl", contextNodeP->value.s));
      contextP = orionldAltContextCreateFromUrl(contextNodeP->value.s, pdP);
      contextP->tree = kjClone(contextNodeP);
      return contextP;
    }
  }
  else if (contextNodeP->type == KjObject)
  {
    contextP = orionldAltContextCreateFromKeyValueObject(contextNodeP);
    orionldAltContextListInsert(contextP);
  }
  else if (contextNodeP->type == KjArray)
  {
    LM_TMP(("CTX: The @context is an Array"));

    // First, count the number of items in the array, excluding the Core Context, if present
    int arrayItems = 0;
    for (KjNode* arrayItemP = contextNodeP->value.firstChildP; arrayItemP != NULL; arrayItemP = arrayItemP->next)
    {
      if (arrayItemP->type == KjString)
      {
        if (strcmp(arrayItemP->value.s, ORIONLD_CORE_CONTEXT_URL) == 0)
          continue;
      }
      ++arrayItems;
    }

    contextP = orionldAltContextCreate(url, "nick", NULL, contextNodeP, false, false);

    contextP->keyValues              = false;
    contextP->context.array.items    = arrayItems;
    contextP->context.array.vector   = (OrionldAltContext**) kaAlloc(&kalloc, arrayItems * sizeof(OrionldAltContext*));

    int slot        = arrayItems - 1;  // NOTE: Insertion starts at the end of the array - the array is sorted backwards!
    int arrayItemNo = -1;

    for (KjNode* arrayItemP = contextNodeP->value.firstChildP; arrayItemP != NULL; arrayItemP = arrayItemP->next)
    {
      ++arrayItemNo;

      if (arrayItemP->type == KjString)
      {
        if (strcmp(arrayItemP->value.s, ORIONLD_CORE_CONTEXT_URL) != 0)
        {
          contextP->context.array.vector[slot] = orionldAltContextLookup(arrayItemP->value.s);
          if (contextP->context.array.vector[slot] == NULL)
          {
            contextP->context.array.vector[slot] = orionldAltContextCreateFromUrl(arrayItemP->value.s, pdP);
          }
          --slot;
        }
      }
      else if (arrayItemP->type == KjObject)
      {
        LM_TMP(("CTX: current slot: %d. Object context", slot));
        OrionldAltContext* newContextP = contextP->context.array.vector[slot] = orionldAltContextInlineInsert(arrayItemP, pdP);
        --slot;
        LM_TMP(("CTX: inserted context '%s' (an object) in index %d", contextP->context.array.vector[slot]->url, slot));
        kjChildRemove(contextNodeP, arrayItemP);
        KjNode* contextNodeP = kjString(kjsonP, newContextP->url, NULL);
        kjChildAdd(contextNodeP, contextNodeP);
      }
      else
        LM_TMP(("CTX: invalid type of array item: %s", kjValueType(arrayItemP->type)));
    }

    LM_TMP(("CLISTI: Calling orionldAltContextListInsert for the array context '%s'", contextP->url));
  }

  LM_TMP(("CLISTI: Calling orionldAltContextListInsert for %s", contextP->url));
  orionldAltContextListInsert(contextP);
  orionldAltContextListPresent("CLISTI", "End-of-orionldAltContextCreateFromTree");
  
  return contextP;
}



// -----------------------------------------------------------------------------
//
// orionldAltContextCreateFromBuffer -
//
OrionldAltContext* orionldAltContextCreateFromBuffer(const char* url, char* buf, OrionldProblemDetails* pdP)
{
  KjNode*  tree;
  KjNode*  contextNodeP;

  if ((tree = kjParse(kjsonP, buf)) == NULL)
    LM_X(1, ("error parsing the context"));

  if ((contextNodeP = contextMemberGet(tree, pdP)) == NULL)
    LM_X(1, ("Can't find the value of @context"));

  LM_TMP(("CTX: Calling orionldAltContextCreateFromTree"));
  return orionldAltContextCreateFromTree(url, contextNodeP, pdP);
}



// -----------------------------------------------------------------------------
//
// orionldAltContextCreateFromUrl -
//
OrionldAltContext* orionldAltContextCreateFromUrl(const char* url, OrionldProblemDetails* pdP)
{
  //
  // 0. If it is the Core Context, then a pointer to the Code context is returned
  // 1. Download the context
  // 2. Parse the context
  // 3. Get the value of the "@context" field
  // 4. If the Core Context is part of the context, remove it
  // 5. If nothing left after (4), then a pointer to the Code context is returned
  // 6. Count the number of toplevel members
  // 7. Create the context with its URI as identifier
  // 8. What type of context is it?
  // 8.0 Assign OrionldAltContext::context.array[ix] to each "sub-context"
  //     Except if 8.2, where OrionldAltContext::context.hashTable is used
  // 8.1 if String                        - call orionldAltContextCreateFromUrl()
  // 8.2 if Object                        - call orionldAltContextCreateFromKeyValueList()
  // 8.3 if Array with all strings        - call orionldAltContextCreateFromUrl() for each item
  // 8.4 if Array with mix string/object  - call orionldAltContextCreateFromUrl() for the sxtrings and 
  // 9. 
  //

  //
  // If the context already exists in the cache, then it is simply returned
  //
  OrionldAltContext* contextP;

  LM_TMP(("CLIST2: looking up context '%s'", url));
  orionldAltContextListPresent("CLOOKUP", "looking up a context");

  char*    buf;
  bool     downloadFailed;

  if (urlCheck((char*) url, &pdP->detail) == false)
  {
    pdP->type   = OrionldBadRequestData;
    pdP->title  = (char*) "invalid URL";
    pdP->detail = (char*) url;
    pdP->status = 400;
    return NULL;
  }

  //
  // Skip this if Core Context, except in the initialization phase 
  //
  if ((orionldAltCoreContextP != NULL) && (strcmp(url, ORIONLD_CORE_CONTEXT_URL) == 0))
  {
    LM_TMP(("it's the Core Context"));
    return orionldAltCoreContextP;
  }

  if ((buf = orionldAltContextDownload(url, &downloadFailed, pdP)) == NULL)
  {
    LM_E(("orionldAltContextDownload: %s: %s", pdP->title, pdP->detail));
    return NULL;
  }

  LM_TMP(("CTX: Calling orionldAltContextCreateFromBuffer"));
  return orionldAltContextCreateFromBuffer(url, buf, pdP);
}



// -----------------------------------------------------------------------------
//
// orionldAltContextListInit -
//
void orionldAltContextListInit(void)
{
  bzero(&orionldAltContextListArray, sizeof(orionldAltContextListArray));

  if (sem_init(&orionldAltContextListSem, 0, 1) == -1)
    LM_X(1, ("Runtime Error (error initializing semaphore for orionld context list; %s)", strerror(errno)));
}



#if DEBUG
// -----------------------------------------------------------------------------
//
// contextFileParse -
//
int contextFileParse(char* fileBuffer, int bufLen, char** urlP, char** jsonP, OrionldProblemDetails* pdP)
{
  //
  // 1. Skip initial whitespace
  // Note: 0xD (13) is the Windows 'carriage ret' character
  //
  while ((*fileBuffer != 0) && ((*fileBuffer == ' ') || (*fileBuffer == '\t') || (*fileBuffer == '\n') || (*fileBuffer == 0xD)))
    ++fileBuffer;

  if (*fileBuffer == 0)
  {
    pdP->detail = (char*) "empty context file (or, only whitespace)";
    return -1;
  }


  //
  // 2. The URL is on the first line of the buffer
  //
  *urlP = fileBuffer;
  LM_T(LmtPreloadedContexts, ("Parsing fileBuffer. URL is %s", *urlP));


  //
  // 3. Find the '\n' that ends the URL
  //
  while ((*fileBuffer != 0) && (*fileBuffer != '\n'))
    ++fileBuffer;

  if (*fileBuffer == 0)
  {
    pdP->detail = (char*) "can't find the end of the URL line";
    return -1;
  }


  //
  // 4. Zero-terminate URL
  //
  *fileBuffer = 0;


  //
  // 5. Jump over the \n and onto the first char of the next line
  //
  ++fileBuffer;


  //
  // 1. Skip initial whitespace
  // Note: 0xD (13) is the Windows 'carriage ret' character
  //
  while ((*fileBuffer != 0) && ((*fileBuffer == ' ') || (*fileBuffer == '\t') || (*fileBuffer == '\n') || (*fileBuffer == 0xD)))
    ++fileBuffer;

  if (*fileBuffer == 0)
  {
    pdP->detail = (char*) "no JSON Context found";
    return -1;
  }

  *jsonP = fileBuffer;
  LM_T(LmtPreloadedContexts, ("Parsing fileBuffer. JSON is %s", *jsonP));

  return 0;
}



// -----------------------------------------------------------------------------
//
// contextFileTreat -
//
static void contextFileTreat(char* dir, struct dirent* dirItemP)
{
  char*                  fileBuffer;
  struct stat            statBuf;
  char                   path[512];
  OrionldProblemDetails  pd;

  snprintf(path, sizeof(path), "%s/%s", dir, dirItemP->d_name);
  LM_T(LmtPreloadedContexts, ("Treating 'preloaded' context file '%s'", path));

  if (stat(path, &statBuf) != 0)
    LM_X(1, ("stat(%s): %s", path, strerror(errno)));

  fileBuffer = (char*) malloc(statBuf.st_size + 1);
  if (fileBuffer == NULL)
    LM_X(1, ("Out of memory"));

  int fd = open(path, O_RDONLY);
  if (fd == -1)
    LM_X(1, ("open(%s): %s", path, strerror(errno)));

  int nb;
  nb = read(fd, fileBuffer, statBuf.st_size);
  if (nb != statBuf.st_size)
    LM_X(1, ("read(%s): %s", path, strerror(errno)));
  fileBuffer[statBuf.st_size] = 0;
  close(fd);


  //
  // OK, the entire buffer is in 'fileBuffer'
  // Now let's parse the buffer to extract URL (first line)
  // and the "payload" that is the JSON of the context
  //
  char* url;
  char* json;

  if (contextFileParse(fileBuffer, statBuf.st_size, &url, &json, &pd) != 0)
    LM_X(1, ("error parsing the context file '%s': %s", path, pd.detail));

  //
  // We have both the URL and the 'JSON Context'.
  // Time to parse the 'JSON Context', create the OrionldContext, and insert it into the list of contexts
  //

  OrionldAltContext* contextP = orionldAltContextCreateFromBuffer(url, json, &pd);

  if (strcmp(url, ORIONLD_CORE_CONTEXT_URL) == 0)
  {
    if (contextP == NULL)
      LM_X(1, ("error creating the core context from file system file '%s'", path));
    orionldAltCoreContextP = contextP;
  }
  else
  {
    if (contextP == NULL)
      LM_E(("error creating context from file system file '%s'", path));
    else
    {
      LM_TMP(("CLISTI: Calling orionldAltContextListInsert for %s", contextP->url));
      orionldAltContextListInsert(contextP);
    }
  }
}



// -----------------------------------------------------------------------------
//
// fileSystemContexts -
//
static bool fileSystemContexts(char* cacheContextDir)
{
  DIR*            dirP;
  struct  dirent  dirItem;
  struct  dirent* result;

  dirP = opendir(cacheContextDir);
  if (dirP == NULL)
  {
    //
    // FIXME PR: Should the broker die here (Cache Context Directory given but it doesn't exist)
    //           or should the broker continue (downloading the core context) ???
    //           Continue, by returning false.
    LM_X(1, ("opendir(%s): %s", cacheContextDir, strerror(errno)));
  }

  while (readdir_r(dirP, &dirItem, &result) == 0)
  {
    if (result == NULL)
      break;

    if (dirItem.d_name[0] == '.')  // skip hidden files and '.'/'..'
      continue;

    contextFileTreat(cacheContextDir, &dirItem);
  }
  closedir(dirP);
  return true;
}
#endif



// -----------------------------------------------------------------------------
//
// orionldAltContextInit -
//
bool orionldAltContextInit(OrionldProblemDetails* pdP)
{
  LM_TMP(("ALT: Initializing ALT Context list"));
  orionldAltContextListInit();

  char* cacheContextDir = getenv("ORIONLD_CACHED_CONTEXT_DIRECTORY");
  bool  gotCoreContext  = false;

  LM_TMP(("CTX: In orionldAltContextInit"));

#if DEBUG
  if (cacheContextDir != NULL)
  {
    LM_TMP(("ALT: Getting initial contexts from '%s'", cacheContextDir));
    gotCoreContext = fileSystemContexts(cacheContextDir);
    if (gotCoreContext == false)
      LM_E(("Unable to cache pre-loaded contexts from '%s'", cacheContextDir));
    orionldAltContextListPresent("INIT", "After loading initial cached contexts");
  }
#endif

  if (gotCoreContext == false)
  {
    LM_TMP(("ALT: Downloading and processing Core Context"));
    orionldAltCoreContextP = orionldAltContextCreateFromUrl(ORIONLD_CORE_CONTEXT_URL, pdP);

    if (orionldAltCoreContextP == NULL)
    {
      LM_TMP(("ALT: orionldAltContextCreateFromUrl: %s %s", pdP->title, pdP->detail));
      return false;
    }
  }

  OrionldContextItem* vocabP = orionldAltContextItemLookup(orionldAltCoreContextP, "@vocab", NULL);

  if (vocabP == NULL)
  {
    LM_E(("Context Error (no @vocab item found in Core Context)"));
    orionldAltDefaultUrl    = (char*) "https://example.org/ngsi-ld/default/";
  }
  else
    orionldAltDefaultUrl = vocabP->id;

  orionldAltDefaultUrlLen = strlen(orionldAltDefaultUrl);

  LM_TMP(("ALT: orionldAltCoreContextP at %p", orionldAltCoreContextP));
  LM_TMP(("ALT:                      url: %s", orionldAltCoreContextP->url));

  LM_TMP(("CTX: From orionldAltContextInit"));
  return true;
}



// -----------------------------------------------------------------------------
//
// orionldAltContextInlineInsert -
//
OrionldAltContext* orionldAltContextInlineInsert(KjNode* contextNodeP, OrionldProblemDetails* pdP)
{
  OrionldAltContext* contextP;

  if (contextNodeP->type == KjString)
  {
    LM_TMP(("ALT: inline context is a STRING"));

    LM_TMP(("ALT: Looking up context '%s'", contextNodeP->value.s));
    if ((contextP = orionldAltContextLookup(contextNodeP->value.s)) != NULL)
    {
      LM_TMP(("ALT: Found context %s", contextNodeP->value.s));
      return contextP;
    }
    LM_TMP(("ALT: Context %s was not found", contextNodeP->value.s));
    orionldAltContextListPresent("ALT", "NOT FOUND");

    LM_TMP(("CTX: Calling orionldAltContextCreateFromUrl"));
    contextP = orionldAltContextCreateFromUrl(contextNodeP->value.s, pdP);
    LM_TMP(("CTX: back from orionldAltContextCreateFromUrl"));

    if (contextP == NULL)
      LM_E(("orionldAltContextCreateFromUrl: %s %s", pdP->title, pdP->detail));

    return contextP;
  }

  char* contextId;
  char* url = orionldContextUrlGenerate(&contextId);

  LM_TMP(("NCTX: Not a String - calling orionldAltContextCreateFromTree (url == '%s')", url));
  contextP = orionldAltContextCreateFromTree(url, contextNodeP, pdP);
  contextP->id = contextId;

  return contextP;
}



// ----------------------------------------------------------------------------
//
// orionldAltContextItemLookup - lookup an item in a context
//
OrionldContextItem* orionldAltContextItemLookup(OrionldAltContext* contextP, const char* name, bool* valueMayBeCompactedP)
{
  OrionldContextItem* itemP = NULL;

  if (contextP == NULL)
  {
    LM_TMP(("ALT3: NULL context: using Core Context"));
    contextP = orionldAltCoreContextP;
  }
  
  LM_TMP(("ALT3: Looking for '%s' in context '%s'", name, contextP->url));

  if (contextP->keyValues == true)
  {
    LM_TMP(("ALT3: Context is key-values - direct lookup in hash-table"));
    itemP = (OrionldContextItem*) khashItemLookup(contextP->context.hash.nameHashTable, name);
    LM_TMP(("ALT3: hash-table lookup of '%s': %s", name, (itemP == NULL)? "Not Found" : "FOUND"));
  }
  else
  {
    LM_TMP(("ALT3: Context is an Array, no key-values"));
    for (int ix = 0; ix < contextP->context.array.items; ++ix)
    {
      LM_TMP(("ALT3: recursive call (%d in loop) for context %s", ix, contextP->context.array.vector[ix]->url));
      if ((itemP = orionldAltContextItemLookup(contextP->context.array.vector[ix], name, valueMayBeCompactedP)) != NULL)
        break;
    }
  }

  LM_TMP(("ALT3: %s '%s' in context '%s'", itemP? "Found" : "Didn't find", name, contextP->url));

  if (valueMayBeCompactedP != NULL)
  {
    if ((itemP->type != NULL) && (strcmp(itemP->type, "@vocab") == 0))
      *valueMayBeCompactedP = true;
    else
      *valueMayBeCompactedP = false;
  }

  return itemP;
}



// ----------------------------------------------------------------------------
//
// orionldAltContextItemValueLookup - lookup a value in a context
//
OrionldContextItem* orionldAltContextItemValueLookup(OrionldAltContext* contextP, const char* longname)
{
  OrionldContextItem* itemP = NULL;

  LM_TMP(("ALT: Looking for value '%s' in context '%s'", longname, contextP->url));

  if (contextP->keyValues == true)
  {
    LM_TMP(("ALT: keyValues: calling khashItemCustomLookup"));
    itemP = (OrionldContextItem*) khashItemLookup(contextP->context.hash.valueHashTable, longname);
  }
  else
  {
    LM_TMP(("ALT: Array"));
    for (int ix = 0; ix < contextP->context.array.items; ++ix)
    {
      LM_TMP(("ALT: Recursive call to orionldAltContextItemValueLookup for context '%s'", contextP->context.array.vector[ix]->url));
      if ((itemP = orionldAltContextItemLookup(contextP->context.array.vector[ix], longname, NULL)) != NULL)
        break;
    }
  }

  return itemP;
}



// -----------------------------------------------------------------------------
//
// orionldAltContextItemExpand -
//
// PARAMETERS
//   contextP                the context
//   shortName               the name to expand
//   valueMayBeExpandedP     pointer to a bool that is set to true if @type == @vocab
//   contextItemPP           to give the caller the complete result of the lookup
//
// RETURN VALUE
//   orionldAltContextItemExpand returns a pointer to the expanded value of 'shortName'
//
// NOTE
//   If no expansion is found, and the default URL has been used, then room is allocated using
//   kaAlloc, allocating on orionldState.kalloc, the connection buffer that lives only during
//   the current request. It is liberated "automatically" when the thread exits.
//
//   If the expansion IS found, then a pointer to the longname (that is part of the context where it was found)
//   is returned and we save some time by not copying anything.
//
char* orionldAltContextItemExpand
(
  OrionldAltContext*      contextP,
  const char*             shortName,
  bool*                   valueMayBeExpandedP,
  bool                    useDefaultUrlIfNotFound,
  OrionldContextItem**    contextItemPP
)
{
  OrionldContextItem* contextItemP;

  LM_TMP(("ALT3: contextP at %p", contextP));
  if (contextP != NULL)
    LM_TMP(("ALT3: context url: %s", contextP->url));

  if (valueMayBeExpandedP != NULL)
    *valueMayBeExpandedP = false;

  if (contextP == NULL)
    contextP = orionldAltCoreContextP;

  if (contextP != NULL)
    LM_TMP(("ALT3: Expanding '%s' in context '%s', at %p", shortName, contextP->url, contextP));
  else
    LM_TMP(("ALT3: Expanding '%s' in context 'NuLL'", shortName));
    
  if (strchr(shortName, ':') != NULL)
  {
    LM_TMP(("PREFIX: Found a ':' before expansion - calling orionldAltContextPrefixExpand"));
    return orionldAltContextPrefixExpand(contextP, shortName);
  }

  // 1. Lookup in Core Context
  LM_TMP(("ALT3: Lookup '%s' in Core Context", shortName));
  contextItemP = orionldAltContextItemLookup(orionldAltCoreContextP, shortName, NULL);
  LM_TMP(("ALT3: contextItemP at %p (after lookup in Core Context)", contextItemP));

  // 2. Lookup in given context (unless it's the Core Context)
  if ((contextItemP == NULL) && (contextP != orionldAltCoreContextP))
  {
    LM_TMP(("ALT3: Lookup '%s' in context %s", shortName, contextP->url));
    contextItemP = orionldAltContextItemLookup(contextP, shortName, NULL);
    LM_TMP(("ALT3: contextItemP at %p (after lookup in Context at %p)", contextItemP, contextP));
  }

  // 3. Use the Default URL (or not!)
  if (contextItemP == NULL)
  {
    LM_TMP(("ALT3: Lookup failed - using Default URL ?"));
    if (useDefaultUrlIfNotFound == true)
    {
      LM_TMP(("ALT3: Lookup failed - using Default URL !"));
      char* longName = (char*) kaAlloc(&orionldState.kalloc, 512);

      snprintf(longName, 512, "%s%s", orionldAltDefaultUrl, shortName);

      if (contextItemPP != NULL)
        *contextItemPP = NULL;

      LM_TMP(("ALT3: Returning '%s' for '%s' (default URL)", longName, shortName));
      return longName;
    }
    else
      return NULL;
  }

  // 4. Save the pointer to the context item
  if (contextItemPP != NULL)
    *contextItemPP = contextItemP;

  // 5. May the value be expanded?
  if ((valueMayBeExpandedP != NULL) && (contextItemP->type != NULL))
  {
    if (strcmp(contextItemP->type, "@vocab") == 0)
      *valueMayBeExpandedP = true;
  }

  if (strchr(contextItemP->id, ':') != NULL)
  {
    LM_TMP(("PREFIX: Found a ':' after expansion - calling orionldAltContextPrefixExpand"));
    return orionldAltContextPrefixExpand(contextP, contextItemP->id);
  }

  return contextItemP->id;
}



// -----------------------------------------------------------------------------
//
// orionldAltContextItemAliasLookup -
//
// PARAMETERS
//
// RETURN VALUE
//
char* orionldAltContextItemAliasLookup
(
  OrionldAltContext*      contextP,
  const char*             longName,
  bool*                   valueMayBeCompactedP,
  OrionldContextItem**    contextItemPP
)
{
  OrionldContextItem* contextItemP;

  // 0. Set output values to false/NULL
  if (valueMayBeCompactedP != NULL)
    *valueMayBeCompactedP = false;

  if (contextItemPP != NULL)
    *contextItemPP = NULL;


  // 1. Is it the default URL?
  if (strncmp(longName, orionldAltDefaultUrl, orionldAltDefaultUrlLen) == 0)
    return (char*) &longName[orionldAltDefaultUrlLen];


  // 2. Found in Core Context?
  contextItemP = orionldAltContextItemValueLookup(orionldAltCoreContextP, longName);


  // 3. If not, look in the provided context, unless it's the Core Context
  if ((contextItemP == NULL) && (contextP != orionldAltCoreContextP))
    contextItemP = orionldAltContextItemValueLookup(contextP, longName);


  // 4. If not found anywhere - return the long name
  if (contextItemP == NULL)
    return (char*) longName;


  // 5. Can the value be compacted?
  if ((valueMayBeCompactedP != NULL) && (contextItemP->type != NULL))
  {
    if (strcmp(contextItemP->type, "@vocab") == 0)
      *valueMayBeCompactedP = true;
  }


  // 6. Give back the pointer to the contextItem, if asked for
  if (contextItemPP != NULL)
    *contextItemPP = contextItemP;

  return contextItemP->name;
}



// -----------------------------------------------------------------------------
//
// prefixCacheLookup -
//
const char* prefixCacheLookup(const char* str)
{
  for (int ix = 0; ix < orionldState.prefixCache.items; ix++)
  {
    if (strcmp(orionldState.prefixCache.cache[ix].prefix, str) == 0)
      return orionldState.prefixCache.cache[ix].expanded;
  }

  return NULL;
}



// -----------------------------------------------------------------------------
//
// prefixCacheInsert -
//
// If the cache is full, then we reuse the oldest
//
void prefixCacheInsert(const char* prefix, const char* expansion)
{
  int                     index = orionldState.prefixCache.index % ORIONLD_PREFIX_CACHE_SIZE;
  OrionldPrefixCacheItem* itemP = &orionldState.prefixCache.cache[index];

  itemP->prefix   = (char*) prefix;
  itemP->expanded = (char*) expansion;

  ++orionldState.prefixCache.index;

  if (orionldState.prefixCache.items < ORIONLD_PREFIX_CACHE_SIZE)
    ++orionldState.prefixCache.items;
}

  

// -----------------------------------------------------------------------------
//
// orionldAltContextPrefixExpand -
//
// This function looks for a ':' inside 'name' and if found, treats what's before the ':' as a prefix.
// This prefix is looked up in the context and if found, the name is expanded, replacing the prefix (and the colon)
// with the value of the context item found in the lookup.
// 
// NOTE
//   * URIs contain ':' but we don't want to expand 'urn', not' http', etc.
//     So, if 'name' starts with 'urn:', or if "://" is found in 'name, then no prefix expansion is performed.
//
//   * Normally, just a few prefixes are used, so a "prefix cache" of 10 values is maintained.
//     This cache is local to the thread, so no semaphores are needed
//
char* orionldAltContextPrefixExpand(OrionldAltContext* contextP, const char* str)
{
  char* colonP;
  char* prefix;
  char* rest;
  char* prefixExpansion;

  // Never expand URNs
  if (SCOMPARE4(str, 'u', 'r', 'n', ':'))
    return (char*) str;

  // Is there a colon in 'str'?   If not, nothing will be replaced
  if ((colonP = strchr((char*) str, ':')) == NULL)
    return (char*) str;

  // Never expand anything xxx://
  if ((colonP[1] == '/') && (colonP[2] == '/'))  // takes care of http:// and https:// and any other "xxx://"
    return (char*) str;

  //
  // "Valid" colon found - need to replace a prefix
  //
  // At this point, 'colonP' points to the ':'
  // The simple parse of 'str' is done, now extract the two parts: 'prefix' and 'rest'
  //
  *colonP = 0;
  prefix  = (char*) str;
  rest    = &colonP[1];

  // Is the prefix in the cache?
  prefixExpansion = (char*) prefixCacheLookup(str);

  // If not, look it up in the context and add it to the cache
  if (prefixExpansion == NULL)
  {
    prefixExpansion = (char*) orionldAltContextItemExpand(contextP, prefix, NULL, false, NULL);
    if (prefixExpansion != NULL)
      prefixCacheInsert(prefix, prefixExpansion);
    else
    {
      //
      // Prefix not found anywhere
      // Fix the brokern 'str' (the colon has been nulled out) and return it
      //
      *colonP = ':';
      return (char*) str;
    }
  }

  // Compose the new string
  int    expandedStringLen = strlen(prefixExpansion) + strlen(rest) + 1;
  char*  expandedString    = (char*) kaAlloc(&orionldState.kalloc, expandedStringLen);

  snprintf(expandedString, expandedStringLen, "%s%s", prefixExpansion, rest);
  return expandedString;
}



// -----------------------------------------------------------------------------
//
// valueExpand -
//
static void valueExpand(KjNode* nodeP)
{
  LM_TMP(("VEX: Expanding '%s' using @context %s", nodeP->value.s, orionldState.altContextP->url));
  nodeP->value.s = orionldAltContextItemExpand(orionldState.altContextP, nodeP->value.s, NULL, true, NULL);
  LM_TMP(("VEX: New value: %s", nodeP->value.s));
}



// -----------------------------------------------------------------------------
//
// orionldAltValueExpand -
//
// The "value" is only expanded if the type of the value is either KjString or KjArray
//
void orionldAltValueExpand(KjNode* attrNodeP)
{
  LM_TMP(("VEX: In orionldValueExpand for attribute '%s'", attrNodeP->name));

  KjNode* valueNodeP = kjLookup(attrNodeP, "value");

  LM_TMP(("VEX: Expanding value of attribute '%s'?", attrNodeP->name));

  if (valueNodeP == NULL)
  {
    LM_TMP(("VEX: No value expansion for %s at no @type was found in @context", attrNodeP->name));
    return;
  }
  else if (valueNodeP->type == KjArray)
  {
    LM_TMP(("VEX: Expanding values of array attribute '%s'", attrNodeP->name));
    for (KjNode* nodeP = valueNodeP->value.firstChildP; nodeP != NULL; nodeP = nodeP->next)
    {
      if (nodeP->type == KjString)
      {
        LM_TMP(("VEX: Expanding Array Item String value of attribute '%s'", attrNodeP->name));
        valueExpand(nodeP);
      }
    }
  }
  else if (valueNodeP->type == KjString)
  {
    LM_TMP(("VEX: Expanding String value of attribute '%s'", attrNodeP->name));
    valueExpand(valueNodeP);
  }
  else
    LM_TMP(("VEX: No value expansion for values of type %s", kjValueType(attrNodeP->type)));
}



// -----------------------------------------------------------------------------
//
// orionldDirectValueExpand - FIXME: this function is not needed - just call orionldAltContextItemExpand
//
char* orionldDirectValueExpand(char* shortName)
{
  return orionldAltContextItemExpand(orionldState.altContextP, shortName, NULL, true, NULL);
}



// -----------------------------------------------------------------------------
//
// orionldContextPrefixExpand -
//
// This function looks for a ':' inside 'name' and if found, treats what's before the ':' as a prefix.
// This prefix is looked up in the context and if found, the name is expanded, replacing the prefix (and the colon)
// with the value of the context item found in the lookup.
// 
// NOTE
//   * URIs contain ':' but we don't want to expand 'urn', not' http', etc.
//     So, if 'name' starts with 'urn:', or if "://" is found in 'name, then no prefix expansion is performed.
//
//   * Normally, just a few prefixes are used, so a "prefix cache" of 10 values is maintained.
//     This cache is local to the thread, so no semaphores are needed
//
char* orionldContextPrefixExpand(OrionldAltContext* contextP, const char* str)
{
  char* colonP;
  char* prefix;
  char* rest;
  char* prefixExpansion;

  // Never expand URNs
  if (SCOMPARE4(str, 'u', 'r', 'n', ':'))
    return (char*) str;

  // Is there a colon in 'str'?   If not, nothing will be replaced
  if ((colonP = strchr((char*) str, ':')) == NULL)
    return (char*) str;

  // Never expand anything xxx://
  if ((colonP[1] == '/') && (colonP[2] == '/'))  // takes care of http:// and https:// and any other "xxx://"
    return (char*) str;

  //
  // "Valid" colon found - need to replace a prefix
  //
  // At this point, 'colonP' points to the ':'
  // The simple parse of 'str' is done, now extract the two parts: 'prefix' and 'rest'
  //
  *colonP = 0;
  prefix  = (char*) str;
  rest    = &colonP[1];

  // Is the prefix in the cache?
  prefixExpansion = (char*) prefixCacheLookup(str);

  // If not, look it up in the context and add it to the cache
  if (prefixExpansion == NULL)
  {
    prefixExpansion = (char*) orionldContextItemExpand(contextP, prefix, NULL, false, NULL);
    if (prefixExpansion != NULL)
      prefixCacheInsert(prefix, prefixExpansion);
    else
    {
      //
      // Prefix not found anywhere
      // Fix the brokern 'str' (the colon has been nulled out) and return it
      //
      *colonP = ':';
      return (char*) str;
    }
  }

  // Compose the new string
  int    expandedStringLen = strlen(prefixExpansion) + strlen(rest) + 1;
  char*  expandedString    = (char*) kaAlloc(&orionldState.kalloc, expandedStringLen);

  snprintf(expandedString, expandedStringLen, "%s%s", prefixExpansion, rest);
  return expandedString;
}



// -----------------------------------------------------------------------------
//
// orionldContextItemExpand -
//
// PARAMETERS
//   contextP                the context
//   shortName               the name to expand
//   valueMayBeExpandedP     pointer to a bool that is set to true if @type == @vocab
//   contextItemPP           to give the caller the complete result of the lookup
//
// RETURN VALUE
//   orionldContextItemExpand returns a pointer to the expanded value of 'shortName'
//
// NOTE
//   If no expansion is found, and the default URL has been used, then room is allocated using
//   kaAlloc, allocating on orionldState.kalloc, the connection buffer that lives only during
//   the current request. It is liberated "automatically" when the thread exits.
//
//   If the expansion IS found, then a pointer to the longname (that is part of the context where it was found)
//   is returned and we save some time by not copying anything.
//
char* orionldContextItemExpand
(
  OrionldAltContext*      contextP,
  const char*             shortName,
  bool*                   valueMayBeExpandedP,
  bool                    useDefaultUrlIfNotFound,
  OrionldContextItem**    contextItemPP
)
{
  OrionldContextItem* contextItemP;

  LM_TMP(("ALT3: contextP at %p", contextP));
  if (contextP != NULL)
    LM_TMP(("ALT3: context url: %s", contextP->url));

  if (valueMayBeExpandedP != NULL)
    *valueMayBeExpandedP = false;

  if (contextP == NULL)
    contextP = orionldCoreContextP;

  if (contextP != NULL)
    LM_TMP(("ALT3: Expanding '%s' in context '%s', at %p", shortName, contextP->url, contextP));
  else
    LM_TMP(("ALT3: Expanding '%s' in context 'NuLL'", shortName));
    
  if (strchr(shortName, ':') != NULL)
  {
    LM_TMP(("PREFIX: Found a ':' before expansion - calling orionldContextPrefixExpand"));
    return orionldContextPrefixExpand(contextP, shortName);
  }

  // 1. Lookup in Core Context
  LM_TMP(("ALT3: Lookup '%s' in Core Context", shortName));
  contextItemP = orionldContextItemLookup(orionldCoreContextP, shortName, NULL);
  LM_TMP(("ALT3: contextItemP at %p (after lookup in Core Context)", contextItemP));

  // 2. Lookup in given context (unless it's the Core Context)
  if ((contextItemP == NULL) && (contextP != orionldCoreContextP))
  {
    LM_TMP(("ALT3: Lookup '%s' in context %s", shortName, contextP->url));
    contextItemP = orionldContextItemLookup(contextP, shortName, NULL);
    LM_TMP(("ALT3: contextItemP at %p (after lookup in Context at %p)", contextItemP, contextP));
  }

  // 3. Use the Default URL (or not!)
  if (contextItemP == NULL)
  {
    LM_TMP(("ALT3: Lookup failed - using Default URL ?"));
    if (useDefaultUrlIfNotFound == true)
    {
      LM_TMP(("ALT3: Lookup failed - using Default URL !"));
      char* longName = (char*) kaAlloc(&orionldState.kalloc, 512);

      snprintf(longName, 512, "%s%s", orionldDefaultUrl, shortName);

      if (contextItemPP != NULL)
        *contextItemPP = NULL;

      LM_TMP(("ALT3: Returning '%s' for '%s' (default URL)", longName, shortName));
      return longName;
    }
    else
      return NULL;
  }

  // 4. Save the pointer to the context item
  if (contextItemPP != NULL)
    *contextItemPP = contextItemP;

  // 5. May the value be expanded?
  if ((valueMayBeExpandedP != NULL) && (contextItemP->type != NULL))
  {
    if (strcmp(contextItemP->type, "@vocab") == 0)
      *valueMayBeExpandedP = true;
  }

  if (strchr(contextItemP->id, ':') != NULL)
  {
    LM_TMP(("PREFIX: Found a ':' after expansion - calling orionldContextPrefixExpand"));
    return orionldContextPrefixExpand(contextP, contextItemP->id);
  }

  return contextItemP->id;
}
