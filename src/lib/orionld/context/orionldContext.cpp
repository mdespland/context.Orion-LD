/*
*
* Copyright 2018 FIWARE Foundation e.V.
*
* This file is part of Orion-LD Context Broker.
*
* Orion-LD Context Broker is free software: you can redistribute it and/or
* modify it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* Orion-LD Context Broker is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero
* General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with Orion-LD Context Broker. If not, see http://www.gnu.org/licenses/.
*
* For those usages not covered by this license please contact with
* orionld at fiware dot org
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

#include "orionld/common/OrionldProblemDetails.h"                // OrionldProblemDetails, orionldProblemDetailsFill
#include "orionld/common/SCOMPARE.h"                             // SCOMPAREx
#include "orionld/common/orionldState.h"                         // kalloc
#include "orionld/common/urlCheck.h"                             // urlCheck
#include "orionld/common/OrionldResponseBuffer.h"                // OrionldResponseBuffer
#include "orionld/common/orionldRequestSend.h"                   // orionldRequestSend
#include "orionld/common/uuidGenerate.h"                         // uuidGenerate
#include "orionld/context/OrionldContext.h"                      // OrionldContext
#include "orionld/context/orionldCoreContext.h"                  // orionldCoreContext
#include "orionld/context/orionldContext.h"                      // Temporary Own interface



// -----------------------------------------------------------------------------
//
// ORIONLD_HASH_ARRAY_SIZE -
//
#define ORIONLD_HASH_ARRAY_SIZE   1024



// -----------------------------------------------------------------------------
//
// orionldContextList
//
static sem_t             orionldContextListSem;
static OrionldContext*   orionldContextListArray[100];  // When 100 is not enough, a realloc is done (automatically)
static OrionldContext**  orionldContextList         = orionldContextListArray;
static int               orionldContextListSlots    = 100;
static int               orionldContextListSlotIx   = 0;



// -----------------------------------------------------------------------------
//
// orionldContextCacheGet -
//
KjNode* orionldContextCacheGet(KjNode* arrayP)
{
  for (int ix = 0; ix < orionldContextListSlotIx; ix++)
  {
    OrionldContext*  contextP         = orionldContextListArray[ix];
    KjNode*          contextObjP      = kjObject(orionldState.kjsonP, NULL);
    KjNode*          urlStringP       = kjString(orionldState.kjsonP, "url",  contextP->url);
    KjNode*          idStringP        = kjString(orionldState.kjsonP, "id",  (contextP->id == NULL)? "None" : contextP->id);
    KjNode*          typeStringP      = kjString(orionldState.kjsonP, "type", contextP->keyValues? "hash-table" : "array");

    kjChildAdd(contextObjP, urlStringP);
    kjChildAdd(contextObjP, idStringP);
    kjChildAdd(contextObjP, typeStringP);

    if (contextP->keyValues)
    {
      // Show a maximum of 5 items from the hash-table
      KjNode*      hashTableObjectP = kjObject(orionldState.kjsonP, "hash-table");
      KHashTable*  htP              = contextP->context.hash.nameHashTable;
      int          noOfItems        = 0;

      for (int slot = 0; slot < ORIONLD_HASH_ARRAY_SIZE; ++slot)
      {
        KHashListItem* itemP = htP->array[slot];

        while (itemP != 0)
        {
          OrionldContextItem* hashItemP       = (OrionldContextItem*) itemP->data;
          KjNode*             hashItemStringP = kjString(orionldState.kjsonP, hashItemP->name, hashItemP->id);

          kjChildAdd(hashTableObjectP, hashItemStringP);

          ++noOfItems;
          if (noOfItems >= 5)
            break;

          itemP = itemP->next;
        }

        if (noOfItems >= 5)
          break;
      }

      kjChildAdd(contextObjP, hashTableObjectP);
    }
    else
    {
      //
      // If ARRAY - show all the URLs in the array
      //
      KjNode* urlArrayP = kjArray(orionldState.kjsonP, "URLs");

      for (int aIx = 0; aIx < contextP->context.array.items; ++aIx)
      {
        KjNode* urlStringP = kjString(orionldState.kjsonP, NULL, contextP->context.array.vector[aIx]->url);

        kjChildAdd(urlArrayP, urlStringP);
      }
      kjChildAdd(contextObjP, urlArrayP);
    }

    kjChildAdd(arrayP, contextObjP);
  }

  return arrayP;
}



// -----------------------------------------------------------------------------
//
// orionldContextPresentTree -
//
void orionldContextPresentTree(const char* prefix, KjNode* contextNodeP)
{
  if (contextNodeP->type == KjString)
    LM_TMP(("%s: the context is a String: %s", prefix, contextNodeP->value.s));
  else if (contextNodeP->type == KjArray)
  {
    LM_TMP(("%s: the context is an Array:", prefix));
    for (KjNode* aItemP = contextNodeP->value.firstChildP; aItemP != NULL; aItemP = aItemP->next)
      orionldContextPresentTree(prefix, aItemP);
  }
  else if (contextNodeP->type == KjObject)
  {
    int items = 0;

    LM_TMP(("%s: the context is an Object:", prefix));
    for (KjNode* itemP = contextNodeP->value.firstChildP; itemP != NULL; itemP = itemP->next)
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
// orionldContextPresent -
//
void orionldContextPresent(const char* prefix, OrionldContext* contextP)
{
  if (contextP == NULL)
    return;

  LM_TMP(("    %s: Context '%s' (%s)", prefix, contextP->url, contextP->keyValues? "Key-Values" : "Array"));
  LM_TMP(("    %s: ----------------------------------------------------------------------------", prefix));

  if (contextP->keyValues == true)
  {
    int          noOfItems = 0;
    KHashTable*  htP       = contextP->context.hash.nameHashTable;

    for (int slot = 0; slot < ORIONLD_HASH_ARRAY_SIZE; slot++)
    {
      KHashListItem* itemP = htP->array[slot];

      while (itemP != 0)
      {
        OrionldContextItem* hiP = (OrionldContextItem*) itemP->data;

        LM_TMP(("    %s: key-value[slot %d]: %s -> %s (type: %s)", prefix, slot, hiP->name, hiP->id, hiP->type));
        itemP = itemP->next;
        ++noOfItems;

        if (noOfItems >= 100)
          break;
      }

      if (noOfItems >= 100)
        break;
    }
  }
  else
  {
    for (int iIx = 0; iIx < contextP->context.array.items; iIx++)
    {
      if (contextP->context.array.vector[iIx] == NULL)
      {
        LM_TMP(("    %s:   Array Item %d is not ready", prefix, iIx));
      }
      else
      {
        LM_TMP(("    %s:   Array Item %d: %s (%s)",
                prefix,
                iIx,
                contextP->context.array.vector[iIx]->url,
                contextP->context.array.vector[iIx]->keyValues? "Key-Values" : "Array"));
      }
    }
  }
}



// -----------------------------------------------------------------------------
//
// orionldContextListPresent -
//
void orionldContextListPresent(const char* prefix, const char* info)
{
  LM_TMP(("%s: *************** %s: %d Contexts *************************", prefix, info, orionldContextListSlotIx));
  LM_TMP(("%s: ========================================================================", prefix));
  for (int ix = 0; ix < orionldContextListSlotIx; ix++)
  {
    orionldContextPresent(prefix, orionldContextList[ix]);
    LM_TMP(("%s:", prefix));
  }
  LM_TMP(("%s: ========================================================================", prefix));
}



// -----------------------------------------------------------------------------
//
// orionldContextHashTablesFill -
//
// Fill both the name hashtable and the value hashtable.
//
// The values in the key-value list can be either a string or an object
//
// To support prefix expansion, eventual prefixes in the values must be expanded.
// If not expanded, it's too slow to fich longnamesd in the process of finding aliases.
//
// For example, we have a context with the following items:
//
// @context: {
//   "prefix": "http://a.b.c/mySet/",
//   "P1": "prefix:P1"
// }
//
// Now, an attribute named 'P1' will get the longname "http://a.b.c/mySet/P1".
//
// In a GET process, that's what we get from the database, and we now want to look in the current @context to
// see if we can find a shortname (i.e. a key-value whose value is "http://a.b.c/mySet/P1").
//
// If "P1" == "prefix:P1", we won't find "http://a.b.c/mySet/P1" ...
//
// It could be found, bt parting "prefix:P1" and looking up "prefix", concatenate "http://a.b.c/mySet/" and "P1" and get to
// "http://a.b.c/mySet/P1".
// However, this would be really really slow.
//
// Instead, we expand the values of the context key-values (if the value uses a prefix).
// The problem is that when we treat the first item in a context, that context isn't created yet.
// So, we will need to do this in two passes:
//
// 1. Create the context without expanding prefixes - create only the name-table
// 2. Go over the context again and expand prefixes - create the value-table
//
// The value-table MUST be created in the second pass as its key is the value, and as the value changes in pass II, no other choice.
// Remember that the key (in this case the value is used as key) is what decides the slot in the hash array.
//
static bool orionldContextHashTablesFill(OrionldContext* contextP, KjNode* keyValueTree, OrionldProblemDetails* pdP)
{
  OrionldContextHashTables* hashP           = &contextP->context.hash;
  KHashTable*               nameHashTableP  = hashP->nameHashTable;
  KHashTable*               valueHashTableP = hashP->valueHashTable;

  for (KjNode* kvP = keyValueTree->value.firstChildP; kvP != NULL; kvP = kvP->next)
  {
    OrionldContextItem* hiP = (OrionldContextItem*) kaAlloc(&kalloc, sizeof(OrionldContextItem));

    hiP->name = kaStrdup(&kalloc, kvP->name);
    hiP->type = NULL;

    if (kvP->type == KjString)
      hiP->id = kvP->value.s;  // Will be allocated in pass II
    else if (kvP->type == KjObject)
    {
      hiP->id = NULL;

      //
      // Find @id, @type
      //
      for (KjNode* itemP = kvP->value.firstChildP; itemP != NULL; itemP = itemP->next)
      {
        if (strcmp(itemP->name, "@id") == 0)
          hiP->id = itemP->value.s;  // Will be allocated in pass II
        else if (strcmp(itemP->name, "@type") == 0)
          hiP->type = kaStrdup(&kalloc, itemP->value.s);
      }
    }
    else
    {
      LM_W(("Bad Input (invalid value type for '%s': %s)", kvP->name, kjValueType(kvP->type)));
      orionldProblemDetailsFill(pdP, OrionldBadRequestData, "Invalid key-value in @context", kvP->name, 400);
      return false;
    }

    if ((hiP->id == NULL) || (hiP->id[0] == 0))
    {
      LM_W(("Bad Input (NULL value for key '%s')", kvP->name));

      pdP->type   = OrionldBadRequestData;
      pdP->title  = (char*) "NULL value for key in context";
      pdP->detail = (char*) kvP->name;
      pdP->status = 400;

      return false;
    }

    khashItemAdd(nameHashTableP,  hiP->name, hiP);
  }


  //
  // Second pass, to fix prefix expansion in the values, and to create the valueHashTable
  // In this pass, the 'id' (value) is allocated on the global kalloc instance
  //
  for (int slot = 0; slot < ORIONLD_HASH_ARRAY_SIZE; ++slot)
  {
    KHashListItem* itemP = nameHashTableP->array[slot];

    while (itemP != NULL)
    {
      //
      // Expand if the value contains a colon
      //
      OrionldContextItem* hashItemP = (OrionldContextItem*) itemP->data;
      char*               colonP    = strchr(hashItemP->id, ':');

      if (colonP != NULL)
        hashItemP->id = orionldContextPrefixExpand(contextP, hashItemP->id, colonP);

      hashItemP->id = kaStrdup(&kalloc, hashItemP->id);
      LM_TMP(("HASH: %s -> %s", hashItemP->name, hashItemP->id));

      LM_TMP(("HASHEXPAND: Inserting '%s' in the valueHashTable", hashItemP->id));
      khashItemAdd(valueHashTableP, hashItemP->id, hashItemP);

      itemP = itemP->next;
    }
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// orionldContextInsert -
//
void orionldContextInsert(OrionldContext* contextP)
{
  sem_wait(&orionldContextListSem);

  //
  // Reallocation necessary?
  //

  if (orionldContextListSlotIx >= orionldContextListSlots)
  {
    int   slotsToAdd   = 50;
    int   addedSize    = slotsToAdd * sizeof(OrionldContext*);
    int   newNoOfSlots = orionldContextListSlots + slotsToAdd;
    char* newArray     = (char*) kaAlloc(&kalloc, sizeof(OrionldContext*) * newNoOfSlots);

    memcpy(newArray, (char*) orionldContextList, sizeof(OrionldContext*) * orionldContextListSlots);
    bzero(&newArray[sizeof(OrionldContext*) * orionldContextListSlots], addedSize);

    orionldContextListSlots += 50;
    orionldContextList = (OrionldContext**) newArray;
  }

  orionldContextList[orionldContextListSlotIx] = contextP;
  ++orionldContextListSlotIx;

  sem_post(&orionldContextListSem);

  if (contextP->keyValues)
    LM_TMP(("CTX: Inserted key-value context '%s' in context-cache - nameHashTable at %p", contextP->url, contextP->context.hash.nameHashTable));
  else
    LM_TMP(("CTX: Inserted array context '%s' in context-cache - %d array items", contextP->url, contextP->context.array.items));
}



// -----------------------------------------------------------------------------
//
// orionldContextLookup -
//
OrionldContext* orionldContextLookup(const char* url)
{
  LM_TMP(("CTX: Looking up context '%s'", url));
  orionldContextListPresent("CTX", "orionldContextLookup");
  for (int ix = 0; ix < orionldContextListSlotIx; ix++)
  {
    if (strcmp(url, orionldContextList[ix]->url) == 0)
      return orionldContextList[ix];

    if ((orionldContextList[ix]->id != NULL) && (strcmp(url, orionldContextList[ix]->id) == 0))
      return orionldContextList[ix];
  }

  LM_TMP(("CTX: did not find context '%s'", url));
  return NULL;
}



// -----------------------------------------------------------------------------
//
// httpResponse -
//
static __thread OrionldResponseBuffer  httpResponse;



// -----------------------------------------------------------------------------
//
// orionldContextDownload -
//
char* orionldContextDownload(const char* url, bool* downloadFailedP, OrionldProblemDetails* pdP)
{
  bool reqOk = false;

  *downloadFailedP = true;

  if ((url == NULL || *url == 0))
  {
    pdP->type   = OrionldBadRequestData;
    pdP->title  = (char*) "Invalid @context";
    pdP->detail = (char*) ((url == NULL)? "Null @context" : "Empty @context");
    pdP->status = 400;

    return NULL;
  }
  else if (urlCheck((char*) url, NULL) == false)
  {
    pdP->type   = OrionldBadRequestData;
    pdP->title  = (char*) "Invalid @context";
    pdP->detail = (char*) "Not a URL";
    pdP->status = 400;

    return NULL;
  }

  *downloadFailedP = false;

  LM_TMP(("CTX: Downloading context '%s'", url));
  for (int tries = 0; tries < contextDownloadAttempts; tries++)
  {
    httpResponse.buf       = NULL;  // orionldRequestSend allocates
    httpResponse.size      = 0;
    httpResponse.used      = 0;
    httpResponse.allocated = false;

    //
    // detailsPP is filled in by orionldRequestSend()
    // httpResponse.buf freed by orionldRequestSend() in case of error
    //
    bool tryAgain = false;

    reqOk = orionldRequestSend(&httpResponse, url, contextDownloadTimeout, &pdP->detail, &tryAgain, downloadFailedP, "Accept: application/ld+json");
    if (reqOk == true)
      break;

    LM_E(("CTX: orionldRequestSend failed (try number %d out of %d. Timeout is: %dms): %s", tries + 1, contextDownloadAttempts, contextDownloadTimeout, pdP->detail));
    if (tryAgain == false)
      break;
  }

  if (reqOk == false)
  {
    LM_TMP(("CTX: Download of '%s' failed", url));
    pdP->type   = OrionldLdContextNotAvailable;
    pdP->title  = (char*) "Unable to download context";
    pdP->detail = (char*) url;
    pdP->status = 503;

    return NULL;
  }

  return httpResponse.buf;
}



// -----------------------------------------------------------------------------
//
// orionldContextFromBuffer -
//
OrionldContext* orionldContextFromBuffer(char* url, char* buffer, OrionldProblemDetails* pdP)
{
  LM_TMP(("CTX: preparing context (%s) from buffer", url));

  KjNode* tree = kjParse(kjsonP, buffer);

  if (tree == NULL)
  {
    LM_E(("CTX: kjParse error"));
    pdP->type   = OrionldBadRequestData;
    pdP->title  = (char*) "Parse Error";
    pdP->detail = kjsonP->errorString;
    pdP->status = 400;

    return NULL;
  }

  KjNode* contextNodeP = kjLookup(tree, "@context");
  if (contextNodeP == NULL)
  {
    LM_E(("CTX: No @context field in the context"));
    pdP->type   = OrionldBadRequestData;
    pdP->title  = (char*) "Invalid context";
    pdP->detail = (char*) "No @context field";
    pdP->status = 400;

    return NULL;
  }

  return orionldContextFromTree(url, false, contextNodeP, pdP);
}



// -----------------------------------------------------------------------------
//
// orionldContextFromUrl -
//
OrionldContext* orionldContextFromUrl(char* url, OrionldProblemDetails* pdP)
{
  OrionldContext* contextP = orionldContextLookup(url);

  LM_TMP(("CTX: Getting context from a URL: '%s'", url));

  if (contextP != NULL)
    return contextP;

  bool  downloadFailed;
  char* buffer = orionldContextDownload(url, &downloadFailed, pdP);  // downloadFailed not used ... remove?

  if (buffer == NULL)
  {
    // orionldContextDownload fills in pdP
    LM_W(("Bad Input? (%s: %s)", pdP->title, pdP->detail));
    return NULL;
  }

  return orionldContextFromBuffer(url, buffer, pdP);
}



// -----------------------------------------------------------------------------
//
// orionldContextSimplify - simplify the context, if possible
//
// Only for arrays.
// Any string in the array, that is the Core Context, is removed from the array
// If after processing, there is only ONE item in the array, that item is returned
// as the context, regardless of its type.
//
// If on the other hand the array becomes empty after removel of Core Context, the NULL is returned.
// The caller needs to check for this.
//
KjNode* orionldContextSimplify(KjNode* contextTreeP, int* itemsInArrayP)
{
  LM_TMP(("CTX: Simplifying context tree"));

  if (contextTreeP->type != KjArray)
    return contextTreeP;

  KjNode* nodeP       = contextTreeP->value.firstChildP;
  int    itemsInArray = 0;

  while (nodeP != NULL)
  {
    KjNode* next = nodeP->next;

    if ((nodeP->type == KjString) && (strcmp(nodeP->value.s, ORIONLD_CORE_CONTEXT_URL) == 0))
    {
      LM_TMP(("CTX: Removed Core Context from Array"));
      kjChildRemove(contextTreeP, nodeP);
    }
    else
      ++itemsInArray;

    nodeP = next;
  }

  if (itemsInArrayP != 0)
    *itemsInArrayP = itemsInArray;

  if (itemsInArray == 0)
  {
    LM_TMP(("CTX: array resulted EMPTY - returning NULL"));
    return NULL;
  }
  else if (itemsInArray == 1)
  {
    LM_TMP(("CTX: array with only one item. Type is: %s", kjValueType(contextTreeP->type)));
    return contextTreeP->value.firstChildP;
  }

  return contextTreeP;
}



// -----------------------------------------------------------------------------
//
// orionldContextFromTree -
//
OrionldContext* orionldContextFromTree(char* url, bool toBeCloned, KjNode* contextTreeP, OrionldProblemDetails* pdP)
{
  int itemsInArray;

  LM_TMP(("CTX: Creating context from Tree. url: %s (at %p) - tree of type %s", url, url, kjValueType(contextTreeP->type)));

  if (contextTreeP->type == KjArray)
  {
    contextTreeP = orionldContextSimplify(contextTreeP, &itemsInArray);
    if (contextTreeP == NULL)
    {
      LM_TMP(("CTX: got an array with only Core Context ..."));

      pdP->type   = OrionldBadRequestData;
      pdP->title  = (char*) "Empty @context";
      pdP->detail = (char*) "got an array with only Core Context";
      pdP->status = 200;

      return NULL;
    }
  }

  if      (contextTreeP->type == KjString)   return orionldContextFromUrl(contextTreeP->value.s, pdP);
  else if (contextTreeP->type == KjObject)   return orionldContextFromObject(url, toBeCloned, contextTreeP, pdP);
  else if (contextTreeP->type == KjArray)    return orionldContextFromArray(url, toBeCloned, itemsInArray, contextTreeP, pdP);

  //
  // None of the above. Error
  //
  pdP->type   = OrionldBadRequestData;
  pdP->title  = (char*) "Invalid type for item in @context array";
  pdP->detail = (char*) kjValueType(contextTreeP->type);
  pdP->status = 400;

  LM_E(("CTX: %s: %s", pdP->title, pdP->detail));
  return NULL;
}



// -----------------------------------------------------------------------------
//
// orionldContextCreate -
//
OrionldContext* orionldContextCreate(const char* url, const char* id, KjNode* tree, bool keyValues, bool toBeCloned)
{
  LM_TMP(("CTX: Creating empty context object"));

  OrionldContext* contextP = (OrionldContext*) kaAlloc(&kalloc, sizeof(OrionldContext));

  contextP->url       = kaStrdup(&kalloc, url);
  contextP->id        = (id == NULL)? NULL : kaStrdup(&kalloc, id);
  contextP->tree      = (toBeCloned == true)? kjClone(tree) : NULL;
  contextP->keyValues = keyValues;

  return contextP;
}



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

  LM_TMP(("LOCA: Looking for name '%s', comparing with '%s'", name, cItemP->name));
  return strcmp(name, cItemP->name);
}



// ----------------------------------------------------------------------------
//
// valueCompareFunction -
//
int valueCompareFunction(const char* longname, void* itemP)
{
  OrionldContextItem* cItemP = (OrionldContextItem*) itemP;

  LM_TMP(("LCA: Looking for value '%s', comparing with '%s'", longname, cItemP->id));
  return strcmp(longname, cItemP->id);
}



// -----------------------------------------------------------------------------
//
// orionldContextUrlGenerate -
//
// The size used in the call to kaAlloc:
//   - strlen("http://HOSTNAME:PORT"):       12
//   - strlen("/ngsi-ld/ex/v1/contexts/"):   24
//   - uuidGenerate:                         37
//   - zero termination:                     1
//   - orionldHostNameLen
//
//  => 74 + orionldHostNameLen
//
char* orionldContextUrlGenerate(char** contextIdP)
{
  char* url = (char*) kaAlloc(&kalloc, 74 + orionldHostNameLen);

  snprintf(url, 74 + orionldHostNameLen, "http://%s:%d/ngsi-ld/ex/v1/contexts/", orionldHostName, portNo);
  uuidGenerate(&url[36 + orionldHostNameLen]);

  *contextIdP = &url[36 + orionldHostNameLen];

  return url;
}



// -----------------------------------------------------------------------------
//
// orionldContextFromObject -
//
// If the context object 'contextObjectP' is part of an array, then it's a local context and
// it is not served.
// Served contexts need to be cloned so that they can be copied back to the caller (GET /ngsi-ld/ex/contexts/xxx).
// For example, the URL "http:/x.y.z/contexts/context1.jsonld" was downloaded and its content is a key-value object.
//
OrionldContext* orionldContextFromObject(char* url, bool toBeCloned, KjNode* contextObjectP, OrionldProblemDetails* pdP)
{
  char*           id = NULL;
  OrionldContext* contextP;

  LM_TMP(("CTX: Creating hash tables for context '%s'", url));

  if (url == NULL)
    url  = orionldContextUrlGenerate(&id);

  LM_TMP(("CTX: Calling orionldContextCreate. url: '%s' (at %p)", url, url));
  contextP = orionldContextCreate(url, id, contextObjectP, true, toBeCloned);
  orionldContextInsert(contextP);

  contextP->context.hash.nameHashTable  = khashTableCreate(&kalloc, hashCode, nameCompareFunction,  ORIONLD_HASH_ARRAY_SIZE);
  contextP->context.hash.valueHashTable = khashTableCreate(&kalloc, hashCode, valueCompareFunction, ORIONLD_HASH_ARRAY_SIZE);

  if (orionldContextHashTablesFill(contextP, contextObjectP, pdP) == false)
  {
    // orionldContextHashTablesFill fills in pdP
    return NULL;
  }

  orionldContextListPresent("CTX", "After orionldContextFromObject");

  return contextP;
}



// -----------------------------------------------------------------------------
//
// orionldContextFromArray -
//
OrionldContext* orionldContextFromArray(char* url, bool toBeCloned, int itemsInArray, KjNode* contextArrayP, OrionldProblemDetails* pdP)
{
  char*           id = NULL;
  OrionldContext* contextP;

  LM_TMP(("CTX: Creating array for context '%s'", url));

  if (url == NULL)
    url = orionldContextUrlGenerate(&id);

  contextP = orionldContextCreate(url, id, contextArrayP, false, toBeCloned);
  orionldContextInsert(contextP);

  contextP->context.array.items    = itemsInArray;
  contextP->context.array.vector   = (OrionldContext**) kaAlloc(&kalloc, itemsInArray * sizeof(OrionldContext*));


  //
  // Valid types for members of a context array are:   KjString and KjObject
  // I could probably easily allow KjArray also.
  //
  // If KjString, the url must be looked up first, then orionldContextFromUrl is called
  // If KjObject, the object is made a "standalone" vontext and replaced in the KjNode tree by a string (its URL)
  //
  int slot = itemsInArray - 1;  // NOTE: Insertion starts at the end of the array - the array is sorted backwards

  LM_TMP(("CTX: ================================ orionldContextFromArray Start =================================================================================="));
  LM_TMP(("CTX: Starting with slot %d. array at %p", slot, contextArrayP));
  for (KjNode* arrayItemP = contextArrayP->value.firstChildP; arrayItemP != NULL; arrayItemP = arrayItemP->next)
  {
    OrionldContext* childContextP;

    LM_TMP(("CTX: current slot: %d, item type is: '%s'", slot, kjValueType(arrayItemP->type)));

    if (arrayItemP->type == KjString)
    {
      LM_TMP(("CTX: current slot: %d, KjString: %s", slot, arrayItemP->value.s));
      childContextP = orionldContextLookup(arrayItemP->value.s);
      if (childContextP == NULL)
        childContextP = orionldContextFromUrl(arrayItemP->value.s, pdP);
      if (childContextP == NULL)
      {
        // orionldContextFromUrl fills in pdP
        LM_E(("CTX: orionldContextFromUrl: %s: %s", pdP->title, pdP->detail));
        return NULL;
      }
    }
    else if (arrayItemP->type == KjObject)
    {
      childContextP = orionldContextFromObject(NULL, false, arrayItemP, pdP);

      if (childContextP == NULL)
      {
        // orionldContextFromObject fills in pdP
        LM_E(("CTX: orionldContextFromObject: %s: %s", pdP->title, pdP->detail));
        return NULL;
      }
    }
    else
    {
      LM_E(("CTX: invalid type of @context array item: %s", kjValueType(arrayItemP->type)));
      pdP->type   = OrionldBadRequestData;
      pdP->title  = (char*) "Invalid @context - invalid type for @context array item";
      pdP->detail = (char*) kjValueType(arrayItemP->type);
      pdP->status = 400;

      return NULL;
    }

    contextP->context.array.vector[slot] = childContextP;
    --slot;
  }

  LM_TMP(("CTX: ===================================== orionldContextFromArray End ==============================================================================="));
  orionldContextListPresent("CTX", "After orionldContextFromArray");
  return contextP;
}



// -----------------------------------------------------------------------------
//
// orionldContextListInit -
//
static void orionldContextListInit(void)
{
  bzero(&orionldContextListArray, sizeof(orionldContextListArray));

  if (sem_init(&orionldContextListSem, 0, 1) == -1)
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
    pdP->type   = OrionldBadRequestData;
    pdP->title  = (char*) "Invalid @context";
    pdP->detail = (char*) "empty context file (or, only whitespace)";
    pdP->status = 400;

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
    pdP->type   = OrionldBadRequestData;
    pdP->title  = (char*) "Invalid @context";
    pdP->detail = (char*) "can't find the end of the URL line";
    pdP->status = 400;

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
    pdP->type   = OrionldBadRequestData;
    pdP->title  = (char*) "Invalid @context";
    pdP->detail = (char*) "no JSON Context found";
    pdP->status = 400;

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

  OrionldContext* contextP = orionldContextFromBuffer(url, json, &pd);

  if (strcmp(url, ORIONLD_CORE_CONTEXT_URL) == 0)
  {
    if (contextP == NULL)
      LM_X(1, ("error creating the core context from file system file '%s'", path));
    orionldCoreContextP = contextP;
  }
  else
  {
    if (contextP == NULL)
      LM_E(("CTX: error creating context from file system file '%s'", path));
    else
    {
      LM_TMP(("CTX: Calling orionldContextInsert for %s", contextP->url));
      orionldContextInsert(contextP);
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
// orionldContextInit -
//
bool orionldContextInit(OrionldProblemDetails* pdP)
{
  LM_TMP(("CTX: Initializing ALT Context list"));
  orionldContextListInit();

  char* cacheContextDir = getenv("ORIONLD_CACHED_CONTEXT_DIRECTORY");
  bool  gotCoreContext  = false;

#if DEBUG
  if (cacheContextDir != NULL)
  {
    LM_TMP(("CTX: Getting initial contexts from '%s'", cacheContextDir));
    gotCoreContext = fileSystemContexts(cacheContextDir);
    if (gotCoreContext == false)
      LM_E(("Unable to cache pre-loaded contexts from '%s'", cacheContextDir));
    orionldContextListPresent("INIT", "After loading initial cached contexts");
  }
#endif

  if (gotCoreContext == false)
  {
    LM_TMP(("CTX: Downloading and processing Core Context"));
    orionldCoreContextP = orionldContextFromUrl(ORIONLD_CORE_CONTEXT_URL, pdP);

    if (orionldCoreContextP == NULL)
    {
      LM_TMP(("CTX: orionldContextCreateFromUrl: %s %s", pdP->title, pdP->detail));
      return false;
    }
  }

  OrionldContextItem* vocabP = orionldContextItemLookup(orionldCoreContextP, "@vocab", NULL);

  if (vocabP == NULL)
  {
    LM_E(("CTX: Context Error (no @vocab item found in Core Context)"));
    orionldDefaultUrl    = (char*) "https://example.org/ngsi-ld/default/";
  }
  else
    orionldDefaultUrl = vocabP->id;

  orionldDefaultUrlLen = strlen(orionldDefaultUrl);

  return true;
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
  OrionldContext*       contextP,
  const char*           shortName,
  bool*                 valueMayBeExpandedP,
  bool                  useDefaultUrlIfNotFound,
  OrionldContextItem**  contextItemPP
)
{
  OrionldContextItem* contextItemP;
  char*               colonP;

  LM_TMP(("EXPAND:"));
  LM_TMP(("EXPAND: looking for '%s' in context %s", shortName, contextP? contextP->url : "Core Context"));

  if (valueMayBeExpandedP != NULL)
    *valueMayBeExpandedP = false;

  if (contextP == NULL)
    contextP = orionldCoreContextP;

  if ((colonP = strchr((char*) shortName, ':')) != NULL)
  {
    char* longName = orionldContextPrefixExpand(contextP, shortName, colonP);
    LM_TMP(("EXPAND: '%s' -> '%s'", shortName, longName));
    return longName;
  }

  // 1. Lookup in Core Context
  contextItemP = orionldContextItemLookup(orionldCoreContextP, shortName, NULL);
  if (contextItemP != NULL)
    LM_TMP(("EXPAND: Found '%s' -> '%s' in Core Context", shortName, contextItemP->id));

  // 2. Lookup in given context (unless it's the Core Context)
  if ((contextItemP == NULL) && (contextP != orionldCoreContextP))
  {
    contextItemP = orionldContextItemLookup(contextP, shortName, NULL);
    if (contextItemP != NULL)
      LM_TMP(("EXPAND: Found '%s' -> '%s' in User Context: '%s'", shortName, contextItemP->id, contextP->url));
  }

  // 3. Use the Default URL (or not!)
  if (contextItemP == NULL)
  {
    if (useDefaultUrlIfNotFound == true)
    {
      int   shortNameLen = strlen(shortName);
      int   longNameLen  = orionldDefaultUrlLen + shortNameLen + 1;
      char* longName     = (char*) kaAlloc(&orionldState.kalloc, longNameLen);

      snprintf(longName, longNameLen, "%s%s", orionldDefaultUrl, shortName);

      if (contextItemPP != NULL)
        *contextItemPP = NULL;

      LM_TMP(("EXPAND: Found '%s' -> '%s' via Default URL", shortName, longName));
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

  return contextItemP->id;
}



// -----------------------------------------------------------------------------
//
// valueExpand -
//
static void valueExpand(KjNode* nodeP)
{
  nodeP->value.s = orionldContextItemExpand(orionldState.contextP, nodeP->value.s, NULL, true, NULL);
}



// -----------------------------------------------------------------------------
//
// orionldValueExpand -
//
// The "value" is only expanded if the type of the value is either KjString or KjArray
//
void orionldValueExpand(KjNode* attrNodeP)
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



// ----------------------------------------------------------------------------
//
// orionldContextItemLookup - lookup an item in a context
//
OrionldContextItem* orionldContextItemLookup(OrionldContext* contextP, const char* name, bool* valueMayBeCompactedP)
{
  OrionldContextItem* itemP = NULL;

  if (contextP == NULL)
    contextP = orionldCoreContextP;

  if (contextP->keyValues == true)
    itemP = (OrionldContextItem*) khashItemLookup(contextP->context.hash.nameHashTable, name);
  else
  {
    for (int ix = 0; ix < contextP->context.array.items; ++ix)
    {
      if ((itemP = orionldContextItemLookup(contextP->context.array.vector[ix], name, valueMayBeCompactedP)) != NULL)
        break;
    }
  }

  if (valueMayBeCompactedP != NULL)
  {
    if ((itemP->type != NULL) && (strcmp(itemP->type, "@vocab") == 0))
      *valueMayBeCompactedP = true;
    else
      *valueMayBeCompactedP = false;
  }

  return itemP;
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
char* orionldContextPrefixExpand(OrionldContext* contextP, const char* str, char* colonP)
{
  char* prefix;
  char* rest;
  char* prefixExpansion;

  // Never expand URNs
  if (SCOMPARE4(str, 'u', 'r', 'n', ':'))
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
      // Fix the broken 'str' (the colon has been nulled out) and return it
      //
      *colonP = ':';
      return (char*) str;
    }
  }

  // Compose the new string
  int    expandedStringLen = strlen(prefixExpansion) + strlen(rest) + 1;
  char*  expandedString    = (char*) kaAlloc(&orionldState.kalloc, expandedStringLen);

  snprintf(expandedString, expandedStringLen, "%s%s", prefixExpansion, rest);


  //
  // Before returning - fix the broken 'str' (the colon has been nulled out)
  //
  *colonP = ':';

  return expandedString;
}



void debugHashValue(const char* prefix, const char* name)
{
  OrionldContextItem* cItemP = orionldContextItemLookup(orionldState.contextP, name, NULL);

  if (cItemP == NULL)
    LM_TMP(("%s: '%s' not found", prefix, name));
  else
    LM_TMP(("%s: '%s' == '%s' (OrionldContextItem::id at %p)", prefix, name, cItemP->id, cItemP->id));
}



// -----------------------------------------------------------------------------
//
// orionldContextItemAliasLookup -
//
// PARAMETERS
//
// RETURN VALUE
//
char* orionldContextItemAliasLookup
(
  OrionldContext*       contextP,
  const char*           longName,
  bool*                 valueMayBeCompactedP,
  OrionldContextItem**  contextItemPP
)
{
  OrionldContextItem* contextItemP;

  LM_TMP(("ALIAS: looking for long-name (in values) '%s'", longName));

  // 0. Set output values to false/NULL
  if (valueMayBeCompactedP != NULL)
    *valueMayBeCompactedP = false;

  if (contextItemPP != NULL)
    *contextItemPP = NULL;


  // 1. Is it the default URL?
  LM_TMP(("ALIAS: looking for long-name '%s' - is it the default URL (%s)?", longName, orionldDefaultUrl));
  if (strncmp(longName, orionldDefaultUrl, orionldDefaultUrlLen) == 0)
  {
    LM_TMP(("ALIAS: looking for long-name '%s' - it was the default URL - returning shortname '%s'", longName, &longName[orionldDefaultUrlLen]));
    return (char*) &longName[orionldDefaultUrlLen];
  }

  // 2. Found in Core Context?
  LM_TMP(("ALIAS: looking for long-name '%s' in the core context '%s'", longName, orionldCoreContextP->url));
  contextItemP = orionldContextItemValueLookup(orionldCoreContextP, longName);
  if (contextItemP != NULL)
    LM_TMP(("ALIAS: looking for long-name '%s' - found in the Core Context (shortname: %s)", longName, contextItemP->name));

  // 3. If not, look in the provided context, unless it's the Core Context
  if ((contextItemP == NULL) && (contextP != orionldCoreContextP))
  {
    LM_TMP(("ALIAS: looking for long-name '%s' in user provided context '%s'", longName, contextP->url));
    contextItemP = orionldContextItemValueLookup(contextP, longName);
    if (contextItemP != NULL)
      LM_TMP(("ALIAS: looking for long-name '%s' - found in user provided context '%s' (shortname: %s)", longName, contextP->url, contextItemP->name));
  }

  // 4. If not found anywhere - return the long name
  if (contextItemP == NULL)
  {
    LM_TMP(("ALIAS: looking for long-name '%s' - NOT FOUND - returning the long name", longName));
    return (char*) longName;
  }

  LM_TMP(("ALIAS: looking for long-name '%s' - FOUND - returning the shortname '%s'", longName, contextItemP->name));

  // 5. Can the value be compacted?
  if ((valueMayBeCompactedP != NULL) && (contextItemP->type != NULL))
  {
    if (strcmp(contextItemP->type, "@vocab") == 0)
      *valueMayBeCompactedP = true;
  }


  // 6. Give back the pointer to the contextItem, if asked for
  if (contextItemPP != NULL)
    *contextItemPP = contextItemP;

  // Return the short name
  return contextItemP->name;
}



// ----------------------------------------------------------------------------
//
// orionldContextItemValueLookup - lookup a value in a context
//
OrionldContextItem* orionldContextItemValueLookup(OrionldContext* contextP, const char* longname)
{
  OrionldContextItem* itemP = NULL;

  LM_TMP(("ALIAS: Looking for value '%s' in context '%s'", longname, contextP->url));

  if (contextP->keyValues == true)
  {
    LM_TMP(("ALIAS: Context is an Object (Hashed key-values): calling khashItemLookup"));
    itemP = (OrionldContextItem*) khashItemLookup(contextP->context.hash.valueHashTable, longname);
    if (itemP != NULL)
      LM_TMP(("ALIAS: Found %s: %s", longname, itemP->name));
  }
  else
  {
    LM_TMP(("ALIAS: Context is an Array"));
    for (int ix = 0; ix < contextP->context.array.items; ++ix)
    {
      LM_TMP(("ALIAS: Recursive call to orionldContextItemValueLookup for context '%s'", contextP->context.array.vector[ix]->url));
      if ((itemP = orionldContextItemValueLookup(contextP->context.array.vector[ix], longname)) != NULL)
      {
        LM_TMP(("ALIAS: Found %s: %s", longname, itemP->name));
        break;
      }
    }
  }

  return itemP;
}
