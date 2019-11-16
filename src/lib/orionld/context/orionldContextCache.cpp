/*
*
* Copyright 2019 FIWARE Foundation e.V.
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
#include <semaphore.h>                                           // sem_t, sem_init, sem_wait, sem_post

extern "C"
{
#include "kjson/KjNode.h"                                        // KjNode
#include "kjson/kjBuilder.h"                                     // kjString, kjObject, ...
}

#include "logMsg/logMsg.h"                                       // LM_*
#include "logMsg/traceLevels.h"                                  // Lmt*

#include "orionld/common/orionldState.h"                         // orionldState
#include "orionld/context/OrionldContext.h"                      // OrionldContext
#include "orionld/context/OrionldContextItem.h"                  // OrionldContextItem
#include "orionld/context/orionldContextPresent.h"               // orionldContextPresent
#include "orionld/context/orionldContextCache.h"                 // Own interface



// -----------------------------------------------------------------------------
//
// orionldContextCache
//
sem_t             orionldContextCacheSem;
OrionldContext*   orionldContextCacheArray[100];  // When 100 is not enough, a realloc is done (automatically)
OrionldContext**  orionldContextCache         = orionldContextCacheArray;
int               orionldContextCacheSlots    = 100;
int               orionldContextCacheSlotIx   = 0;



// -----------------------------------------------------------------------------
//
// orionldContextCacheInit -
//
void orionldContextCacheInit(void)
{
  bzero(&orionldContextCacheArray, sizeof(orionldContextCacheArray));

  if (sem_init(&orionldContextCacheSem, 0, 1) == -1)
    LM_X(1, ("Runtime Error (error initializing semaphore for orionld context list; %s)", strerror(errno)));
}



// -----------------------------------------------------------------------------
//
// orionldContextCacheGet -
//
KjNode* orionldContextCacheGet(KjNode* arrayP)
{
  for (int ix = 0; ix < orionldContextCacheSlotIx; ix++)
  {
    OrionldContext*  contextP         = orionldContextCacheArray[ix];
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

      for (int slot = 0; slot < ORIONLD_CONTEXT_CACHE_HASH_ARRAY_SIZE; ++slot)
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
// orionldContextCachePresent -
//
void orionldContextCachePresent(const char* prefix, const char* info)
{
  LM_TMP(("%s: *************** %s: %d Contexts *************************", prefix, info, orionldContextCacheSlotIx));
  LM_TMP(("%s: ========================================================================", prefix));
  for (int ix = 0; ix < orionldContextCacheSlotIx; ix++)
  {
    orionldContextPresent(prefix, orionldContextCache[ix]);
    LM_TMP(("%s:", prefix));
  }
  LM_TMP(("%s: ========================================================================", prefix));
}



// -----------------------------------------------------------------------------
//
// orionldContextCacheInsert -
//
void orionldContextCacheInsert(OrionldContext* contextP)
{
  sem_wait(&orionldContextCacheSem);

  //
  // Reallocation necessary?
  //

  if (orionldContextCacheSlotIx >= orionldContextCacheSlots)
  {
    int   slotsToAdd   = 50;
    int   addedSize    = slotsToAdd * sizeof(OrionldContext*);
    int   newNoOfSlots = orionldContextCacheSlots + slotsToAdd;
    char* newArray     = (char*) kaAlloc(&kalloc, sizeof(OrionldContext*) * newNoOfSlots);

    memcpy(newArray, (char*) orionldContextCache, sizeof(OrionldContext*) * orionldContextCacheSlots);
    bzero(&newArray[sizeof(OrionldContext*) * orionldContextCacheSlots], addedSize);

    orionldContextCacheSlots += 50;
    orionldContextCache = (OrionldContext**) newArray;
  }

  orionldContextCache[orionldContextCacheSlotIx] = contextP;
  ++orionldContextCacheSlotIx;

  sem_post(&orionldContextCacheSem);

  if (contextP->keyValues)
    LM_TMP(("CTX: Inserted key-value context '%s' in context-cache - nameHashTable at %p", contextP->url, contextP->context.hash.nameHashTable));
  else
    LM_TMP(("CTX: Inserted array context '%s' in context-cache - %d array items", contextP->url, contextP->context.array.items));
}



// -----------------------------------------------------------------------------
//
// orionldContextCacheLookup -
//
OrionldContext* orionldContextCacheLookup(const char* url)
{
  LM_TMP(("CTX: Looking up context '%s'", url));
  orionldContextCachePresent("CTX", "orionldContextCacheLookup");
  for (int ix = 0; ix < orionldContextCacheSlotIx; ix++)
  {
    if (strcmp(url, orionldContextCache[ix]->url) == 0)
      return orionldContextCache[ix];

    if ((orionldContextCache[ix]->id != NULL) && (strcmp(url, orionldContextCache[ix]->id) == 0))
      return orionldContextCache[ix];
  }

  LM_TMP(("CTX: did not find context '%s'", url));
  return NULL;
}



