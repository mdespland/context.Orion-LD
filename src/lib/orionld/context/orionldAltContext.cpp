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
#include <semaphore.h>                                           // sem_t, sem_init, sem_wait, sem_post

extern "C"
{
#include "kalloc/kaStrdup.h"                                     // kaStrdup
#include "kalloc/kaAlloc.h"                                      // kaAlloc
#include "kjson/KjNode.h"                                        // KjNode
#include "kjson/kjParse.h"                                       // kjParse
#include "kjson/kjBuilder.h"                                     // kjString, ...
#include "khash/khash.h"                                         // KHashTable
}

#include "logMsg/logMsg.h"                                       // LM_*
#include "logMsg/traceLevels.h"                                  // Lmt*

#include "orionld/common/OrionldProblemDetails.h"                // OrionldProblemDetails
#include "orionld/common/orionldState.h"                         // kalloc
#include "orionld/common/urlCheck.h"                             // urlCheck
#include "orionld/common/OrionldResponseBuffer.h"                // OrionldResponseBuffer
#include "orionld/common/orionldRequestSend.h"                   // orionldRequestSend
#include "orionld/common/uuidGenerate.h"                         // uuidGenerate
#include "orionld/context/orionldCoreContext.h"                  // ORIONLD_CORE_CONTEXT_URL
#include "orionld/context/orionldAltContext.h"                   // Own interface



// -----------------------------------------------------------------------------
//
// hashCode -
//
int hashCode(char* name)
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
int nameCompareFunction(char* name, void* itemP)
{
  OrionldContextItem* cItemP = (OrionldContextItem*) itemP;

  return strcmp(name, cItemP->name);
}



// ----------------------------------------------------------------------------
//
// valueCompareFunction -
//
int valueCompareFunction(char* longname, void* itemP)
{
  OrionldContextItem* cItemP = (OrionldContextItem*) itemP;

  LM_TMP(("ALT: Looking for '%s', comparing with '%s'", longname, cItemP->id));
  return strcmp(longname, cItemP->id);
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
// orionldContextDownload -
//
static char* orionldContextDownload(const char* url, bool* downloadFailedP, OrionldProblemDetails* pdP)
{
  bool ok = false;

  *downloadFailedP = false;

  LM_TMP(("ALT: downloading context '%s'. %d as timeout and %d attempts", url, contextDownloadTimeout, contextDownloadAttempts));
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

    reqOk = orionldRequestSend(&httpResponse, url, contextDownloadTimeout, &pdP->detail, &tryAgain, downloadFailedP, "Accept: application/ld+json");
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
    // detailsPP filled in by orionldRequestSend
    return NULL;
  }

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
// kjChildCount - very special way of counting children - children that are strings and have the value ORIONLD_CORE_CONTEXT_URL don't count
//
static int kjChildCount(KjNode* tree)
{
  int children = 0;

  for (KjNode* childP = tree->value.firstChildP; childP != NULL; childP = childP->next)
  {
    if ((childP->type != KjString) || (strcmp(childP->value.s, ORIONLD_CORE_CONTEXT_URL) != 0))
      ++children;
  }

  return children;
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
// orionldAltContextListPresent -
//
void orionldAltContextListPresent(const char* info)
{
  LM_TMP(("ALT: %s: %d Contexts", info, orionldAltContextListSlotIx));
  LM_TMP(("ALT: ========================================================================"));
  for (int ix = 0; ix < orionldAltContextListSlotIx; ix++)
  {
    LM_TMP(("ALT: Context %03d: %s (%s)", ix, orionldAltContextList[ix]->url, orionldAltContextList[ix]->keyValues? "Key-Values" : "Array"));

    if (orionldAltContextList[ix]->keyValues == false)
    {
      for (int iIx = 0; iIx < orionldAltContextList[ix]->context.array.items; iIx++)
        LM_TMP(("ALT:   Array Item %d: %s (%s)", iIx, orionldAltContextList[ix]->context.array.vector[iIx]->url, orionldAltContextList[ix]->context.array.vector[iIx]->keyValues? "Key-Values" : "Array"));
    }
  }
  LM_TMP(("ALT: ========================================================================"));
}



// -----------------------------------------------------------------------------
//
// orionldAltContextLookup -
//
OrionldAltContext* orionldAltContextLookup(char* url)
{
  for (int ix = 0; ix < orionldAltContextListSlotIx; ix++)
  {
    LM_TMP(("ALT: Looking for '%s', comparing with '%s'", url, orionldAltContextList[ix]->url));
    if (strcmp(url, orionldAltContextList[ix]->url) == 0)
    {
      LM_TMP(("ALT: Found context '%s'", url));
      return orionldAltContextList[ix];
    }
  }

  LM_TMP(("ALT: Did not find context '%s'", url));
  return NULL;
}



// -----------------------------------------------------------------------------
//
// orionldAltContextListInsert -
//
void orionldAltContextListInsert(OrionldAltContext* contextP)
{
  LM_TMP(("ALT: inserting context %s", contextP->url));

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

  orionldAltContextListPresent("AFTER INSERTION");
}



OrionldAltContext* orionldAltCoreContextP = NULL;
// -----------------------------------------------------------------------------
//
// orionldAltContextCreateFromUrl -
//
OrionldAltContext* orionldAltContextCreateFromUrl(char* url, OrionldProblemDetails* pdP)
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

  if ((contextP = orionldAltContextLookup(url)) != NULL)
    return contextP;

  KjNode*  tree;
  KjNode*  contextNodeP;
  char*    buf;
  bool     downloadFailed;

  LM_TMP(("In orionldAltContextCreateFromUrl"));
  if (urlCheck(url, &pdP->detail) == false)
  {
    LM_X(1, ("invalid URL: %s", url));
  }

  //
  // Skip this if Core Context, except in the initialization phase 
  //
  if ((orionldAltCoreContextP != NULL) && (strcmp(url, ORIONLD_CORE_CONTEXT_URL) == 0))
  {
    LM_TMP(("it's the Core Context"));
    return orionldAltCoreContextP;
  }

  if ((buf = orionldContextDownload(url, &downloadFailed, pdP)) == NULL)
  {
    LM_X(1, ("orionldContextDownload(%s) failed", url));
  }

  if ((tree = kjParse(kjsonP, buf)) == NULL)
    LM_X(1, ("error parsing the context"));

  if ((contextNodeP = contextMemberGet(tree, pdP)) == NULL)
  {
    LM_X(1, ("Can't find the value of @context"));
  }

  if (contextNodeP->type == KjString)
  {
    if (strcmp(url, ORIONLD_CORE_CONTEXT_URL) == 0)
    {
      LM_TMP(("the context is a string and it's the core context"));
      return orionldAltCoreContextP;
    }
    else
    {
      LM_TMP(("the context is a string '%s', calling orionldAltContextCreateFromUrl", contextNodeP->value.s));
      return orionldAltContextCreateFromUrl(contextNodeP->value.s, pdP);  // OR: Array of one?
    }
  }
  else if ((contextNodeP->type != KjObject) && (contextNodeP->type != KjArray))
    LM_X(1, ("invalid type of context item: %s", kjValueType(contextNodeP->type)));

  contextP = (OrionldAltContext*) kaAlloc(&kalloc, sizeof(OrionldAltContext));

  contextP->url = kaStrdup(&kalloc, url);

  if (contextNodeP->type == KjObject)
  {
    contextP->keyValues = true;
    contextP->context.hash.nameHashTable  = khashTableCreate(hashCode, nameCompareFunction,  1024);
    contextP->context.hash.valueHashTable = khashTableCreate(hashCode, valueCompareFunction, 1024);

    orionldContextHashTablesFill(&contextP->context.hash, contextNodeP);
  }
  else
  {
    contextP->keyValues              = false;
    contextP->context.array.items    = kjChildCount(tree);  // NOTE: strings that are the Core Context URL DO NOT COUNT !!!
    contextP->context.array.vector   = (OrionldAltContext**) kaAlloc(&kalloc, contextP->context.array.items * sizeof(OrionldAltContext*));

    int ix = contextP->context.array.items - 1;  // NOTE: Insertion starts at the end of the array - the array is sorted backwards!

    for (KjNode* arrayItem = contextNodeP->value.firstChildP; arrayItem != NULL; arrayItem = arrayItem->next)
    {
      if (arrayItem->type == KjString)
      {
        if (strcmp(arrayItem->value.s, ORIONLD_CORE_CONTEXT_URL) == 0)
          continue;
        else
        {
          contextP->context.array.vector[ix] = orionldAltContextLookup(arrayItem->value.s);
          if (contextP->context.array.vector[ix] == NULL)
            contextP->context.array.vector[ix] = orionldAltContextCreateFromUrl(arrayItem->value.s, pdP);
        }
      }
      else if (arrayItem->type == KjObject)
      {
        contextP->keyValues = true;
        contextP->context.array.vector[ix] =	(OrionldAltContext*) kaAlloc(&kalloc, sizeof(OrionldAltContext));
        orionldContextHashTablesFill(&contextP->context.hash, arrayItem);
      }

      --ix;
    }
  }

  LM_TMP(("ALT: Calling orionldAltContextListInsert for %s", contextP->url));
  orionldAltContextListInsert(contextP);

  return contextP;
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



// -----------------------------------------------------------------------------
//
// orionldAltContextInit -
//
bool orionldAltContextInit(OrionldProblemDetails* pdP)
{
  LM_TMP(("ALT: Initializing ALT Context list"));
  orionldAltContextListInit();
  LM_TMP(("ALT: Downloading and processing Core Context"));
  orionldAltCoreContextP = orionldAltContextCreateFromUrl(ORIONLD_CORE_CONTEXT_URL, pdP);

  if (orionldAltCoreContextP == NULL)
  {
    LM_TMP(("ALT: orionldAltContextCreateFromUrl: %s %s", pdP->title, pdP->detail));
    return false;
  }

  LM_TMP(("ALT: orionldAltCoreContextP at %p", orionldAltCoreContextP));
  LM_TMP(("ALT:                      url: %s", orionldAltCoreContextP->url));

  return true;
}



// -----------------------------------------------------------------------------
//
// orionldAltContextInlineInsert -
//
void orionldAltContextInlineInsert(KjNode* contextTree)
{
  if (contextTree->type == KjString)
  {
    LM_TMP(("ALT: inline context is a STRING"));

    LM_TMP(("ALT: Looking up context '%s'", contextTree->value.s));
    if (orionldAltContextLookup(contextTree->value.s) != NULL)
    {
      LM_TMP(("ALT: Found context %s", contextTree->value.s));
      return;
    }
    LM_TMP(("ALT: Context %s was not found", contextTree->value.s));
    orionldAltContextListPresent("NOT FOUND");

    OrionldProblemDetails  pd;
    OrionldAltContext*     contextP = orionldAltContextCreateFromUrl(contextTree->value.s, &pd);

    if (contextP == NULL)
    {
      LM_E(("orionldAltContextCreateFromUrl: %s %s", pd.title, pd.detail));
      return;
    }

    return;
  }
  
  char* url = (char*) kaAlloc(&kalloc, 68 + hostnameLen);  // strlen(http://HOSTNAME:PORT)==12+hostnameLen + strlen("/ngsi-ld/contexts/")==18 + 37 (for uuidGenerate) + 1 (zero termination)
  LM_TMP(("ALT: Adding an inline context to the alternative context list"));

  snprintf(url, 68 + hostnameLen, "http://%s:%d/ngsi-ld/contexts/", hostname, portNo);
  uuidGenerate(&url[30 + hostnameLen]);

  LM_TMP(("ALT: inline context URL: %s", url));

  if (contextTree->type == KjArray)
  {
    LM_TMP(("ALT: inline context is an ARRAY"));
  }
  else if (contextTree->type == KjObject)
  {
    LM_TMP(("ALT: inline context is an OBJECT"));
  }
}



// ----------------------------------------------------------------------------
//
// orionldAltContextItemLookup - lookup an item in a context
//
OrionldContextItem* orionldAltContextItemLookup(OrionldAltContext* contextP, char* name)
{
  OrionldContextItem* itemP = NULL;

  if (contextP->keyValues == true)
    itemP = (OrionldContextItem*) khashItemLookup(contextP->context.hash.nameHashTable, name);
  else
  {
    for (int ix = 0; ix < contextP->context.array.items; ++ix)
    {
      if ((itemP = orionldAltContextItemLookup(contextP->context.array.vector[ix], name)) != NULL)
        break;
    }
  }

  return itemP;
}



// ----------------------------------------------------------------------------
//
// orionldAltContextItemValueLookup - lookup a value in a context
//
// Need a second hash table, to lookup values ... :( 
//
OrionldContextItem* orionldAltContextItemValueLookup(OrionldAltContext* contextP, char* longname)
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
      if ((itemP = orionldAltContextItemLookup(contextP->context.array.vector[ix], longname)) != NULL)
        break;
    }
  }

  return itemP;
}
