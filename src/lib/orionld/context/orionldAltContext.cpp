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
#include "kjson/KjNode.h"                                        // KjNode
#include "kjson/kjParse.h"                                       // kjParse
#include "kjson/kjBuilder.h"                                     // kjString, ...
#include "khash/khash.h"                                         // KHashTable
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
#include "orionld/context/orionldCoreContext.h"                  // ORIONLD_CORE_CONTEXT_URL
#include "orionld/context/orionldAltContext.h"                   // Own interface



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
    // detailsPP filled in by orionldRequestSend
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
OrionldAltContext* orionldAltContextLookup(const char* url)
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

  orionldAltContextListPresent(url);

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



// -----------------------------------------------------------------------------
//
// orionldAltCoreContextP - The Core Context
//
OrionldAltContext* orionldAltCoreContextP   = NULL;
char*              orionldAltDefaultUrl     = NULL;
int                orionldAltDefaultUrlLen  = 1000;  // To provoke errors is not properly set



// -----------------------------------------------------------------------------
//
// orionldAltContextCreateFromTree -
//
OrionldAltContext* orionldAltContextCreateFromTree(const char* url, KjNode* contextNodeP, OrionldProblemDetails* pdP)
{
  OrionldAltContext* contextP;

  LM_TMP(("HASH: ***** In orionldAltContextCreateFromTree (url: %s)", url));

  if (contextNodeP->type == KjString)
  {
    if ((url != NULL) && (strcmp(url, ORIONLD_CORE_CONTEXT_URL) == 0))
    {
      LM_TMP(("the context is a string and it's the core context"));
      return orionldAltCoreContextP;
    }
    else if ((contextP = orionldAltContextLookup(contextNodeP->value.s)) != NULL)
    {
      LM_TMP(("ALT: Found context %s", contextNodeP->value.s));
      return contextP;
    }
    else
    {
      LM_TMP(("the context is a string '%s', calling orionldAltContextCreateFromUrl", contextNodeP->value.s));
      return orionldAltContextCreateFromUrl(contextNodeP->value.s, pdP);  // OR: Array of one?
    }
  }
  else if ((contextNodeP->type != KjObject) && (contextNodeP->type != KjArray))
    LM_X(1, ("invalid type of context item: %s", kjValueType(contextNodeP->type)));

  contextP      = (OrionldAltContext*) kaAlloc(&kalloc, sizeof(OrionldAltContext));
  contextP->url = (url != NULL)? kaStrdup(&kalloc, url) : NULL;

  if (contextNodeP->type == KjObject)
  {
    contextP->keyValues = true;

    contextP->context.hash.nameHashTable  = khashTableCreate(&kalloc, hashCode, nameCompareFunction,  1024);
    contextP->context.hash.valueHashTable = khashTableCreate(&kalloc, hashCode, valueCompareFunction, 1024);

    LM_TMP(("HASH: nameHashTable  at %p", contextP->context.hash.nameHashTable));
    LM_TMP(("HASH: valueHashTable at %p", contextP->context.hash.valueHashTable));
    LM_TMP(("HASH: Calling orionldContextHashTablesFill"));
    orionldContextHashTablesFill(&contextP->context.hash, contextNodeP);
  }
  else  // Array
  {
    contextP->keyValues              = false;
    contextP->context.array.items    = kjChildCount(contextNodeP);  // NOTE: strings that are the Core Context URL DO NOT COUNT !!!
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
//        contextP->context.array.vector[ix] = orionldAltContextCreateFromTree("NO URL", arrayItem, pdP);        
        contextP->context.array.vector[ix] = orionldAltContextInlineInsert(arrayItem, pdP);
      --ix;
    }
  }

  LM_TMP(("ALT: Calling orionldAltContextListInsert for %s", contextP->url));
  orionldAltContextListInsert(contextP);

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

  if ((contextP = orionldAltContextLookup(url)) != NULL)
    return contextP;

  char*    buf;
  bool     downloadFailed;

  LM_TMP(("In orionldAltContextCreateFromUrl"));
  if (urlCheck((char*) url, &pdP->detail) == false)
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

  if ((buf = orionldAltContextDownload(url, &downloadFailed, pdP)) == NULL)
  {
    LM_X(1, ("orionldAltContextDownload(%s) failed", url));
  }

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

  OrionldAltContext*     contextP = orionldAltContextCreateFromBuffer(url, json, &pd);

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
      orionldAltContextListInsert(contextP);
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

#if DEBUG
  if (cacheContextDir != NULL)
  {
    LM_TMP(("ALT: Getting initial contexts from '%s'", cacheContextDir));
    gotCoreContext = fileSystemContexts(cacheContextDir);
    if (gotCoreContext == false)
      LM_E(("Unable to cache pre-loaded contexts from '%s'", cacheContextDir));
    orionldAltContextListPresent("After loading initial cached contexts");
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

  OrionldContextItem* vocabP = orionldAltContextItemLookup(orionldAltCoreContextP, "@vocab");

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
    orionldAltContextListPresent("NOT FOUND");

    contextP = orionldAltContextCreateFromUrl(contextNodeP->value.s, pdP);

    if (contextP == NULL)
      LM_E(("orionldAltContextCreateFromUrl: %s %s", pdP->title, pdP->detail));

    return contextP;
  }
  
  char* url = (char*) kaAlloc(&kalloc, 68 + orionldHostNameLen);  // strlen(http://HOSTNAME:PORT)==12+orionldHostNameLen + strlen("/ngsi-ld/contexts/")==18 + 37 (for uuidGenerate) + 1 (zero termination)
  LM_TMP(("ALT: Adding an inline context to the alternative context list"));

  snprintf(url, 68 + orionldHostNameLen, "http://%s:%d/ngsi-ld/contexts/", orionldHostName, portNo);
  uuidGenerate(&url[30 + orionldHostNameLen]);

  return orionldAltContextCreateFromTree(url, contextNodeP, pdP);
}



// ----------------------------------------------------------------------------
//
// orionldAltContextItemLookup - lookup an item in a context
//
OrionldContextItem* orionldAltContextItemLookup(OrionldAltContext* contextP, const char* name)
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
    LM_TMP(("ALT3: Context is key-values"));
    itemP = (OrionldContextItem*) khashItemLookup(contextP->context.hash.nameHashTable, name);
  }
  else
  {
    LM_TMP(("ALT3: Context is an Array, no key-values"));
    for (int ix = 0; ix < contextP->context.array.items; ++ix)
    {
      LM_TMP(("ALT3: recursive call for context %s", contextP->context.array.vector[ix]->url));
      if ((itemP = orionldAltContextItemLookup(contextP->context.array.vector[ix], name)) != NULL)
        break;
    }
  }

  LM_TMP(("ALT3: %s '%s' in context '%s'", itemP? "Found" : "Didn't find", name, contextP->url));
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
      if ((itemP = orionldAltContextItemLookup(contextP->context.array.vector[ix], longname)) != NULL)
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
    
  // 1. Lookup in Core Context
  LM_TMP(("ALT3: Lookup '%s' in Core Context", shortName));
  contextItemP = orionldAltContextItemLookup(orionldAltCoreContextP, shortName);
  LM_TMP(("ALT3: contextItemP at %p (after lookup in Core Context)", contextItemP));

  // 2. Lookup in given context (unless it's the Core Context)
  if ((contextItemP == NULL) && (contextP != orionldAltCoreContextP))
  {
    LM_TMP(("ALT3: Lookup '%s' in context %p", shortName, contextP));
    contextItemP = orionldAltContextItemLookup(contextP, shortName);
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

  // 6. Return the long name
  LM_TMP(("ALT3: Returning '%s' for '%s' (found in context)", contextItemP->id, shortName));
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
const char* orionldAltContextItemAliasLookup
(
  OrionldAltContext*      contextP,
  const char*             longName,
  bool*                   valueMayBeContractedP,
  OrionldContextItem**    contextItemPP
)
{
  OrionldContextItem* contextItemP;

  // 0. Set output values to false/NULL
  if (valueMayBeContractedP != NULL)
    *valueMayBeContractedP = false;

  if (contextItemPP != NULL)
    *contextItemPP = NULL;


  // 1. Is it the default URL?
  if (strncmp(longName, orionldAltDefaultUrl, orionldAltDefaultUrlLen) == 0)
    return &longName[orionldAltDefaultUrlLen];


  // 2. Found in Core Context?
  contextItemP = orionldAltContextItemValueLookup(orionldAltCoreContextP, longName);


  // 3. If not, look in the provided context, unless it's the Core Context
  if ((contextItemP == NULL) && (contextP != orionldAltCoreContextP))
    contextItemP = orionldAltContextItemValueLookup(contextP, longName);


  // 4. If not found anywhere - return the long name
  if (contextItemP == NULL)
    return longName;


  // 5. Can the value be contracted?
  if ((valueMayBeContractedP != NULL) && (contextItemP->type != NULL))
  {
    if (strcmp(contextItemP->type, "@vocab") == 0)
      *valueMayBeContractedP = true;
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
// This function looks for a ':' inside 'name' and if found, treats what's before rthe ':' as a prefix.
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
const char* orionldAltContextPrefixExpand(OrionldAltContext* contextP, const char* str)
{
  char* colonP;
  char* prefix;
  char* rest;
  char* prefixExpansion;

  // Never expand URNs
  if (SCOMPARE4(str, 'u', 'r', 'n', ':'))
    return str;

  // Is there a colon in 'str'?   If not, nothing will be replaced
  if ((colonP = strchr((char*) str, ':')) == NULL)
    return str;

  // Never expand anything xxx://
  if ((colonP[1] == '/') && (colonP[2] == '/'))  // takes care of http:// and https:// and any other "xxx://"
    return str;

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
      return str;
    }
  }

  // Compose the new string
  int    expandedStringLen = strlen(prefixExpansion) + strlen(rest) + 1;
  char*  expandedString    = (char*) kaAlloc(&orionldState.kalloc, expandedStringLen);

  snprintf(expandedString, expandedStringLen, "%s%s", prefixExpansion, rest);
  return expandedString;
}
