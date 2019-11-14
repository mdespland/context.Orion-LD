#ifndef SRC_LIB_ORIONLD_CONTEXT_ORIONLDALTCONTEXT_H_
#define SRC_LIB_ORIONLD_CONTEXT_ORIONLDALTCONTEXT_H_

/*
*
* Copyright 2018 Telefonica Investigacion y Desarrollo, S.A.U
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
extern "C"
{
#include "khash/khash.h"
#include "kjson/KjNode.h"
}

#include "orionld/common/OrionldProblemDetails.h"       // OrionldProblemDetails



// -----------------------------------------------------------------------------
//
// OrionldAltContextHashTables -
//
typedef struct OrionldAltContextHashTables
{
  KHashTable*  nameHashTable;
  KHashTable*  valueHashTable;
} OrionldAltContextHashTables;



struct OrionldAltContext;
typedef struct OrionldAltContextArray
{
  int                        items;
  struct OrionldAltContext** vector;
} OrionldAltContextArray;



// -----------------------------------------------------------------------------
//
// OrionldAltContextValue
//
typedef union OrionldAltContextValue
{
  OrionldAltContextHashTables  hash;
  OrionldAltContextArray       array;
} OrionldAltContextValue;



// ----------------------------------------------------------------------------
//
// OrionldAltContext -
//
// The context is either an array of contexts or "the real thing" - a list of key-values in
// a hash-list
//
typedef struct OrionldAltContext
{
  char*                  nickname;
  char*                  url;
  char*                  id;         // For contexts that were created by the broker itself
  KjNode*                tree;
  bool                   keyValues;
  bool                   local;      // To Be Removed
  bool                   served;
  OrionldAltContextValue context;
} OrionldAltContext;



// -----------------------------------------------------------------------------
//
// orionldAltCoreContextP -
//
extern OrionldAltContext* orionldAltCoreContextP;



// -----------------------------------------------------------------------------
//
// orionldAltContextCreateFromUrl -
//
extern OrionldAltContext* orionldAltContextCreateFromUrl(const char* url, OrionldProblemDetails* pdP);



// -----------------------------------------------------------------------------
//
// orionldAltContextInit -
//
extern bool orionldAltContextInit(OrionldProblemDetails* pdP);



// -----------------------------------------------------------------------------
//
// orionldAltContextInlineInsert -
//
extern OrionldAltContext* orionldAltContextInlineInsert(KjNode* contextNodeP, OrionldProblemDetails* pdP);



// -----------------------------------------------------------------------------
//
// OrionldContextItem -
//
typedef struct OrionldContextItem
{
  char* name;
  char* id;
  char* type;
} OrionldContextItem;



// ----------------------------------------------------------------------------
//
// orionldAltContextItemLookup - lookup an item in a context
//
extern OrionldContextItem* orionldAltContextItemLookup(OrionldAltContext* contextP, const char* name, bool* valueMayBeCompactedP);



// ----------------------------------------------------------------------------
//
// orionldAltContextItemValueLookup -
//
extern OrionldContextItem* orionldAltContextItemValueLookup(OrionldAltContext* contextP, const char* longname);



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
//   kaAlloc, allocating on orionldState.kallocP, the connection buffer that lives only during
//   the current request. It is liberated "automatically" when the thread exits.
//
//   If the expansion IS found, then a pointer to the longname (that is part of the context where it was found)
//   is returned and we save some time by not copying anything.
//
extern char* orionldAltContextItemExpand
(
  OrionldAltContext*      contextP,
  const char*             shortName,
  bool*                   valueMayBeExpandedP,
  bool                    useDefaultUrlIfNotFound,
  OrionldContextItem**    contextItemPP
);



// -----------------------------------------------------------------------------
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
extern char* orionldAltContextPrefixExpand(OrionldAltContext* contextP, const char* str);



// -----------------------------------------------------------------------------
//
// orionldAltContextPresentTree -
//
extern void orionldAltContextPresentTree(const char* prefix, KjNode* contextNodeP);



// -----------------------------------------------------------------------------
//
// orionldAltContextPresent -
//
extern void orionldAltContextPresent(const char* prefix, OrionldAltContext* contextP);



// -----------------------------------------------------------------------------
//
// orionldAltContextListPresent -
//
extern void orionldAltContextListPresent(const char* prefix, const char* info);



// -----------------------------------------------------------------------------
//
// orionldAltContextLookup -
//
extern OrionldAltContext* orionldAltContextLookup(const char* url);



// -----------------------------------------------------------------------------
//
// orionldAltContextCreateFromTree -
//
extern OrionldAltContext* orionldAltContextCreateFromTree
(
  const char*             url,
  KjNode*                 contextNodeP,
  OrionldProblemDetails*  pdP
);



// -----------------------------------------------------------------------------
//
// orionldAltContextItemAliasLookup -
//
// PARAMETERS
//
// RETURN VALUE
//
extern char* orionldAltContextItemAliasLookup
(
  OrionldAltContext*      contextP,
  const char*             longName,
  bool*                   valueMayBeCompactedP,
  OrionldContextItem**    contextItemPP
);



// -----------------------------------------------------------------------------
//
// orionldAltValueExpand -
//
// The "value" is only expanded if the type of the value is either KjString or KjArray
//
extern void orionldAltValueExpand(KjNode* attrNodeP);



// -----------------------------------------------------------------------------
//
// orionldDirectValueExpand - FIXME: this function is not needed - just call orionldAltContextItemExpand
//
extern char* orionldDirectValueExpand(char* shortName);



// -----------------------------------------------------------------------------
//
// orionldContextArraySimplify - simplify array if possible
//
extern KjNode* orionldContextArraySimplify(KjNode* contextNodeP, const char* url);

#endif  // SRC_LIB_ORIONLD_CONTEXT_ORIONLDALTCONTEXT_H_
