#ifndef SRC_LIB_ORIONLD_CONTEXT_OORIONLDCONTEXT_H_
#define SRC_LIB_ORIONLD_CONTEXT_OORIONLDCONTEXT_H_

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
extern "C"
{
#include "kjson/KjNode.h"
}

#include "orionld/common/OrionldProblemDetails.h"                // OrionldProblemDetails
#include "orionld/context/OrionldContextItem.h"                  // OrionldContextItem
#include "orionld/context/OrionldContext.h"                      // OrionldContext



// -----------------------------------------------------------------------------
//
// orionldContextLookup -
//
extern OrionldContext* orionldContextLookup(const char* url);



// -----------------------------------------------------------------------------
//
// orionldContextFromUrl -
//
extern OrionldContext* orionldContextFromUrl(char* url, OrionldProblemDetails* pdP);



// -----------------------------------------------------------------------------
//
// orionldContextFromTree -
//
extern OrionldContext* orionldContextFromTree(char* url, bool toBeCloned, KjNode* contextTreeP, OrionldProblemDetails* pdP);



// -----------------------------------------------------------------------------
//
// orionldContextFromObject -
//
// If the context object 'contextObjectP' is part of an array, then it's a local context and
// it is not served.
// Served contexts need to be cloned so that they can be copied back to the caller (GET /ngsi-ld/ex/contexts/xxx).
// For example, the URL "http:/x.y.z/contexts/context1.jsonld" was downloaded and its content is a key-value object.
//
extern OrionldContext* orionldContextFromObject(char* url, bool toBeCloned, KjNode* contextObjectP, OrionldProblemDetails* pdP);



// -----------------------------------------------------------------------------
//
// orionldContextFromArray -
//
extern OrionldContext* orionldContextFromArray(char* url, bool toBeCloned, int itemsInArray, KjNode* contextArrayP, OrionldProblemDetails* pdP);



// -----------------------------------------------------------------------------
//
// orionldContextInit -
//
extern bool orionldContextInit(OrionldProblemDetails* pdP);



// -----------------------------------------------------------------------------
//
// orionldValueExpand -
//
// The "value" is only expanded if the type of the value is either KjString or KjArray
//
extern void orionldValueExpand(KjNode* attrNodeP);



// -----------------------------------------------------------------------------
//
// orionldContextPresent -
//
extern void orionldContextPresent(const char* prefix, OrionldContext* contextP);



// -----------------------------------------------------------------------------
//
// orionldContextListPresent -
//
extern void orionldContextListPresent(const char* prefix, const char* info);



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
extern char* orionldContextItemExpand
(
  OrionldContext*      contextP,
  const char*          shortName,
  bool*                valueMayBeExpandedP,
  bool                 useDefaultUrlIfNotFound,
  OrionldContextItem** contextItemPP
);



// ----------------------------------------------------------------------------
//
// orionldContextItemLookup - lookup an item in a context
//
extern OrionldContextItem* orionldContextItemLookup(OrionldContext* contextP, const char* name, bool* valueMayBeCompactedP);



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
extern char* orionldContextPrefixExpand(OrionldContext* contextP, const char* str, char* colonP);



// -----------------------------------------------------------------------------
//
// orionldContextItemAliasLookup -
//
// PARAMETERS
//
// RETURN VALUE
//
extern char* orionldContextItemAliasLookup
(
  OrionldContext*      contextP,
  const char*          longName,
  bool*                valueMayBeCompactedP,
  OrionldContextItem** contextItemPP
);



// ----------------------------------------------------------------------------
//
// orionldContextItemValueLookup - lookup a value in a context
//
extern OrionldContextItem* orionldContextItemValueLookup(OrionldContext* contextP, const char* longname);



// -----------------------------------------------------------------------------
//
// orionldContextCacheGet -
//
extern KjNode* orionldContextCacheGet(KjNode* arrayP);

#endif  // SRC_LIB_ORIONLD_CONTEXT_OORIONLDCONTEXT_H_
