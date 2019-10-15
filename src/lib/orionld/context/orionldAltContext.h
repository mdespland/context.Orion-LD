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
#include "orionld/context/OrionldContext.h"             // OrionldAltContext
#include "orionld/common/OrionldProblemDetails.h"       // OrionldProblemDetails



// -----------------------------------------------------------------------------
//
// orionldAltCoreContextP -
//
extern OrionldAltContext* orionldAltCoreContextP;



// -----------------------------------------------------------------------------
//
// orionldAltContextCreateFromUrl -
//
extern OrionldAltContext* orionldAltContextCreateFromUrl(char* url, OrionldProblemDetails* pdP);



// -----------------------------------------------------------------------------
//
// orionldAltContextInit -
//
extern bool orionldAltContextInit(OrionldProblemDetails* pdP);



// -----------------------------------------------------------------------------
//
// orionldAltContextInlineInsert -
//
extern void orionldAltContextInlineInsert(KjNode* contextTree);



// -----------------------------------------------------------------------------
//
// OrionldContextItem - Change name to ... OrionldContextItem
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
extern OrionldContextItem* orionldAltContextItemLookup(OrionldAltContext* contextP, char* name);



// ----------------------------------------------------------------------------
//
// orionldAltContextItemValueLookup -
//
extern OrionldContextItem* orionldAltContextItemValueLookup(OrionldAltContext* contextP, char* longname);

#endif  // SRC_LIB_ORIONLD_CONTEXT_ORIONLDALTCONTEXT_H_
