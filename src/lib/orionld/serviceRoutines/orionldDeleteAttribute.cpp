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
* Author: Ken Zangelin and Gabriel Quaresma
*/
#include <string>                                                // std::string  - for servicePath only
#include <vector>                                                // std::vector  - for servicePath only

extern "C"
{
#include "kbase/kMacros.h"                                       // K_VEC_SIZE, K_FT
#include "kjson/kjBuilder.h"                                     // kjChildRemove
#include "kjson/kjRender.h"                                      // kjRender
#include "kjson/kjLookup.h"                                      // kjLookup
#include "kjson/kjClone.h"                                       // kjClone
#include "kalloc/kaAlloc.h"                                      // kaAlloc
#include "kalloc/kaStrdup.h"                                     // kaStrdup
}

#include "logMsg/logMsg.h"                                       // LM_*
#include "logMsg/traceLevels.h"                                  // Lmt*

#include "rest/ConnectionInfo.h"                                 // ConnectionInfo
#include "rest/HttpStatusCode.h"                                 // SccContextElementNotFound

#include "orionld/common/orionldErrorResponse.h"                 // orionldErrorResponseCreate
#include "orionld/common/urlCheck.h"                             // urlCheck
#include "orionld/common/urnCheck.h"                             // urnCheck
#include "orionld/common/httpStatusCodeToOrionldErrorType.h"     // httpStatusCodeToOrionldErrorType
#include "orionld/common/orionldState.h"                         // orionldState
#include "orionld/common/dotForEq.h"                             // dotForEq
#include "orionld/common/eqForDot.h"                             // eqForDot
#include "orionld/db/dbConfiguration.h"                          // dbEntityAttributeLookup, dbEntityAttributesDelete
#include "orionld/context/orionldContextItemExpand.h"            // orionldContextItemExpand
#include "orionld/serviceRoutines/orionldDeleteAttribute.h"      // Own Interface



// ----------------------------------------------------------------------------
//
// orionldDeleteAttribute -
//
bool orionldDeleteAttribute(ConnectionInfo* ciP)
{
  char*    entityId = orionldState.wildcard[0];
  char*    details;
  char*    attrNameP;

  // Make sure the Entity ID is a valid URI
  if ((urlCheck(entityId, &details) == false) && (urnCheck(entityId, &details) == false))
  {
    orionldErrorResponseCreate(OrionldBadRequestData, "Invalid Entity ID", details);
    orionldState.httpStatusCode = SccBadRequest;
    return false;
  }

  if (dbEntityLookup(entityId) == NULL)
  {
    orionldErrorResponseCreate(OrionldResourceNotFound, "The requested entity has not been found. Check its id", entityId);
    orionldState.httpStatusCode = SccNotFound;  // 404
    return false;
  }

  if ((strncmp(orionldState.wildcard[1], "http://", 7) == 0) || (strncmp(orionldState.wildcard[1], "https://", 8) == 0))
    attrNameP = orionldState.wildcard[1];
  else
    attrNameP = orionldContextItemExpand(orionldState.contextP, orionldState.wildcard[1], NULL, true, NULL);

  if (dbEntityAttributeLookup(entityId, attrNameP) == NULL)
  {
    orionldState.httpStatusCode = SccContextElementNotFound;
    orionldErrorResponseCreate(OrionldBadRequestData, "Attribute Not Found", orionldState.wildcard[1]);
    return false;
  }

  LM_T(LmtServiceRoutine, ("Deleting attribute '%s' of entity '%s'", orionldState.wildcard[1], entityId));

  int     size           = 1;
  KjNode* attrObjectP    = kjObject(orionldState.kjsonP, NULL);
  KjNode* attrToRemoveP  = kjObject(orionldState.kjsonP, attrNameP);

  kjChildAdd(attrObjectP, attrToRemoveP);

  // Create a single array of the attribute passed.
  char** attrNameV  = (char**) kaAlloc(&orionldState.kalloc, size * sizeof(char*));
  int    attrNameIx = 0;

  // Save attribute long name in attrNameV, also - replace all '.' for '='
  for (KjNode* attrP = attrObjectP->value.firstChildP; attrP != NULL; attrP = attrP->next)
  {
    LM_TMP(("attrP => %s", attrP->name));
    attrNameV[attrNameIx] = attrP->name;
    dotForEq(attrNameV[attrNameIx]);
    ++attrNameIx;
  }

  if (dbEntityAttributesDelete(entityId, attrNameV, size) == false)
  {
    orionldState.httpStatusCode = SccContextElementNotFound;
    orionldErrorResponseCreate(OrionldBadRequestData, "Attribute Not Found", orionldState.wildcard[1]);
    return false;
  }

  orionldState.httpStatusCode = SccNoContent;
  return true;

}
