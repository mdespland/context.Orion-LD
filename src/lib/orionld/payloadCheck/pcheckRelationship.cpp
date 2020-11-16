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
#include <string.h>                                              // strcmp

extern "C"
{
#include "kjson/KjNode.h"                                        // KjNode
}

#include "orionld/common/urlCheck.h"                             // urlCheck
#include "orionld/common/urnCheck.h"                             // urnCheck
#include "orionld/common/CHECK.h"                                // DUPLICATE_CHECK, STRING_CHECK, ...
#include "orionld/common/orionldErrorResponse.h"                 // orionldErrorResponseCreate
#include "orionld/payloadCheck/pcheckAttribute.h"                // pcheckAttribute
#include "orionld/payloadCheck/pcheckRelationship.h"             // Own interface



// -----------------------------------------------------------------------------
//
// pcheckRelationship -
//
bool pcheckRelationship(KjNode* attrNodeP)
{
  KjNode* typeP    = NULL;
  KjNode* objectP  = NULL;

  LM_TMP(("CHECK: In pcheckRelationship"));

  for (KjNode* nodeP = attrNodeP->value.firstChildP; nodeP != NULL; nodeP = nodeP->next)
  {
    if (strcmp(nodeP->name, "type") == 0)
    {
      // It's all checked - except for duplicity
      DUPLICATE_CHECK(typeP, "type", nodeP);
    }
    else if (strcmp(nodeP->name, "object") == 0)
    {
      DUPLICATE_CHECK(objectP, "object", nodeP);
    }
    else  // Sub-attribute
    {
      if (pcheckAttribute(nodeP, false) == false)
        return false;
    }
  }

  // Relationships MUST have an "object"
  if (objectP == NULL)
  {
    orionldErrorResponseCreate(OrionldBadRequestData, "Mandatory field missing", "Mandatory field missing: Relationship object");
    return false;
  }

  // "object" must be a string that is a valid URI
  if (objectP->type != KjString)
  {
    orionldErrorResponseCreate(OrionldBadRequestData, "Invalid JSON Type for Relationship object", kjValueType(objectP->type));
    return false;
  }

  char* detail;
  if ((urlCheck(objectP->value.s, &detail) == false) && (urnCheck(objectP->value.s, &detail) == false))
  {
    orionldErrorResponseCreate(OrionldBadRequestData, "Not a URI", "The object field of a Relationship must be a valid URI");
    return false;
  }

  return true;
}
