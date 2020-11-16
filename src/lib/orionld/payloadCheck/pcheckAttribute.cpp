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
#include "kbase/kMacros.h"                                       // K_FT
#include "kjson/KjNode.h"                                        // KjNode
#include "kjson/kjLookup.h"                                      // kjLookup
}

#include "logMsg/logMsg.h"                                       // LM_*
#include "logMsg/traceLevels.h"                                  // Lmt*

#include "orionld/common/isSpecialAttribute.h"                   // isSpecialAttribute
#include "orionld/common/isSpecialSubAttribute.h"                // isSpecialSubAttribute
#include "orionld/common/orionldErrorResponse.h"                 // orionldErrorResponseCreate
#include "orionld/payloadCheck/pcheckSpecialAttribute.h"         // pcheckSpecialAttribute
#include "orionld/payloadCheck/pcheckProperty.h"                 // pcheckProperty
#include "orionld/payloadCheck/pcheckGeoProperty.h"              // pcheckGeoProperty
#include "orionld/payloadCheck/pcheckRelationship.h"             // pcheckRelationship
#include "orionld/payloadCheck/pcheckAttribute.h"                // Own interface



// -----------------------------------------------------------------------------
//
// pcheckAttribute -
//
bool pcheckAttribute(KjNode* aP, bool toplevel)
{
  LM_TMP(("CHECK: attribute             '%s'", aP->name));
  LM_TMP(("CHECK: attribute value type  '%s'", kjValueType(aP->type)));
  LM_TMP(("CHECK: top level             '%s'", K_FT(toplevel)));

  AttributeType  aType      = ATTRIBUTE_ANY;
  KjNode*        typeNodeP  = (aP->type == KjObject)? kjLookup(aP, "type") : NULL;
  bool           special    = (toplevel == true)? isSpecialAttribute(aP->name, &aType, typeNodeP) : isSpecialSubAttribute(aP->name, &aType, typeNodeP);

  LM_TMP(("CHECK: type node %p", typeNodeP));
  LM_TMP(("CHECK: attribute '%s' is %s a special attribute (aType: %s)", aP->name, special? "" : "not", attributeTypeName(aType)));

  if (special == false)  // keyValues ...
  {
    if (aP->type != KjObject)
    {
      orionldErrorResponseCreate(OrionldBadRequestData, "Invalid JSON field type", "attribute must be a JSON Object");
      return false;
    }
  }

  if (aType == ATTRIBUTE_ANY)
  {
    LM_E(("Bad Input (strange attribute '%s' - no type found)", aP->name));
    return false;
  }

  //
  // Check that the attribute is syntactically OK
  //
  if (special == true)
  {
    if (pcheckSpecialAttribute(aP, toplevel, aType) == false)
      return false;
  }
  else
  {
    if (aP->type != KjObject)
    {
      orionldErrorResponseCreate(OrionldBadRequestData, "Invalid JSON field type", "attribute must be a JSON Object");
      return false;
    }
    else if (typeNodeP == NULL)
    {
      orionldErrorResponseCreate(OrionldBadRequestData, "Missing mandatory field", "attribute type");
      return false;
    }
    else if (typeNodeP->type != KjString)
    {
      orionldErrorResponseCreate(OrionldBadRequestData, "Invalid JSON field type", "attribute type must be a JSON String");
      return false;
    }

    switch (aType)
    {
    case ATTRIBUTE_PROPERTY:
      if (pcheckProperty(aP) == false)
        return false;
      break;

    case ATTRIBUTE_GEO_PROPERTY:
      if (pcheckGeoProperty(aP) == false)
        return false;
      break;

    case ATTRIBUTE_RELATIONSHIP:
      if (pcheckRelationship(aP) == false)
        return false;
      break;

    default:
      orionldErrorResponseCreate(OrionldBadRequestData, "Unreachable point?", "Invalid attribute type");
      return false;
    }
  }

  return true;
}
