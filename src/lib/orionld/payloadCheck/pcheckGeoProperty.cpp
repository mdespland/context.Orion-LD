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
#include <unistd.h>                                               // NULL
#include <string.h>                                               // strcmp

extern "C"
{
#include "kjson/KjNode.h"                                         // KjNode
}

#include "orionld/common/CHECK.h"                                 // DUPLICATE_CHECK
#include "orionld/payloadCheck/pcheckAttribute.h"                 // pcheckAttribute
#include "orionld/payloadCheck/pcheckGeoPropertyValue.h"          // pcheckGeoPropertyValue
#include "orionld/payloadCheck/pcheckGeoProperty.h"               // Own interface



// ----------------------------------------------------------------------------
//
// pcheckGeoProperty -
//
bool pcheckGeoProperty(KjNode* geoPropertyP)
{
  KjNode* valueP = NULL;
  KjNode* typeP  = NULL;

  for (KjNode* nodeP = geoPropertyP->value.firstChildP; nodeP != NULL; nodeP = nodeP->next)
  {
    if (strcmp(nodeP->name, "type") == 0)
    {
      // It's all checked - except for duplicity
      DUPLICATE_CHECK(typeP, "type", nodeP);
    }
    else if (strcmp(nodeP->name, "value") == 0)
    {
      DUPLICATE_CHECK(valueP, "value", nodeP);
      if (pcheckGeoPropertyValue(valueP, NULL, NULL) == false)
        return false;
    }
    else  // Sub-attribute
    {
      if (pcheckAttribute(nodeP, false) == false)
        return false;
    }
  }

  // GeoProperties MUST have a "value"
  if (valueP == NULL)
  {
    orionldErrorResponseCreate(OrionldBadRequestData, "Mandatory field missing", "Property value");
    return false;
  }

  return true;
}
