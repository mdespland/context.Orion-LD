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

#include "logMsg/logMsg.h"                                       // LM_*
#include "logMsg/traceLevels.h"                                  // Lmt*

#include "common/globals.h"                                      // parse8601Time

#include "orionld/common/urlCheck.h"                             // urlCheck
#include "orionld/common/urnCheck.h"                             // urnCheck
#include "orionld/common/orionldErrorResponse.h"                 // orionldErrorResponseCreate
#include "orionld/types/AttributeType.h"                         // AttributeType
#include "orionld/payloadCheck/pcheckGeoProperty.h"              // pcheckGeoProperty
#include "orionld/payloadCheck/pcheckTimestamp.h"                // pcheckTimestamp
#include "orionld/payloadCheck/pcheckUrlValue.h"                 // pcheckUrlValue
#include "orionld/payloadCheck/pcheckString.h"                   // pcheckString
#include "orionld/payloadCheck/pcheckSpecialAttribute.h"         // Own interface



// ----------------------------------------------------------------------------
//
// pcheckSpecialAttribute -
//
bool pcheckSpecialAttribute(KjNode* attrNodeP, bool toplevel, AttributeType aType)
{
  if ((aType == ATTRIBUTE_CREATED_AT) || (aType == ATTRIBUTE_MODIFIED_AT))  // Ignored
    return true;

  if (toplevel == true)
  {
    if      (aType == ATTRIBUTE_LOCATION)           return pcheckGeoProperty(attrNodeP);
    else if (aType == ATTRIBUTE_OBSERVATION_SPACE)  return pcheckGeoProperty(attrNodeP);
    else if (aType == ATTRIBUTE_OPERATION_SPACE)    return pcheckGeoProperty(attrNodeP);
    else
    {
      LM_W(("Can't get here (invalid attribute type for a toplevel special attribute)"));
      return false;
    }
  }
  else
  {
    if      (aType == ATTRIBUTE_DATASETID)        return pcheckUrlValue(attrNodeP);
    else if (aType == ATTRIBUTE_OBSERVED_AT)      return pcheckTimestamp(attrNodeP);
    else if (aType == ATTRIBUTE_UNITCODE)         return pcheckString(attrNodeP);
    else
    {
      LM_W(("Can't get here (invalid attribute type for a special attribute)"));
      orionldErrorResponseCreate(OrionldInternalError, "Can't get here?", "invalid attribute type for a special attribute");
      return false;
    }
  }

  return true;
}
