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
extern "C"
{
#include "kjson/KjNode.h"                                        // KjNode
}

#include "logMsg/logMsg.h"                                       // LM_*
#include "logMsg/traceLevels.h"                                  // Lmt*

#include "common/globals.h"                                      // parse8601Time
#include "orionld/common/CHECK.h"                                // STRING_CHECK
#include "orionld/common/urlCheck.h"                             // urlCheck
#include "orionld/common/urnCheck.h"                             // urnCheck
#include "orionld/common/orionldErrorResponse.h"                 // orionldErrorResponseCreate
#include "orionld/payloadCheck/pcheckUrlValue.h"                 // Own interface



// ----------------------------------------------------------------------------
//
// pcheckUrlValue -
//
bool pcheckUrlValue(KjNode* attrNodeP)
{
  STRING_CHECK(attrNodeP, attrNodeP->name);

  char* detail;
  if ((urlCheck(attrNodeP->value.s, &detail) == false) && (urnCheck(attrNodeP->value.s, &detail)))
  {
    orionldErrorResponseCreate(OrionldBadRequestData, "Not a URI", attrNodeP->name);
    return false;
  }

  return true;
}



