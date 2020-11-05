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
#include "orionld/payloadCheck/pcheckSpecialAttribute.h"         // Own interface



// ----------------------------------------------------------------------------
//
// pcheckSpecialAttribute -
//
bool pcheckSpecialAttribute(KjNode* attrNodeP, bool toplevel, char** titleP, char** detailP)
{
  if (strcmp(attrNodeP->name, "createdAt") == 0)
  {
    *titleP  = (char*) "createdAt";
    *detailP = (char*) "Builtin attributes are ignored";
    return false;
  }
  else if (strcmp(attrNodeP->name, "modifiedAt") == 0)
  {
    *titleP  = (char*) "modifiedAt";
    *detailP = (char*) "Builtin attributes are ignored";
    return false;
  }
  else if (strcmp(attrNodeP->name, "datasetId") == 0)
  {
    if (attrNodeP->type != KjString)
    {
      *titleP  = (char*) "Invalid JSON type";
      *detailP = (char*) "datasetId must be a JSON string";
      return false;
    }

    if ((urlCheck(attrNodeP->value.s, detailP) == false) && (urnCheck(attrNodeP->value.s, detailP)))
    {
      *titleP  = (char*) "Not a URI";
      *detailP = (char*) "datasetId must be a URI";
      return false;
    }
  }

  //
  // FIXME: Rest of special sub-attributes:
  // * observedAt
  // * unitCode (only if Property
  //
  // And the special top-level attributes:
  // * location
  // * observationSpace
  // * operationSpace
  //

  return true;
}
