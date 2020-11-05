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

#include "orionld/common/isSpecialSubAttribute.h"                // Own interface



// ----------------------------------------------------------------------------
//
// isSpecialSubAttribute -
//
bool isSpecialSubAttribute(const char* attrName)
{
  if (strcmp(attrName, "createdAt") == 0)
    return true;
  if (strcmp(attrName, "modifiedAt") == 0)
    return true;
  if (strcmp(attrName, "observedAt") == 0)
    return true;
  if (strcmp(attrName, "datasetId") == 0)
    return true;
  if (strcmp(attrName, "instanceId") == 0)
    return true;
  if (strcmp(attrName, "unitCode") == 0)  // Only if type == Property
    return true;

  return false;
}
