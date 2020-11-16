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
#include "kjson/kjLookup.h"                                      // kjLookup
}

#include "logMsg/logMsg.h"                                       // LM_*
#include "logMsg/traceLevels.h"                                  // Lmt*

#include "ngsi/ContextElementResponse.h"                         // ContextElementResponse
#include "orionld/common/orionldState.h"                         // orionldState
#include "orionld/kjTree/kjTreeToContextAttribute.h"             // kjTreeToContextAttribute
#include "orionld/kjTree/kjTreeToContextElementResponse.h"       // Own interface



// -----------------------------------------------------------------------------
//
// kjTreeToContextElementResponse
//
bool kjTreeToContextElementResponse(OrionldContext* contextP, KjNode* kTreeP, ContextElementResponse* cerP)
{
  char buf[2048];

  kjRender(orionldState.kjsonP, kTreeP, buf, sizeof(buf));
  LM_TMP(("NOTIF: tree to become a ContextElementResponse: %s", buf));

  for (KjNode* itemP = kTreeP->value.firstChildP; itemP != NULL; itemP = itemP->next)
  {
    LM_TMP(("NOTIF: First Level Tree Member: '%s'", itemP->name));
    if (strcmp(itemP->name, "id") == 0)
      cerP->contextElement.entityId.id = itemP->value.s;
    else if (strcmp(itemP->name, "type") == 0)
      cerP->contextElement.entityId.type = itemP->value.s;
    else
    {
      ContextAttribute*  caP = new ContextAttribute();
      char*              detail;

      LM_TMP(("NOTIF: Calling kjTreeToContextAttribute for %s", itemP->name));

      caP->name = itemP->name;
      if (kjTreeToContextAttribute(contextP, itemP, caP, NULL, &detail) == true)
        cerP->contextElement.contextAttributeVector.push_back(caP);
      else
      {
        LM_TMP(("NOTIF: ERROR!!! kjTreeToContextAttribute(%s) failed!!! (%s)", itemP->name, detail));
        delete caP;
        return false;
      }
    }
  }

  return true;
}
