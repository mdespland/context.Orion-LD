/*
*
* Copyright 2020 FIWARE Foundation e.V.
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
#include <postgresql/libpq-fe.h>                               // PGconn

extern "C"
{
#include "kjson/KjNode.h"                                      // KjNode
#include "kjson/kjLookup.h"                                    // kjLookup
#include "kjson/kjBuilder.h"                                   // kjChildRemove
#include "kjson/kjRender.h"                                    // kjRender
}

#include "logMsg/logMsg.h"                                     // LM_*
#include "logMsg/traceLevels.h"                                // Lmt*

#include "orionld/common/orionldState.h"                       // orionldState
#include "orionld/common/uuidGenerate.h"                       // uuidGenerate

#include "orionld/temporal/pgEntityPush.h"                     // pgEntityPush
#include "orionld/temporal/pgAttributeTreat.h"                 // pgAttributeTreat
#include "orionld/temporal/pgEntityTreat.h"                    // Own interface



// -----------------------------------------------------------------------------
//
// pgEntityTreat -
//
bool pgEntityTreat(PGconn* connectionP, KjNode* entityP, char* id, char* type, char* createdAt, char* modifiedAt)
{
  // <DEBUG>
  char buf[1024];
  kjRender(orionldState.kjsonP, entityP, buf, sizeof(buf));
  LM_TMP(("TEMP: entityP: %s", buf));
  // </DEBUG>

  if (id == NULL)  // Find the entity id in the entity tree
  {
    KjNode* nodeP = kjLookup(entityP, "id");

    if (nodeP == NULL)
      LM_X(1, ("Entity without id"));

    id = nodeP->value.s;
    kjChildRemove(entityP, nodeP);
  }


  if (type == NULL)  // Find the entity type in the entity tree
  {
    KjNode* nodeP = kjLookup(entityP, "type");

    if (nodeP == NULL)
      LM_X(1, ("Entity without type"));

    type = nodeP->value.s;
    kjChildRemove(entityP, nodeP);
  }

  char instanceId[64];
  uuidGenerate(instanceId);

  LM_TMP(("Calling pgEntityPush(%p, '%s', '%s', '%s', '%s', '%s')", connectionP, instanceId, id, type, createdAt, modifiedAt));
  if (pgEntityPush(connectionP, instanceId, id, type, createdAt, modifiedAt) == false)
    LM_RE(false, ("pgEntityPush failed"));

  for (KjNode* attrP = entityP->value.firstChildP; attrP != NULL; attrP = attrP->next)
  {
    // FIXME: createdAt ... I need to know that the Attribute did not exist for this to be OK ...
    if (pgAttributeTreat(connectionP, attrP, instanceId, id, createdAt, modifiedAt) == false)
      LM_RE(false, ("pgAttributeTreat failed for attribute '%s'", attrP->name));
  }

  return true;
}