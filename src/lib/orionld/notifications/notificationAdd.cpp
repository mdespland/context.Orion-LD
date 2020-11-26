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
#include "kalloc/kaAlloc.h"                                      // kaAlloc
#include "kjson/KjNode.h"                                        // KjNode
}

#include "logMsg/logMsg.h"                                       // LM_*
#include "logMsg/traceLevels.h"                                  // Lmt*

#include "cache/subCache.h"                                      // CachedSubscription
#include "orionld/common/orionldState.h"                         // orionldState
#include "orionld/types/OrionldNotification.h"                   // OrionldNotification
#include "orionld/notifications/notificationAdd.h"               // Own interface



// ----------------------------------------------------------------------------
//
// notificationAdd - add a notifiction record
//
void notificationAdd(CachedSubscription* cSubP, KjNode* patchTreeCopy, KjNode* dbEntityTree)
{
  OrionldNotification* niP = (OrionldNotification*) kaAlloc(&orionldState.kalloc, sizeof(OrionldNotification));

  niP->subP       = cSubP;
  niP->changeTree = patchTreeCopy;
  niP->resultTree = dbEntityTree;
  niP->next       = NULL;

  if (orionldState.notificationHead == NULL)
    orionldState.notificationHead = niP;
  else
    orionldState.notificationTail->next = niP;

  orionldState.notificationTail = niP;
}
