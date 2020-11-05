/*
*
* Copyright 2018 FIWARE Foundation e.V.
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
#include <string>                                                // string
#include <vector>                                                // vector

extern "C"
{
#include "kbase/kMacros.h"                                       // K_FT
#include "kjson/KjNode.h"                                        // KjNode
#include "kjson/kjLookup.h"                                      // kjLookup
}

#include "logMsg/logMsg.h"                                       // LM_*
#include "logMsg/traceLevels.h"                                  // Lmt*

#include "cache/subCache.h"                                      // CachedSubscription, EntityInfo, subCacheGet, ...

#include "orionld/common/dotForEq.h"                             // dotForEq
#include "orionld/common/orionldState.h"                         // orionldState
#include "orionld/notifications/notificationAdd.h"               // notificationAdd
#include "orionld/notifications/notificationsPrepare.h"          // Own interface



// ----------------------------------------------------------------------------
//
// subEntityMatch -
//
static bool subEntityMatch(char* entityId, char* entityType, const std::vector<EntityInfo*>& entityIdInfos)
{
  for (unsigned int ix = 0; ix < entityIdInfos.size(); ix++)
  {
    EntityInfo* eiP = entityIdInfos[ix];

    if ((eiP->entityId != "") && (eiP->entityId != entityId))
      continue;
    if ((eiP->entityType != "") && (eiP->entityType != entityType))
      continue;

    if (eiP->isPattern == true)
    {
      if (regexec(&eiP->entityIdPattern, entityId, 0, NULL, 0) != 0)
        continue;
    }

    return true;
  }

  return false;
}



// ----------------------------------------------------------------------------
//
// subAttributesMatch -
//
static bool subAttributesMatch(KjNode* patchTree, const std::vector<std::string>& attributes)
{
  if (attributes.size() == 0)
  {
    LM_TMP(("NOTIF: subscription has empty attribute list - accepted"));
    return true;
  }

  for (KjNode* aP = patchTree->value.firstChildP; aP != NULL; aP = aP->next)
  {
    for (unsigned int ix = 0; ix < attributes.size(); ix++)
    {
      char subscriptionAttr[512];

      strncpy(subscriptionAttr, attributes[ix].c_str(), sizeof(subscriptionAttr));
      dotForEq(subscriptionAttr);
      LM_TMP(("NOTIF: Comparing patch-tree attribute '%s' with subscription attribute '%s'", aP->name, subscriptionAttr));
      if (strcmp(aP->name, subscriptionAttr) == 0)
        return true;
    }
  }

  return false;
}



// -----------------------------------------------------------------------------
//
// entityTypeGet -
//
static char* entityTypeGet(KjNode* dbEntityTree)
{
  KjNode* idNodeP = kjLookup(dbEntityTree, "_id");

  if (idNodeP == NULL)
  {
    LM_E(("Database Error (no _id field found in the db entity)"));
    return NULL;
  }

  KjNode* entityTypeNodeP = kjLookup(idNodeP, "type");

  if (entityTypeNodeP == NULL)
  {
    LM_E(("Database Error (no _id.type field found in the db entity)"));
    return NULL;
  }

  if (entityTypeNodeP->type != KjString)
  {
    LM_E(("Database Error (_id.type field found in the db entity but instead of being a string it's of type '%s')", kjValueType(entityTypeNodeP->type)));
    return NULL;
  }

  return entityTypeNodeP->value.s;
}



// ----------------------------------------------------------------------------
//
// notificationsPrepare -
//
void notificationsPrepare(KjNode* dbEntityTree, KjNode* patchTreeCopy)
{
  char* entityType = entityTypeGet(dbEntityTree);

  if (entityType == NULL)
  {
    LM_TMP(("NOTIF: unable to notify - no entity type found in entity from database"));
    return;  // Update OK, notifications FAIL but as errors to log file by entityTypeGet() ... OK sent to client
  }

  LM_TMP(("NOTIF: Entity ID:   %s", orionldState.wildcard[0]));
  LM_TMP(("NOTIF: Entity Type: %s", entityType));

  for (KjNode* aP = patchTreeCopy->value.firstChildP; aP != NULL; aP = aP->next)
  {
    LM_TMP(("NOTIF: Attribute: '%s'", aP->name));
  }

  //
  // Loop over subscription cache to find matching subs
  //
  for (CachedSubscription* cSubP = subCacheGet()->head; cSubP != NULL; cSubP = cSubP->next)
  {
    char* status = (char*) cSubP->status.c_str();

    // <DEBUG>
    LM_TMP(("NOTIF: Subscription %s:",       cSubP->subscriptionId));
    LM_TMP(("NOTIF:   tenant:           %s", cSubP->tenant));
    LM_TMP(("NOTIF:   status:           %s", status));
    LM_TMP(("NOTIF:   entityIdInfos:    %d", cSubP->entityIdInfos.size()));

    // Skip if paused
    if ((*status != 0) && (strcmp(status, "active") != 0))
    {
      LM_TMP(("NOTIF: skipping subscription '%s': status == %s", cSubP->subscriptionId, cSubP->status.c_str()));
      continue;
    }

    // Skip if expired
    if (cSubP->expirationTime < orionldState.requestTime)
    {
      LM_TMP(("NOTIF: skipping subscription '%s': expired (expirationTime:%f < requestTime:%f)", cSubP->subscriptionId, cSubP->expirationTime, orionldState.requestTime));
      continue;
    }

    //
    // Skip if already notified and the throttling time hasn't been reached
    // If this is the first notification for the subscription, throttling isn't considered
    //
    if ((cSubP->throttling != 0) && (cSubP->lastNotificationTime != 0))
    {
      if ((orionldState.requestTime - cSubP->lastNotificationTime) < cSubP->throttling)
      {
        LM_TMP(("NOTIF: skipping subscription '%s': throttling (requestTime:%f - lastNotif:%f == %f   <   throttling:%f)",
                cSubP->subscriptionId,
                orionldState.requestTime,
                cSubP->lastNotificationTime,
                orionldState.requestTime - cSubP->lastNotificationTime,
                cSubP->throttling));
        continue;
      }
    }
    LM_TMP(("NOTIF: not skipping subscription '%s': throttling (lastNotif:%f + throttling:%f (sum: %f) > requestTime:%f)",
            cSubP->subscriptionId,
            cSubP->lastNotificationTime,
            cSubP->throttling,
            cSubP->lastNotificationTime + cSubP->throttling,
            orionldState.requestTime));

    for (unsigned int ix = 0; ix < cSubP->entityIdInfos.size(); ix++)
    {
      LM_TMP(("NOTIF:     entityIdInfo %02d", ix));

      EntityInfo* eiP = cSubP->entityIdInfos[ix];
      LM_TMP(("NOTIF:       entity id:         %s", eiP->entityId.c_str()));
      LM_TMP(("NOTIF:       entity id pattern: %s", K_FT(eiP->isPattern)));
      LM_TMP(("NOTIF:       entity type:       %s", eiP->entityType.c_str()));
    }

    LM_TMP(("NOTIF:   notifyConditions: %d", cSubP->notifyConditionV.size()));
    LM_TMP(("NOTIF:   attributes:       %d", cSubP->attributes.size()));

    for (unsigned int ix = 0; ix < cSubP->attributes.size(); ix++)
      LM_TMP(("NOTIF:     attribute %02d:   %s", ix, cSubP->attributes[ix].c_str()));
    // </DEBUG>

    if (subEntityMatch(orionldState.wildcard[0], entityType, cSubP->entityIdInfos) == false)
    {
      LM_TMP(("NOTIF: skipping subscription '%s': no entity match", cSubP->subscriptionId));
      continue;
    }
    LM_TMP(("NOTIF: Entity ID and/or type is a match"));

    if (subAttributesMatch(patchTreeCopy, cSubP->attributes) == false)
    {
      LM_TMP(("NOTIF: skipping subscription '%s': no attribute match", cSubP->subscriptionId));
      continue;
    }

    // notifyConditionV
    // NGSI-LD Scope

    //
    // All OK - enqueing the notification
    //
    notificationAdd(cSubP, patchTreeCopy, dbEntityTree);
    LM_KTREE("NOTIF: enqueued: ", dbEntityTree);
  }
}
