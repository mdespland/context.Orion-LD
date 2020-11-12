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
#include <string>                                                // std::string
#include <vector>                                                // std::vector

extern "C"
{
#include "kalloc/kaStrdup.h"                                     // kaStrdup
#include "kjson/KjNode.h"                                        // KjNode
#include "kjson/kjLookup.h"                                      // kjLookup
#include "kjson/kjBuilder.h"                                     // kjChildRemove, kjChildAdd
#include "kjson/kjRender.h"                                      // kjRender
#include "kjson/kjClone.h"                                       // kjClone
}

#include "logMsg/logMsg.h"                                       // LM_*
#include "logMsg/traceLevels.h"                                  // Lmt*

#include "rest/ConnectionInfo.h"                                 // ConnectionInfo
#include "mongoBackend/mongoUpdateContext.h"                     // mongoUpdateContext

#include "orionld/common/orionldErrorResponse.h"                 // orionldErrorResponseCreate
#include "orionld/common/orionldState.h"                         // orionldState
#include "orionld/common/SCOMPARE.h"                             // SCOMPAREx
#include "orionld/common/CHECK.h"                                // DUPLICATE_CHECK, STRING_CHECK, ...
#include "orionld/common/dotForEq.h"                             // dotForEq
#include "orionld/common/attributeUpdated.h"                     // attributeUpdated
#include "orionld/common/attributeNotUpdated.h"                  // attributeNotUpdated
#include "orionld/common/isSpecialAttribute.h"                   // isSpecialAttribute
#include "orionld/db/dbConfiguration.h"                          // dbEntityLookup, dbEntityUpdate
#include "orionld/context/orionldContextItemExpand.h"            // orionldContextItemExpand
#include "orionld/kjTree/kjTreeToContextAttribute.h"             // kjTreeToContextAttribute
#include "orionld/kjTree/kjStringValueLookupInArray.h"           // kjStringValueLookupInArray
#include "orionld/kjTree/kjTreeToUpdateContextRequest.h"         // kjTreeToUpdateContextRequest
#include "orionld/payloadCheck/pcheckAttribute.h"                // pcheckAttribute
#include "orionld/notifications/notificationsPrepare.h"          // notificationsPrepare
#include "orionld/serviceRoutines/orionldPatchEntity.h"          // Own Interface



// ----------------------------------------------------------------------------
//
// orionldPatchEntity -
//
// mongoBackend doesn't work for this request.
// I have managed to get the DB update fully working, except for createdAt, that is updated when it should not be,
// But, any notifications are sent with a mix between the old and the new. See Orion-LD issue #559 for more info.
//
// So, I have decided to NOT USE mongoBackend for PATCH Entity.
//
// This is how it will work:
// 1. Get the entity from the DB - let's call it the "DB-Entity tree".
// 2. Loop over the incoming payload KjNode tree (patch tree) and remove from the patch tree all those attributes that do not exist in the "DB-Entity tree"
// 3. Overwrite the "DB-Entity tree" with all remaining attributes from the "Patch Tree".
// - Now we have the resulting entity in the "DB-Entity tree"
// 4. Write the "DB-Entity tree" to mongo
// 5. Find matching subscriptions for the "Patch Tree"
// 6. Prepare the notifications - to be sent in requestCompleted (src/lib/rest/rest.cpp)
//
// "datasetId" will be implemented later.
//
// ----------------------------------------------------------------------------
//
// The input payload is a collection of attributes.
// Those attributes that don't exist already in the entity are not added, but reported in the response payload data as "notUpdated".
// The remaining attributes replace the old attributes.
//
// Extract from ETSI NGSI-LD spec:
//   For each of the Attributes included in the Fragment, if the target Entity includes a matching one (considering
//   term expansion rules as mandated by clause 5.5.7), then replace it by the one included by the Fragment. If the
//   Attribute includes a datasetId, only an Attribute instance with the same datasetId is replaced.
//   In all other cases, the Attribute shall be ignored.
//
bool orionldPatchEntity(ConnectionInfo* ciP)
{
  char*    entityId      = orionldState.wildcard[0];
  KjNode*  patchTree     = orionldState.requestTree;
  KjNode*  dbEntityTree;
  char*    title;
  char*    detail;

  // Entity ID must be a valid URI
  if ((urlCheck(entityId, &detail) == false) && (urnCheck(entityId, &detail) == false))
  {
    orionldState.httpStatusCode = 400;
    orionldErrorResponseCreate(OrionldBadRequestData, "Entity ID must be a valid URI", entityId);
    return false;
  }

  // The payload must be a JSON object
  OBJECT_CHECK(orionldState.requestTree, kjValueType(orionldState.requestTree->type));

  // Get the entity from mongo
  if ((dbEntityTree = dbEntityLookup(entityId)) == NULL)
  {
    orionldState.httpStatusCode = 404;
    orionldErrorResponseCreate(OrionldBadRequestData, "Entity does not exist", entityId);
    return false;
  }

  // <DEBUG>
  char buf[2048];
  kjRender(orionldState.kjsonP, dbEntityTree, buf, sizeof(buf));
  LM_TMP(("dbEntityTree: %s", buf));

  kjRender(orionldState.kjsonP, patchTree, buf, sizeof(buf));
  LM_TMP(("patchTree: %s", buf));
  // </DEBUG>

  //
  // Set pointers to the DB attribute array (attrNames) and object (attrs)
  //
  KjNode* dbAttrs      = kjLookup(dbEntityTree, "attrs");
  KjNode* dbAttrNames  = kjLookup(dbEntityTree, "attrNames");

  if (dbAttrNames == NULL)
  {
    LM_E(("Database Error ('attrNames' field not found the the entity in the database)"));
    orionldState.httpStatusCode = 500;
    orionldErrorResponseCreate(OrionldBadRequestData, "Database Error", "'attrNames' field not found the the entity in the database");
    return false;
  }

  if (dbAttrs == NULL)
  {
    LM_E(("Database Error ('attrs' field not found the the entity in the database)"));
    orionldState.httpStatusCode = 500;
    orionldErrorResponseCreate(OrionldBadRequestData, "Database Error", "'attrs' field not found the the entity in the database");
    return false;
  }

  //
  // Remove from patchTree all attributes that are incorrect
  // Removed attributes are stored in the array 'notUpdatedP' to be a part of the response
  //
  KjNode* updatedP     = kjArray(orionldState.kjsonP, "updated");
  KjNode* notUpdatedP  = kjArray(orionldState.kjsonP, "notUpdated");
  KjNode* aP           = patchTree->value.firstChildP;
  KjNode* next;

  while (aP != NULL)
  {
    next = aP->next;

    char shortName[512];
    strncpy(shortName, aP->name, sizeof(shortName));

    if (pcheckAttribute(aP, true, &title, &detail) == false)
    {
      attributeNotUpdated(notUpdatedP, shortName, detail);
      kjChildRemove(patchTree, aP);
      aP = next;
      continue;
    }

    aP = next;
  }

  kjRender(orionldState.kjsonP, patchTree, buf, sizeof(buf));
  LM_TMP(("New patchTree: %s", buf));


  //
  // Overwrite the "DB-Entity tree" with all remaining attributes from the "Patch Tree".
  // Keep the "Patch Tree" intact as it is needed for finding matching subscriptions
  // FIXME: Match subs before merging "Patch Tree" into "DB-Entity tree" to avoid cloning patchTree.
  //
  KjNode* patchTreeCopy = kjObject(orionldState.kjsonP, "patchTree");  // To be used to match subscriptions
  KjNode* attrP         = patchTree->value.firstChildP;

  while (attrP != NULL)
  {
    next = attrP->next;

    char  shortName[256];
    bool  special = isSpecialAttribute(attrP->name);

    strncpy(shortName, attrP->name, sizeof(shortName));

    // Only non-special attributes are subject to attribute name expansion
    if (special == false)
    {
      attrP->name = orionldContextItemExpand(orionldState.contextP, shortName, true, NULL);
      dotForEq(attrP->name);
    }

    //
    // Attributes that did not previously exist are ignored
    //
    LM_TMP(("Looking up attr '%s'", attrP->name));
    KjNode* dbAttrP = kjLookup(dbAttrs, attrP->name);
    if (dbAttrP == NULL)
    {
      attributeNotUpdated(notUpdatedP, shortName, "attribute doesn't exist in original entity");
      kjChildRemove(patchTree, attrP);
      attrP = next;
      continue;
    }

    KjNode* dbCreatedAtP  = kjLookup(dbAttrP, "creDate");
    KjNode* dbModifiedAtP = kjLookup(dbAttrP, "modDate");
    KjNode* dbTypeP       = kjLookup(dbAttrP, "type");
    KjNode* typeP         = kjLookup(attrP,   "type");

    // "type" cannot change in an update
    if (strcmp(dbTypeP->value.s, typeP->value.s) != 0)
    {
      LM_W(("Bad Input (mismatch in attribute type - in DB: '%s', in PAYLOAD: '%s')", dbTypeP->value.s, typeP->value.s));
      attributeNotUpdated(notUpdatedP, shortName, "mismatch in attribute type");
      kjChildRemove(patchTree, attrP);

      attrP = next;
      continue;
    }

    //
    // Changing "object" for "value" if "type" == "Relationship"
    //
    if (strcmp(typeP->value.s, "Relationship") == 0)
    {
      KjNode* objectP = kjLookup(attrP, "object");

      if (objectP == NULL)
      {
        LM_W(("Bad Input (Relationship without object)"));
        attributeNotUpdated(notUpdatedP, shortName, "Relationship without object");
        kjChildRemove(patchTree, attrP);

        attrP = next;
        continue;
      }

      objectP->name = (char*) "value";
    }
    else  // if not Relationship, there must be a value
    {
      KjNode* valueP = kjLookup(attrP, "value");

      if (valueP == NULL)
      {
        LM_W(("Bad Input (Property without value)"));
        attributeNotUpdated(notUpdatedP, shortName, "Property without value");
        kjChildRemove(patchTree, attrP);

        attrP = next;
        continue;
      }
    }

    attributeUpdated(updatedP, shortName);

    //
    // Attribute accepted - add to patchTreeCopy to be used for subscription matching
    //
    KjNode* attrCopy = kjClone(orionldState.kjsonP, attrP);
    kjChildAdd(patchTreeCopy, attrCopy);

    //
    // Remove the old db attr from its list and replace with the one from the payload
    //
    // This will reorder the attributes in the entity ... unfortunately - could "easily" be fixed
    // by instead of removing and appending, replace the old attribute in the same place of the linked list
    //
    kjChildRemove(dbAttrs, dbAttrP);
    kjChildRemove(patchTree, attrP);
    kjChildAdd(dbAttrs, attrP);


    // modifiedAt
    if (dbModifiedAtP != NULL)
    {
      dbModifiedAtP->value.f = orionldState.requestTime;
    }
    else  // No modifiedAt ... !
    {
      KjNode* modifiedAtP = kjFloat(orionldState.kjsonP, "modDate", orionldState.requestTime);
      kjChildAdd(dbAttrP, modifiedAtP);
    }

    // createdAt
    if (dbCreatedAtP == NULL)  // No createdAt !!!
    {
      KjNode* createdAtP = kjFloat(orionldState.kjsonP, "creDate", orionldState.requestTime);
      kjChildAdd(dbAttrP, createdAtP);
    }

    attrP = next;
  }

  if (updatedP->value.firstChildP == NULL)
  {
    LM_TMP(("Nothing has been updated - a 207 with all the info is returned"));
  }
  else
  {
    LM_TMP(("Something has been updated - writing to DB"));
    //
    // Update 'modifiedAt' for the entity
    //
    KjNode* modifiedAtP = kjLookup(dbEntityTree, "modDate");

    if (modifiedAtP == NULL)
    {
      KjNode* modifiedAtP = kjFloat(orionldState.kjsonP, "modDate", orionldState.requestTime);
      kjChildAdd(dbEntityTree, modifiedAtP);
    }
    else
      modifiedAtP->value.f = orionldState.requestTime;

    //
    // Write to DB
    //
    LM_TMP(("EPATCH: Writing updated entity to DB"));
    kjRender(orionldState.kjsonP, dbEntityTree, buf, sizeof(buf));
    LM_TMP(("updated entity: %s", buf));

    if (dbEntityUpdate(entityId, dbEntityTree) == false)
    {
      LM_E(("Database Error (Error writing entity '%s' to database)", entityId));
      orionldState.httpStatusCode = 500;
      orionldErrorResponseCreate(OrionldInternalError, "Database Error", "Error writing entity to database");
      return false;
    }
  }

  // 204 or 207?
  if (notUpdatedP->value.firstChildP != NULL)  // non-empty list of 'not updated'
  {
    //
    // 204 or 207?
    //
    // 204 if all went ok (== empty list of 'not updated')
    // 207 if something went wrong (== non-empty list of 'not updated')
    //
    // If 207 - prepare the response payload data
    //
    orionldState.responseTree = kjObject(orionldState.kjsonP, NULL);

    kjChildAdd(orionldState.responseTree, updatedP);
    kjChildAdd(orionldState.responseTree, notUpdatedP);

    orionldState.httpStatusCode = 207;
  }
  else
  {
    orionldState.httpStatusCode = 204;
  }

  //
  // Prepare Notifications, unless no notifications are desired (to help with performance)
  //
  if (notifications)
    notificationsPrepare(dbEntityTree, patchTreeCopy);

  return true;
}
