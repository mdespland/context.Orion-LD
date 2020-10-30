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
#include "orionld/db/dbConfiguration.h"                          // dbEntityLookup, dbEntityUpdate
#include "orionld/context/orionldContextItemExpand.h"            // orionldContextItemExpand
#include "orionld/kjTree/kjTreeToContextAttribute.h"             // kjTreeToContextAttribute
#include "orionld/kjTree/kjStringValueLookupInArray.h"           // kjStringValueLookupInArray
#include "orionld/kjTree/kjTreeToUpdateContextRequest.h"         // kjTreeToUpdateContextRequest
#include "orionld/serviceRoutines/orionldPatchEntity.h"          // Own Interface



// ----------------------------------------------------------------------------
//
// isSpecialAttribute -
//
bool isSpecialAttribute(const char* attrName)
{
  if (strcmp(attrName, "createdAt") == 0)
    return true;
  if (strcmp(attrName, "modifiedAt") == 0)
    return true;
  if (strcmp(attrName, "location") == 0)
    return true;
  if (strcmp(attrName, "observationSpace") == 0)
    return true;
  if (strcmp(attrName, "operationSpace") == 0)
    return true;

  return false;
}

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

// ----------------------------------------------------------------------------
//
// attributeNotUpdated -
//
// FIXME: there's a copy of this function in orionldPostEntity.cpp
//
static void attributeNotUpdated(KjNode* notUpdatedP, const char* attrName, const char* reason)
{
  LM_TMP(("EPATCH: attribute '%s' does not exist in DB (attrs field) - ignoring it", attrName));

  KjNode* notUpdatedDetailsP = kjObject(orionldState.kjsonP, NULL);
  KjNode* attrNameP          = kjString(orionldState.kjsonP, "attributeName", attrName);
  KjNode* reasonP            = kjString(orionldState.kjsonP, "reason", reason);

  kjChildAdd(notUpdatedDetailsP, attrNameP);
  kjChildAdd(notUpdatedDetailsP, reasonP);

  kjChildAdd(notUpdatedP, notUpdatedDetailsP);
}



// ----------------------------------------------------------------------------
//
// attributeUpdated -
//
// FIXME: there's a copy of this function in orionldPostEntity.cpp
//
static void attributeUpdated(KjNode* updatedP, const char* attrName)
{
  LM_TMP(("EPATCH: attribute '%s' has been updated", attrName));

  KjNode* attrNameP = kjString(orionldState.kjsonP, NULL, attrName);

  kjChildAdd(updatedP, attrNameP);
}



// ----------------------------------------------------------------------------
//
// attributeCheck -
//
// FIXME - move to separate module - should be used also for:
//   * POST /entities/*/attrs
//   * PATCH /entities/*/attrs
//   * etc
//
bool attributeCheck(KjNode* attrNodeP, char** titleP, char** detailP)
{
  if (attrNodeP->type != KjObject)
  {
    *titleP  = (char*) "Invalid JSON Type";
    *detailP = (char*) "Attribute must be an object";

    return false;
  }

  KjNode* typeP    = NULL;
  KjNode* valueP   = NULL;
  KjNode* objectP  = NULL;
  int     attrType = 0;   // 1: Property, 2: Relationship, 3: GeoProperty, 4: TemporalProperty

  for (KjNode* nodeP = attrNodeP->value.firstChildP; nodeP != NULL; nodeP = nodeP->next)
  {
    if (strcmp(nodeP->name, "type") == 0)
    {
      DUPLICATE_CHECK(typeP, "type", nodeP);
      STRING_CHECK(typeP, "type");

      if      (strcmp(typeP->value.s, "Property")         == 0)  attrType = 1;
      else if (strcmp(typeP->value.s, "Relationship")     == 0)  attrType = 2;
      else if (strcmp(typeP->value.s, "GeoProperty")      == 0)  attrType = 3;
      else if (strcmp(typeP->value.s, "TemporalProperty") == 0)  attrType = 4;
      else
      {
        *titleP  = (char*) "Invalid Value of Attribute Type";
        *detailP = typeP->value.s;

        return false;
      }
    }
    else if (strcmp(nodeP->name, "value") == 0)
    {
      DUPLICATE_CHECK(valueP, "value", nodeP);
    }
    else if (strcmp(nodeP->name, "object") == 0)
    {
      DUPLICATE_CHECK(objectP, "object", nodeP);
    }
  }

  if (typeP == NULL)
  {
    *titleP  = (char*) "Mandatory field missing";
    *detailP = (char*) "attribute type";

    return false;
  }

  if (attrType == 2)  // 2 == Relationship
  {
    // Relationships MUST have an "object"
    if (objectP == NULL)
    {
      *titleP  = (char*) "Mandatory field missing";
      *detailP = (char*) "Mandatory field missing: Relationship object";

      return false;
    }
  }
  else
  {
    // Properties MUST have a "value"
    if (valueP == NULL)
    {
      *titleP  = (char*) "Mandatory field missing";
      *detailP = (char*) "Mandatory field missing: Property value";

      return false;
    }
  }

  return true;
}



// ----------------------------------------------------------------------------
//
// specialAttributeCheck -
//
// FIXME - move to separate module - should be used also for:
//   * POST /entities/*/attrs
//   * PATCH /entities/*/attrs
//   * etc
//
bool specialAttributeCheck(KjNode* attrNodeP, char** titleP, char** detailP)
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

  return true;
}


#if 1
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
  char*   entityId      = orionldState.wildcard[0];
  KjNode* patchTree     = orionldState.requestTree;
  KjNode* dbEntityTree;
  char*   title;
  char*   detail;

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
  // Set pointers to the DB attributes
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
  // Remove from patchTree:
  //   * attributes that are incorrect
  //   * attributes that are not found in the DB
  //
  // Removed attributes are stored in the array 'notUpdatedP' to be a part of the response
  //
  KjNode* updatedP     = kjArray(orionldState.kjsonP, "updated");
  KjNode* notUpdatedP  = kjArray(orionldState.kjsonP, "notUpdated");
  KjNode* aP           = patchTree->value.firstChildP;
  KjNode* next;

  while (aP != NULL)
  {
    char*   shortName   = aP->name;
    bool    special     = isSpecialAttribute(shortName);

    next = aP->next;

    // Only non-special attributes are subject to attribute name expansion
    if (special == false)
    {
      aP->name = orionldContextItemExpand(orionldState.contextP, shortName, true, NULL);
      dotForEq(aP->name);
    }

    //
    // Check that the attribute is syntactically OK
    //
    if (special == true)
    {
      if (specialAttributeCheck(aP, &title, &detail) == false)
      {
        attributeNotUpdated(notUpdatedP, shortName, detail);
        kjChildRemove(patchTree, aP);
      }
    }
    else
    {
      if (attributeCheck(aP, &title, &detail) == false)
      {
        attributeNotUpdated(notUpdatedP, shortName, detail);
        kjChildRemove(patchTree, aP);
      }
    }

    //
    // Attributes that did not previously exist are ignored
    //
    if (kjLookup(dbAttrs, aP->name) == NULL)
    {
      attributeNotUpdated(notUpdatedP, shortName, "attribute doesn't exist in original entity");
      kjChildRemove(patchTree, aP);
    }
    else
      attributeUpdated(updatedP, shortName);

    aP = next;
  }

  kjRender(orionldState.kjsonP, patchTree, buf, sizeof(buf));
  LM_TMP(("New patchTree: %s", buf));


  //
  // Overwrite the "DB-Entity tree" with all remaining attributes from the "Patch Tree".
  // Keep the "Patch Tree" intact as it is needed for finding matching subscriptions
  // FIXME: Match subs before merging "Patch Tree" into "DB-Entity tree" to avoid cloning patchTree.
  //
  // KjNode* patchTreeCopy = kjClone(orionldState.kjsonP, patchTree);  // To be used to match subscriptions
  KjNode* attrP         = patchTree->value.firstChildP;

  while (attrP != NULL)
  {
    next = attrP->next;

    KjNode* dbAttrP = kjLookup(dbAttrs, attrP->name);

    if (dbAttrP == NULL)
    {
      LM_E(("Database Error (previously found attribute '%s' is now lost!!!)", attrP->name));

      attributeNotUpdated(notUpdatedP, attrP->name, "internal db error");
      kjChildRemove(patchTree, attrP);

      attrP = next;
      continue;
    }

    KjNode* dbCreatedAtP  = kjLookup(dbAttrP, "creDate");
    KjNode* dbModifiedAtP =	kjLookup(dbAttrP, "modDate");
    KjNode* dbTypeP       =	kjLookup(dbAttrP, "type");
    KjNode* typeP         =	kjLookup(attrP, "type");

    // "type" cannot change in an update
    if (strcmp(dbTypeP->value.s, typeP->value.s) != 0)
    {
      LM_W(("Bad Input (mismatch in attribute type)"));
      attributeNotUpdated(notUpdatedP, attrP->name, "mismatch in attribute type");
      kjChildRemove(patchTree, attrP);

      attrP = next;
      continue;
    }

    // Removing the old db attr from its list and replace with the one from the payload
    kjChildRemove(dbAttrs, dbAttrP);
    kjChildRemove(patchTree, attrP);
    kjChildAdd(dbAttrs, attrP);
    // This will reorder the attributes in the entity ... unfortunate - could "easily" be fixed

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

  return true;
}
#else
// ----------------------------------------------------------------------------
//
// orionldPatchEntity -
//
// The input payload is a collection of attributes.
// Those attributes that don't exist already in the entity are not added, but reported in the response payload data as "notUpdated".
// The rest of attributes replace the old attribute.
//
// Extract from ETSI NGSI-LD spec:
//   For each of the Attributes included in the Fragment, if the target Entity includes a matching one (considering
//   term expansion rules as mandated by clause 5.5.7), then replace it by the one included by the Fragment. If the
//   Attribute includes a datasetId, only an Attribute instance with the same datasetId is replaced.
//   In all other cases, the Attribute shall be ignored.
//
bool orionldPatchEntity(ConnectionInfo* ciP)
{
  char* entityId   = orionldState.wildcard[0];
  char* detail;

  // 1. Is the Entity ID in the URL a valid URI?
  if ((urlCheck(entityId, &detail) == false) && (urnCheck(entityId, &detail) == false))
  {
    orionldState.httpStatusCode = 400;
    orionldErrorResponseCreate(OrionldBadRequestData, "Entity ID must be a valid URI", entityId);
    return false;
  }

  // 2. Is the payload not a JSON object?
  OBJECT_CHECK(orionldState.requestTree, kjValueType(orionldState.requestTree->type));

  // 3. Get the entity from mongo
  KjNode* dbEntityP;
  if ((dbEntityP = dbEntityLookup(entityId)) == NULL)
  {
    orionldState.httpStatusCode = 404;
    orionldErrorResponseCreate(OrionldBadRequestData, "Entity does not exist", entityId);
    return false;
  }

  // 3. Get the Entity Type, needed later in the call to the constructor of ContextElement
  KjNode* idNodeP = kjLookup(dbEntityP, "_id");

  if (idNodeP == NULL)
  {
    orionldState.httpStatusCode = 500;
    orionldErrorResponseCreate(OrionldInternalError, "Corrupt Database", "'_id' field of entity from DB not found");
    return false;
  }

  KjNode*     entityTypeNodeP = kjLookup(idNodeP, "type");
  const char* entityType      = (entityTypeNodeP != NULL)? entityTypeNodeP->value.s : NULL;

  if (entityTypeNodeP == NULL)
  {
    orionldState.httpStatusCode = 500;
    orionldErrorResponseCreate(OrionldInternalError, "Corrupt Database", "'_id::type' field of entity from DB not found");
    return false;
  }

  // 4. Get the 'attrNames' array from the mongo entity - to see whether an attribute already existed or not
  KjNode* inDbAttrNamesP = kjLookup(dbEntityP, "attrNames");
  if (inDbAttrNamesP == NULL)
  {
    orionldState.httpStatusCode = 500;
    orionldErrorResponseCreate(OrionldInternalError, "Corrupt Database", "'attrNames' field of entity from DB not found");
    return false;
  }

  // 4. Also, get the "attrs" object for insertion of modified attributes
  KjNode* inDbAttrsP = kjLookup(dbEntityP, "attrs");
  if (inDbAttrsP == NULL)
  {
    orionldState.httpStatusCode = 500;
    orionldErrorResponseCreate(OrionldInternalError, "Corrupt Database", "'attrs' field of entity from DB not found");
    return false;
  }

  //
  // 5. Loop over the incoming payload data
  //    Those attrs that don't exist in the DB (dbEntityP) are discarded and added to the 'notUpdated' array
  //    Those that do exist in dbEntityP are removed from dbEntityP and replaced with the corresponding attribute from the incoming payload data.
  //    (finally the modified dbEntityP will REPLACE what is currently in the database)
  //
  KjNode* newAttrP     = orionldState.requestTree->value.firstChildP;
  KjNode* next;
  KjNode* updatedP     = kjArray(orionldState.kjsonP, "updated");
  KjNode* notUpdatedP  = kjArray(orionldState.kjsonP, "notUpdated");

  while (newAttrP != NULL)
  {
    KjNode*  dbAttrP;
    char*    title;
    char*    detail;

    next = newAttrP->next;

    if ((strcmp(newAttrP->name, "location")         != 0) &&
        (strcmp(newAttrP->name, "observationSpace") != 0) &&
        (strcmp(newAttrP->name, "operationSpace")   != 0) &&
        (strcmp(newAttrP->name, "observedAt")       != 0))
    {
      newAttrP->name = orionldContextItemExpand(orionldState.contextP, newAttrP->name, true, NULL);
    }

    // Is the attribute in the incoming payload a valid attribute?
    if (attributeCheck(newAttrP, &title, &detail) == false)
    {
      LM_E(("attributeCheck: %s: %s", title, detail));
      attributeNotUpdated(notUpdatedP, newAttrP->name, detail);
      newAttrP = next;
      continue;
    }

    char* eqName = kaStrdup(&orionldState.kalloc, newAttrP->name);
    dotForEq(eqName);
    dbAttrP = kjLookup(inDbAttrsP, eqName);
    if (dbAttrP == NULL)  // Doesn't already exist - must be discarded
    {
      attributeNotUpdated(notUpdatedP, newAttrP->name, "attribute doesn't exist");
      newAttrP = next;
      continue;
    }

    // Steal createdAt from dbAttrP?

    // Remove the attribute to be updated (from dbEntityP::inDbAttrsP) and insert the attribute from the payload data
    kjChildRemove(inDbAttrsP, dbAttrP);
    kjChildAdd(inDbAttrsP, newAttrP);
    attributeUpdated(updatedP, newAttrP->name);
    newAttrP = next;
  }


  // 6. Convert the resulting tree (dbEntityP) to a ContextElement
  UpdateContextRequest ucRequest;
  ContextElement*      ceP = new ContextElement(entityId, entityType, "false");

  ucRequest.contextElementVector.push_back(ceP);

  for (KjNode* attrP = inDbAttrsP->value.firstChildP; attrP != NULL; attrP = attrP->next)
  {
    ContextAttribute* caP = new ContextAttribute();

    if (kjTreeToContextAttribute(orionldState.contextP, attrP, caP, NULL, &detail) == false)
    {
      LM_E(("kjTreeToContextAttribute: %s", detail));
      attributeNotUpdated(notUpdatedP, attrP->name, "Error");
      delete caP;
    }
    else
      ceP->contextAttributeVector.push_back(caP);
  }
  ucRequest.updateActionType = ActionTypeReplace;


  // 8. Call mongoBackend to do the REPLACE of the entity
  UpdateContextResponse  ucResponse;

  orionldState.httpStatusCode = mongoUpdateContext(&ucRequest,
                                                   &ucResponse,
                                                   orionldState.tenant,
                                                   ciP->servicePathV,
                                                   ciP->uriParam,
                                                   ciP->httpHeaders.xauthToken,
                                                   ciP->httpHeaders.correlator,
                                                   ciP->httpHeaders.ngsiv2AttrsFormat,
                                                   ciP->apiVersion,
                                                   NGSIV2_NO_FLAVOUR);

  ucRequest.release();

  // 9. Postprocess output from mongoBackend
  if (orionldState.httpStatusCode == 200)
  {
    //
    // 204 or 207?
    //
    // 204 if all went ok (== empty list of 'not updated')
    // 207 if something went wrong (== non-empty list of 'not updated')
    //
    // If 207 - prepare the response payload data
    //
    if (notUpdatedP->value.firstChildP != NULL)  // non-empty list of 'not updated'
    {
      orionldState.responseTree = kjObject(orionldState.kjsonP, NULL);

      kjChildAdd(orionldState.responseTree, updatedP);
      kjChildAdd(orionldState.responseTree, notUpdatedP);

      orionldState.httpStatusCode = 207;
    }
    else
      orionldState.httpStatusCode = 204;
  }
  else
  {
    LM_E(("mongoUpdateContext: HTTP Status Code: %d", orionldState.httpStatusCode));
    orionldErrorResponseCreate(OrionldBadRequestData, "Internal Error", "Error from Mongo-DB backend");
    return false;
  }

  return true;
}

#endif
