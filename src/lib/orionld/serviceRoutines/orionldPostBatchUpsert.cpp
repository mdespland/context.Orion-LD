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
* Author: Gabriel Quaresma and Ken Zangelin
*/
#include <vector>                // vector

extern "C"
{
#include "kjson/KjNode.h"     // KjNode
#include "kjson/kjBuilder.h"  // kjString, kjObject, ...
#include "kjson/kjLookup.h"   // kjLookup
#include "kjson/kjRender.h"   // kjRender
}

#include "logMsg/logMsg.h"       // LM_*
#include "logMsg/traceLevels.h"  // Lmt*

#include "common/globals.h"                   // parse8601Time
#include "rest/ConnectionInfo.h"              // ConnectionInfo
#include "rest/httpHeaderAdd.h"               // httpHeaderLocationAdd
#include "orionTypes/OrionValueType.h"        // orion::ValueType
#include "orionTypes/UpdateActionType.h"      // ActionType
#include "parse/CompoundValueNode.h"          // CompoundValueNode
#include "ngsi/ContextAttribute.h"            // ContextAttribute
#include "ngsi10/UpdateContextRequest.h"      // UpdateContextRequest
#include "ngsi10/UpdateContextResponse.h"     // UpdateContextResponse
#include "mongoBackend/mongoEntityExists.h"   // mongoEntityExists
#include "mongoBackend/mongoUpdateContext.h"  // mongoUpdateContext
#include "rest/uriParamNames.h"               // URI_PARAM_PAGINATION_OFFSET, URI_PARAM_PAGINATION_LIMIT
#include "mongoBackend/MongoGlobal.h"         // getMongoConnection()

#include "orionld/rest/orionldServiceInit.h"                   // orionldHostName, orionldHostNameLen
#include "orionld/common/orionldErrorResponse.h"               // orionldErrorResponseCreate
#include "orionld/common/SCOMPARE.h"                           // SCOMPAREx
#include "orionld/common/CHECK.h"                              // CHECK
#include "orionld/common/urlCheck.h"                           // urlCheck
#include "orionld/common/urnCheck.h"                           // urnCheck
#include "orionld/common/orionldState.h"                       // orionldState
#include "orionld/common/orionldAttributeTreat.h"              // orionldAttributeTreat
#include "orionld/context/orionldCoreContext.h"                // orionldDefaultUrl, orionldCoreContext
#include "orionld/context/orionldContextPresent.h"             // orionldContextPresent
#include "orionld/context/orionldContextItemAliasLookup.h"     // orionldContextItemAliasLookup
#include "orionld/context/orionldContextItemExpand.h"          // orionldUriExpand
#include "orionld/kjTree/kjStringValueLookupInArray.h"         // kjStringValueLookupInArray
#include "orionld/serviceRoutines/orionldPostBatchUpsert.h"    // Own Interface
#include "orionld/db/dbEntityUpdateAttribute.h"                // dbEntityUpdateAttribute



// -----------------------------------------------------------------------------
//
// kjTreeToContextElementBatch -
//
// NOTE: "id" and "type" of the entity must be removed from the tree before this function is called
//
bool kjTreeToContextElementAttributes
(
  ConnectionInfo*  ciP,
  KjNode*          entityNodeP,
  KjNode*          createdAtP,
  KjNode*          modifiedAtP,
  ContextElement*  ceP,
  char**           detailP
)
{
  // Iterate over the items of the entity
  for (KjNode *itemP = entityNodeP->value.firstChildP; itemP != NULL; itemP = itemP->next)
  {
    if (itemP == createdAtP)
      continue;
    if (itemP == modifiedAtP)
      continue;

    // No key-values in batch ops - all attrs must be objects
    if (itemP->type != KjObject)
    {
      *detailP = (char *)"attribute must be a JSON object";
      return false;
    }

    KjNode*           attrTypeNodeP = NULL;
    ContextAttribute* caP           = new ContextAttribute();

    // orionldAttributeTreat treats the attribute, including expanding the attribute name and values, if applicable
    if (orionldAttributeTreat(ciP, itemP, caP, &attrTypeNodeP, detailP) == false)
    {
      LM_E(("orionldAttributeTreat failed"));
      delete caP;
      return false;
    }

    ceP->contextAttributeVector.push_back(caP);
  }

  return true;
}



// ----------------------------------------------------------------------------
//
// entitySuccessPush -
//
static void entitySuccessPush(KjNode *successArrayP, const char *entityId)
{
  KjNode *eIdP = kjString(orionldState.kjsonP, "id", entityId);

  kjChildAdd(successArrayP, eIdP);
}



// ----------------------------------------------------------------------------
//
// entityErrorPush -
//
// The array "errors" in BatchOperationResult is an array of BatchEntityError.
// BatchEntityError contains a string (the entity id) and an instance of ProblemDetails.
//
// ProblemDetails is described in https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.01.01_60/gs_CIM009v010101p.pdf
// and contains:
//
// * type      (string) A URI reference that identifies the problem type
// * title     (string) A short, human-readable summary of the problem
// * detail    (string) A human-readable explanation specific to this occurrence of the problem
// * status    (number) The HTTP status code
// * instance  (string) A URI reference that identifies the specific occurrence of the problem
//
// Of these five items, only "type" seems to be mandatory.
//
// This implementation will treat "type", "title", and "status" as MANDATORY, and "detail" as OPTIONAL
//
static void entityErrorPush(KjNode *errorsArrayP, const char *entityId, OrionldResponseErrorType type, const char *title, const char *detail, int status)
{
  KjNode *objP            = kjObject(orionldState.kjsonP, NULL);
  KjNode *eIdP            = kjString(orionldState.kjsonP,  "entityId", entityId);
  KjNode *problemDetailsP = kjObject(orionldState.kjsonP,  "error");
  KjNode *typeP           = kjString(orionldState.kjsonP,  "type",     orionldErrorTypeToString(type));
  KjNode *titleP          = kjString(orionldState.kjsonP,  "title",    title);
  KjNode *statusP         = kjInteger(orionldState.kjsonP, "status",   status);

  kjChildAdd(problemDetailsP, typeP);
  kjChildAdd(problemDetailsP, titleP);

  if (detail != NULL)
  {
    KjNode* detailP = kjString(orionldState.kjsonP, "detail", detail);
    kjChildAdd(problemDetailsP, detailP);
  }

  kjChildAdd(problemDetailsP, statusP);

  kjChildAdd(objP, eIdP);
  kjChildAdd(objP, problemDetailsP);

  kjChildAdd(errorsArrayP, objP);
}



// ----------------------------------------------------------------------------
//
// entityIdPush - The function objective is add ID field from payload entities,
//                for delete the respective entities (if them existis) and
//                replace for entities from payload.
//
static void entityIdPush(KjNode *entityIdsArrayP, const char *entityId)
{
  KjNode *eIdP = kjString(orionldState.kjsonP, "id", entityId);

  kjChildAdd(entityIdsArrayP, eIdP);
}



// ---------------------------------------------------------------------------------------------------
//
// entityIdAndCreDateFromDbPush - The function objective is add ID field and creDate from DB entities,
//                                for use the original creDate for update the entities replaced.
//
static void entityIdAndCreDateFromDbPush(KjNode *entityIdAndCreDateArrayP, const char *entityId, double creDate)
{
  KjNode *objP     = kjObject(orionldState.kjsonP, NULL);
  KjNode *eIdP     = kjString(orionldState.kjsonP, "id", entityId);
  KjNode *creDateP = kjInteger(orionldState.kjsonP, "creDate", creDate);

  kjChildAdd(objP, eIdP);
  kjChildAdd(objP, creDateP);

  kjChildAdd(entityIdAndCreDateArrayP, objP);
}



// ----------------------------------------------------------------------------
//
// orionldPostEntityOperationsUpsert -
//
// POST /ngsu-ld/v1/entityOperations/upsert
//
// From the spec:
//   This operation allows creating a batch of NGSI-LD Entities, updating each of them if they already exist.
//
//   An optional flag indicating the update mode (only applies in case the Entity already exists):
//     - ?options=replace  (default)
//     - ?options=update
//
//   Replace:  All the existing Entity content shall be replaced  - like PUT
//   Update:   Existing Entity content shall be updated           - like PATCH
//
bool orionldPostBatchUpsert(ConnectionInfo *ciP)
{
  //
  // Prerequisites for the payload in orionldState.requestTree:
  // * must be an array
  // * cannot be empty
  // * all entities must contain a entity::id (one level down)
  // * no entity can contain an entity::type (one level down)
  //
  ARRAY_CHECK(orionldState.requestTree, "toplevel");
  EMPTY_ARRAY_CHECK(orionldState.requestTree, "toplevel");

  //
  // Prerequisites for URI params:
  // * both 'update' and 'replace' cannot be set in options (replace is default)
  //
  if ((orionldState.uriParamOptions.update == true) && (orionldState.uriParamOptions.replace == true))
  {
    orionldErrorResponseCreate(OrionldBadRequestData, "URI Param Error", "options: both /update/ and /replace/ present");
    ciP->httpStatusCode = SccBadRequest;
    return false;
  }

  //
  // Here we treat the "?options=update" mode
  //

  UpdateContextRequest mongoRequest;
  UpdateContextResponse mongoResponse;
  KjNode* createdAtP = NULL;
  KjNode* modifiedAtP = NULL;
  KjNode* successArrayP = kjArray(orionldState.kjsonP, "success");
  KjNode* errorsArrayP = kjArray(orionldState.kjsonP, "errors");
  KjNode* entityIdsArrayP = kjArray(orionldState.kjsonP, "entityIds");
  KjNode* entityIdAndCreDateArrayP = kjArray(orionldState.kjsonP, "entitiesIdAndCreDate");
  char   *detail;

  ciP->httpStatusCode = SccOk;

  mongoRequest.updateActionType = ActionTypeAppend;

  for (KjNode* entityNodeP = orionldState.requestTree->value.firstChildP; entityNodeP != NULL; entityNodeP = entityNodeP->next)
  {
    OBJECT_CHECK(entityNodeP, kjValueType(entityNodeP->type));

    //
    // First, extract Entity::id and Entity::type
    //
    // As we will remove items from the tree, we need to save the 'next-pointer' a priori
    // If not, after removing an item, its next pointer point to NULL and the for-loop (if used) is ended
    //
    KjNode*  itemP                 = entityNodeP->value.firstChildP;
    KjNode*  entityIdNodeP         = NULL;
    KjNode*  entityTypeNodeP       = NULL;
    bool     duplicatedType        = false;
    bool     duplicatedId          = false;
    KjNode*  entityNodeCreDateP    = kjLookup(entityNodeP, "createdAt");
    KjNode*  entityNodeModDateP    = kjLookup(entityNodeP, "modifiedAt");

    if (entityNodeCreDateP != NULL)  // Ignore "createdAt" if present in incoming payload
      kjChildRemove(entityNodeP, entityNodeCreDateP);

    if (entityNodeModDateP != NULL)  // Ignore "modifiedAt" if present in incoming payload
      kjChildRemove(entityNodeP, entityNodeModDateP);

    //
    // We only check for duplicated entries in this loop.
    // The rest is taken care of after we've looked up entity::id
    //
    while (itemP != NULL)
    {
      if (SCOMPARE3(itemP->name, 'i', 'd', 0))
      {
        if (entityIdNodeP != NULL)
          duplicatedId = true;
        else
          entityIdNodeP = itemP;

        itemP = itemP->next;  // Point to the next item BEFORE the current one is removed
        kjChildRemove(entityNodeP, entityIdNodeP);
      }
      else if (SCOMPARE5(itemP->name, 't', 'y', 'p', 'e', 0))
      {
        if (entityTypeNodeP != NULL)  // Duplicated 'type' in payload?
          duplicatedType = true;
        else
          entityTypeNodeP = itemP;

        itemP = itemP->next;  // Point to the next item BEFORE the current one is removed
        kjChildRemove(entityNodeP, entityTypeNodeP);
      }
      else
        itemP = itemP->next;
    }

    // Entity ID is mandatory
    if (entityIdNodeP == NULL)
    {
      LM_W(("Bad Input (mandatory field missing: entity::id)"));
      entityErrorPush(errorsArrayP, "no entity::id", OrionldBadRequestData, "mandatory field missing", "entity::id", 400);
      continue;
    }

    // Entity ID must be a string
    if (entityIdNodeP->type != KjString)
    {
      LM_W(("Bad Input (entity::id not a string)"));
      entityErrorPush(errorsArrayP, "invalid entity::id", OrionldBadRequestData, "field with invalid type", "entity::id", 400);
      continue;
    }

    // Entity ID must be a valid URI
    if (!urlCheck(entityIdNodeP->value.s, &detail) && !urnCheck(entityIdNodeP->value.s, &detail))
    {
      LM_W(("Bad Input (entity::id is a string but not a valid URI)"));
      entityErrorPush(errorsArrayP, entityIdNodeP->value.s, OrionldBadRequestData, "Not a URI", entityIdNodeP->value.s, 400);
      continue;
    }

    // Entity ID must not be duplicated
    if (duplicatedId == true)
    {
      LM_W(("Bad Input (Duplicated entity::id)"));
      entityErrorPush(errorsArrayP, entityIdNodeP->value.s, OrionldBadRequestData, "Duplicated field", "entity::id", 400);
      continue;
    }

    // Entity TYPE is mandatory
    if (entityTypeNodeP == NULL)
    {
      LM_W(("Bad Input (mandatory field missing: entity::type)"));
      entityErrorPush(errorsArrayP, entityIdNodeP->value.s, OrionldBadRequestData, "mandatory field missing", "entity::type", 400);
      continue;
    }

    // Entity TYPE must not be duplicated
    if (duplicatedType == true)
    {
      LM_W(("Bad Input (Duplicated entity::type)"));
      entityErrorPush(errorsArrayP, entityIdNodeP->value.s, OrionldBadRequestData, "Duplicated field", "entity::type", 400);
      continue;
    }

    // Entity TYPE must be a string
    if (entityTypeNodeP->type != KjString)
    {
      LM_W(("Bad Input (entity::type not a string)"));
      entityErrorPush(errorsArrayP, entityIdNodeP->value.s, OrionldBadRequestData, "field with invalid type", "entity::type", 400);
      continue;
    }

    //
    // Both Entity::id and Entity::type are OK
    //
    char*           entityId            = entityIdNodeP->value.s;
    char*           entityType          = entityTypeNodeP->value.s;
    ContextElement* ceP                 = new ContextElement();  // FIXME: Any way I can avoid to allocate ?
    EntityId*       entityIdP           = &ceP->entityId;
    bool            valueMayBeExpanded  = false;
    char*           typeExpanded;

    mongoRequest.updateActionType = ActionTypeAppendStrict;
    entityIdP->id = entityId;

    typeExpanded = orionldContextItemExpand(orionldState.contextP, entityType, &valueMayBeExpanded, true, NULL);
    if (typeExpanded == NULL)
    {
      LM_E(("orionldUriExpand failed: %s", detail));
      entityErrorPush(errorsArrayP, entityIdNodeP->value.s, OrionldBadRequestData, "unable to expand entity::type", detail, 400);
      delete ceP;
      continue;
    }

    entityIdP->type      = typeExpanded;
    entityIdP->isPattern = "false";

#if 0
    entityIdP->creDate   = getCurrentTime();  // FIXME: Only if newly created. I think mongoBackend takes care of this - so, outdeffed
    entityIdP->modDate   = getCurrentTime();
#endif

    if (kjTreeToContextElementAttributes(ciP, entityNodeP, createdAtP, modifiedAtP, ceP, &detail) == false)
    {
      LM_W(("kjTreeToContextElementAttributes flags error '%s' for entity '%s'", detail, entityId));
      entityErrorPush(errorsArrayP, entityId, OrionldBadRequestData, "", detail, 400);
      delete ceP;
      continue;
    }

    if (orionldState.uriParamOptions.replace == true)
      entityIdPush(entityIdsArrayP, entityId);

    mongoRequest.contextElementVector.push_back(ceP);

    orionldState.payloadIdNode = NULL;
    orionldState.payloadTypeNode = NULL;
  }

  if (orionldState.uriParamOptions.replace == true)
  {
    if (entityIdsArrayP->value.firstChildP != NULL)
    {
      LM_TMP(("UPSERT: Calling dbEntityLookupMany"));
      KjNode* entitiesFromDbP = dbQueryEntitiesAsKjTree(entityIdsArrayP);

      LM_TMP(("UPSERT: back from dbEntityLookupMany"));

      entityIdsArrayP = kjArray(orionldState.kjsonP, "entityIds");

      if (entitiesFromDbP == NULL)
        LM_TMP(("UPSERT: dbEntityLookupMany found no entities - nothing to remove - we're done here"));
      else
      {
        for (unsigned int ix = 0; ix < mongoRequest.contextElementVector.vec.size(); ix++)
        {
          int                           entityIx                        = 0;
          bool                          isAbleToPushIntoEntityIdsArray  = true;
          std::vector<ContextElement*>  ceMongoReqVecP                  = mongoRequest.contextElementVector.vec;
          const char*                   entityIdMongoReq                = ceMongoReqVecP[ix]->entityId.id.c_str();
          const char*                   typeMongoReq                    = ceMongoReqVecP[ix]->entityId.type.c_str();
          const char*                   typeMongoReqAlias;

          typeMongoReqAlias = orionldContextItemAliasLookup(orionldState.contextP, typeMongoReq, NULL, NULL);

          LM_TMP(("EntityId: %s", entityIdMongoReq));
          LM_TMP(("Type: %s", typeMongoReqAlias));
          LM_TMP(("UPSERT: Looping over the entities to be replaced"));

          for (KjNode* entityNodeP = entitiesFromDbP->value.firstChildP; entityNodeP != NULL; entityNodeP = entityNodeP->next)
          {
            KjNode* itemFromDbP = entityNodeP->value.firstChildP;
            char*   id          = NULL;
            char*   type        = NULL;
            double  creDate     = 0;

            while (itemFromDbP != NULL)
            {
              LM_TMP(("UPSERT: Got item '%s' of entity %d", itemFromDbP->name, entityIx));

              if (SCOMPARE4(itemFromDbP->name, '_', 'i', 'd', 0))
              {
                KjNode* _idContentP = itemFromDbP->value.firstChildP;
                while (_idContentP != NULL)
                {
                  if (SCOMPARE3(_idContentP->name, 'i', 'd', 0))
                  {
                    id = _idContentP->value.s;
                    LM_TMP(("UPSERT: id: %s", id));
                  }
                  else if (SCOMPARE5(_idContentP->name, 't', 'y', 'p', 'e', 0))
                  {
                    char* alias;
                    alias = orionldContextItemAliasLookup(orionldState.contextP, _idContentP->value.s, NULL, NULL);
                    type = alias;
                    LM_TMP(("UPSERT: type: %s", type));
                  }
                  _idContentP = _idContentP->next;
                }
              }
              else if (SCOMPARE8(itemFromDbP->name, 'c', 'r', 'e', 'D', 'a', 't', 'e', 0))
              {
                creDate = itemFromDbP->value.i;
                LM_TMP(("UPSERT: creDate: %d", creDate));
              }
              itemFromDbP = itemFromDbP->next;
            }

            if (strcmp(entityIdMongoReq, id) == false)
            {
              if (strcmp(typeMongoReqAlias, type) != false)
              {
                isAbleToPushIntoEntityIdsArray = false;
                entityErrorPush(errorsArrayP, entityIdMongoReq, OrionldBadRequestData, "attempt to modify entity::type", typeMongoReqAlias, 400);
                mongoRequest.contextElementVector.vec.erase(mongoRequest.contextElementVector.vec.begin() + ix);
              }
              else
              {
                entityIdAndCreDateFromDbPush(entityIdAndCreDateArrayP, id, creDate);
              }
            }
            ++entityIx;
          }
          if (isAbleToPushIntoEntityIdsArray)
            entityIdPush(entityIdsArrayP, entityIdMongoReq);
        }

        if (entitiesFromDbP->value.firstChildP != NULL)
        {
          if (dbEntityBatchDelete(entityIdsArrayP) == false)
          {
            LM_E(("mongoCppLegacyEntityBatchDelete returned false"));
            ciP->httpStatusCode = SccBadRequest;
            if (orionldState.responseTree == NULL)
              orionldErrorResponseCreate(OrionldBadRequestData, "Database Error", "mongoCppLegacyEntityBatchDelete");
            return false;
          }
        }
      }
    }
  }

  //
  // Call mongoBackend - to create/modify the entities
  // In case of REPLACE, all entities have been removed from the DB prior to this call, so, they will all be created.
  //
  LM_TMP(("UPSERT: Calling mongoUpdateContext to create %d entities", mongoRequest.contextElementVector.size()));
  ciP->httpStatusCode = mongoUpdateContext(&mongoRequest,
                                           &mongoResponse,
                                           orionldState.tenant,
                                           ciP->servicePathV,
                                           ciP->uriParam,
                                           ciP->httpHeaders.xauthToken,
                                           ciP->httpHeaders.correlator,
                                           ciP->httpHeaders.ngsiv2AttrsFormat,
                                           ciP->apiVersion,
                                           NGSIV2_NO_FLAVOUR);

  //
  // FIXME: This loop seems to exist only to put back the correct creDate for the entities after calling mongoUpdateContext
  //        First of all, it doesn't seem to be working, and second: it provokes ONE mongo access per entity in the entity vector.
  //        This is of course extremely slow and needs to be fixed in another way.
  //        The call to 'mongoUpdateContext' should be modified to set the creDate correctly already.
  //        - And this loop should be removed.
  //
  for (KjNode* entityNodeP = entityIdAndCreDateArrayP->value.firstChildP; entityNodeP != NULL; entityNodeP = entityNodeP->next)
  {
    KjNode*  itemFromDbP  = entityNodeP->value.firstChildP;
    char*     entityId    = NULL;
    KjNode*   creDateNode = NULL;

    while (itemFromDbP != NULL)
    {
      if (SCOMPARE3(itemFromDbP->name, 'i', 'd', 0))
      {
        entityId = itemFromDbP->value.s;
        LM_TMP(("entityIdAndCreDateArrayP: id: %s", entityId));
      }
      else if (SCOMPARE8(itemFromDbP->name, 'c', 'r', 'e', 'D', 'a', 't', 'e', 0))
      {
        creDateNode  = itemFromDbP;
        LM_TMP(("entityIdAndCreDateArrayP: creDate: %d", itemFromDbP->value.i));
      }
      itemFromDbP = itemFromDbP->next;
    }
    dbEntityUpdateAttribute(entityId, creDateNode);
  }

  //
  // Now check orionldState.errorAttributeArray to see whether any attribute failed to be updated
  //
  // bool partialUpdate = (orionldState.errorAttributeArrayP[0] == 0)? false : true;
  // bool retValue      = true;
  //

  if (ciP->httpStatusCode == SccOk)
  {
    orionldState.responseTree = kjObject(orionldState.kjsonP, NULL);

    for (unsigned int ix = 0; ix < mongoResponse.contextElementResponseVector.vec.size(); ix++)
    {
      const char *entityId = mongoResponse.contextElementResponseVector.vec[ix]->contextElement.entityId.id.c_str();

      if (mongoResponse.contextElementResponseVector.vec[ix]->statusCode.code == SccOk)
        entitySuccessPush(successArrayP, entityId);
      else
        entityErrorPush(errorsArrayP,
                        entityId,
                        OrionldBadRequestData,
                        "",
                        mongoResponse.contextElementResponseVector.vec[ix]->statusCode.reasonPhrase.c_str(),
                        400);
    }

    for (unsigned int ix = 0; ix < mongoRequest.contextElementVector.vec.size(); ix++)
    {
      const char *entityId = mongoRequest.contextElementVector.vec[ix]->entityId.id.c_str();

      if (kjStringValueLookupInArray(successArrayP, entityId) == NULL)
        entitySuccessPush(successArrayP, entityId);
    }

    //
    // Add the success/error arrays to the response-tree
    //
    kjChildAdd(orionldState.responseTree, successArrayP);
    kjChildAdd(orionldState.responseTree, errorsArrayP);

    ciP->httpStatusCode = SccOk;
  }

  mongoRequest.release();
  mongoResponse.release();

  if (ciP->httpStatusCode != SccOk)
  {
    LM_E(("mongoUpdateContext flagged an error"));
    orionldErrorResponseCreate(OrionldBadRequestData, "Internal Error", "Database Error");
    ciP->httpStatusCode = SccReceiverInternalError;
    return false;
  }

  return true;
}
