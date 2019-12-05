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
* Author: Larysse Savanna
*/
extern "C"
{
#include "kjson/KjNode.h"                                               // KjNode
#include "kjson/kjBuilder.h"                                            // kjString, kjObject, ...
#include "kjson/kjRender.h"                                             // kjRender
}

#include "logMsg/logMsg.h"                                              // LM_*
#include "logMsg/traceLevels.h"                                         // Lmt*

#include "rest/ConnectionInfo.h"                                        // ConnectionInfo
#include "ngsi10/UpdateContextRequest.h"                                // UpdateContextRequest
#include "ngsi10/UpdateContextResponse.h"                               // UpdateContextResponse
#include "orionld/common/SCOMPARE.h"                                    // SCOMPAREx
#include "orionld/common/urlCheck.h"                                    // urlCheck
#include "orionld/common/urnCheck.h"                                    // urnCheck
#include "orionld/common/orionldState.h"                                // orionldState
#include "orionld/common/orionldErrorResponse.h"                        // orionldErrorResponseCreate
#include "orionld/db/dbEntityBatchDelete.h"                             // dbEntityBatchDelete.h
#include "orionld/mongoCppLegacy/mongoCppLegacyEntityBatchDelete.h"     // mongoCppLegacyEntityBatchDelete
#include "orionld/mongoCppLegacy/mongoCppLegacyQueryEntitiesAsKjTree.h" // mongoCppLegacyQueryEntitiesAsKjTree
#include "orionld/serviceRoutines/orionldPostBatchDeleteEntities.h"     // Own interface



// ----------------------------------------------------------------------------
//
// orionldPostBatchDeleteEntities -
//
bool orionldPostBatchDeleteEntities(ConnectionInfo* ciP)
{
  KjNode* success   = kjArray(orionldState.kjsonP, "S");
  KjNode* errors    = kjArray(orionldState.kjsonP, "E");
  KjNode* errorObj;
  KjNode* nodeP;

  if (orionldState.requestTree->type != KjArray)
  {
    LM_W(("Bad Input (Payload must be a JSON Array)"));
    orionldErrorResponseCreate(OrionldBadRequestData, "Invalid payload", "Must be a JSON Array");
    ciP->httpStatusCode = SccBadRequest;
    return false;
  }

  //
  // Making sure all items of the array are strings and valid URIs
  //
  for (KjNode* idNodeP = orionldState.requestTree->value.firstChildP; idNodeP != NULL; idNodeP = idNodeP->next)
  {
    char* detail;

    if (idNodeP->type != KjString)
    {
      LM_W(("Bad Input (Invalid payload - Array items must be JSON Strings)"));
      orionldErrorResponseCreate(OrionldBadRequestData, "Invalid payload", "Array items must be JSON Strings");
      ciP->httpStatusCode = SccBadRequest;
      return false;
    }

    if (!urlCheck(idNodeP->value.s, &detail) && !urnCheck(idNodeP->value.s, &detail))
    {
      LM_W(("Bad Input (Invalid payload - Array items must be valid URIs)"));
      orionldErrorResponseCreate(OrionldBadRequestData, "Invalid payload", "Array items must be valid URIs");
      ciP->httpStatusCode = SccBadRequest;
      return false;
    }
  }

  //
  // First get the entities from database to check if they exist
  //
  KjNode* dbEntities = mongoCppLegacyQueryEntitiesAsKjTree(orionldState.requestTree);
  
  if (dbEntities == NULL)
  {
    LM_E(("mongoCppLegacyQueryEntitiesAsKjTree returned null"));
    ciP->httpStatusCode = SccBadRequest;
    if (orionldState.responseTree == NULL)
      orionldErrorResponseCreate(OrionldBadRequestData, "Database Error", "mongoCppLegacyQueryEntitiesAsKjTree returned null");
    return false;
  }

  if (dbEntities->value.firstChildP == NULL)
  {
    LM_E(("mongoCppLegacyQueryEntitiesAsKjTree returned empty array"));
    ciP->httpStatusCode = SccBadRequest;
    if (orionldState.responseTree == NULL)
      orionldErrorResponseCreate(OrionldBadRequestData, "Invalid payload", "Entities were not found in database.");
    return false;
  }
  
  //
  // Now loop in array of entities from database to compare each id with the id from requestTree
  //
  KjNode* reqEntityId = orionldState.requestTree->value.firstChildP;
  while (reqEntityId != NULL)
  {
    KjNode* next  = reqEntityId->next;
    bool idExists = false;
    
    for (KjNode* dbEntity = dbEntities->value.firstChildP; dbEntity != NULL; dbEntity = dbEntity->next)
    {
      KjNode* dbEntity_Id = dbEntity->value.firstChildP;     // _id field
      KjNode* dbEntityId  = dbEntity_Id->value.firstChildP;  // id field

      if (SCOMPARE3(dbEntityId->name, 'i', 'd', 0))
      {
        if (strcmp(reqEntityId->value.s, dbEntityId->value.s) == 0)
        {
          idExists = true;
          break; // Found. No need to keep searching.
        }
      }
      else
      {
        // There is no id field in entity object
        LM_E(("mongoCppLegacyQueryEntitiesAsKjTree returned invalid entity object"));
        ciP->httpStatusCode = SccReceiverInternalError;
        orionldErrorResponseCreate(OrionldInternalError, "Internal Error", "mongoCppLegacyQueryEntitiesAsKjTree returned invalid entity object");
        return false;
      }
    }
    if(idExists == false)
      {
        // Entity not found. Reporting error.

        // entityId field
        errorObj = kjObject(orionldState.kjsonP, NULL);
        nodeP    = kjString(orionldState.kjsonP, "entityId", reqEntityId->value.s);
        kjChildAdd(errorObj, nodeP);

        // error field
        nodeP    = kjString(orionldState.kjsonP, "error", "Entity not found in database.");
        kjChildAdd(errorObj, nodeP);
        kjChildAdd(errors, errorObj);

        // Remove id not found from payload
        kjChildRemove(orionldState.requestTree, reqEntityId);
      }
      else
        kjChildAdd(success, reqEntityId);
      
      reqEntityId = next;
  }

  //
  // Call batch delete function
  //
  if (mongoCppLegacyEntityBatchDelete(orionldState.requestTree) == false)
  {
    LM_E(("mongoCppLegacyEntityBatchDelete returned false"));
    ciP->httpStatusCode = SccBadRequest;
    if (orionldState.responseTree == NULL)
      orionldErrorResponseCreate(OrionldBadRequestData, "Database Error", "mongoCppLegacyEntityBatchDelete");
    return false;
  }
  else
  {
    orionldState.responseTree = kjObject(orionldState.kjsonP, NULL);

    kjChildAdd(orionldState.responseTree, success);
    kjChildAdd(orionldState.responseTree, errors);    
    ciP->httpStatusCode = SccOk;
  }

  return true;
}
