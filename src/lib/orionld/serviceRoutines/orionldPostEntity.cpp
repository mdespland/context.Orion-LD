/*
*
* Copyright 2018 Telefonica Investigacion y Desarrollo, S.A.U
*
* This file is part of Orion Context Broker.
*
* Orion Context Broker is free software: you can redistribute it and/or
* modify it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* Orion Context Broker is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero
* General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with Orion Context Broker. If not, see http://www.gnu.org/licenses/.
*
* For those usages not covered by this license please contact with
* iot_support at tid dot es
*
* Author: Ken Zangelin
*/
#include "logMsg/logMsg.h"                                     // LM_*
#include "logMsg/traceLevels.h"                                // Lmt*

#include "mongoBackend/mongoUpdateContext.h"                   // mongoUpdateContext
#include "ngsi10/UpdateContextRequest.h"                       // UpdateContextRequest
#include "ngsi10/UpdateContextResponse.h"                      // UpdateContextResponse
#include "rest/ConnectionInfo.h"                               // ConnectionInfo
#include "rest/httpHeaderAdd.h"                                // httpHeaderLinkAdd
#include "orionld/common/CHECK.h"                              // CHECK
#include "orionld/common/SCOMPARE.h"                           // SCOMPAREx
#include "orionld/common/orionldErrorResponse.h"               // orionldErrorResponseCreate
#include "orionld/context/orionldContextTreat.h"               // orionldContextTreat
#include "orionld/context/orionldUriExpand.h"                  // orionldUriExpand
#include "orionld/serviceRoutines/orionldPostEntity.h"         // Own Interface



// ----------------------------------------------------------------------------
//
// orionldPostEntity -
//
bool orionldPostEntity(ConnectionInfo* ciP)
{
  // 1. Check that the entity exists?
  OBJECT_CHECK(ciP->requestTree, "toplevel");

  UpdateContextRequest   mongoRequest;
  UpdateContextResponse  mongoResponse;
  ContextElement*        ceP       = new ContextElement();  // FIXME: Any way I can avoid to allocate ?
  EntityId*              entityIdP;

  mongoRequest.contextElementVector.push_back(ceP);
  entityIdP = &mongoRequest.contextElementVector[0]->entityId;
  mongoRequest.updateActionType = ActionTypeAppendStrict;      // So that is fails if the entity doesn't exist

  entityIdP->id = ciP->wildcard[0];


  //
  // 1. Lookup special nodes and actuate over them
  //   FIXME: Why not remove them from the tree? Only Properties/Relationships left after that ...
  //
  KjNode* contextNodeP = NULL;
  
  for (KjNode* kNodeP = ciP->requestTree->children; kNodeP != NULL; kNodeP = kNodeP->next)
  {
    if (SCOMPARE9(kNodeP->name, '@', 'c', 'o', 'n', 't', 'e', 'x', 't', 0))
    {
      ContextAttribute* caP;
      contextNodeP = kNodeP;

      // FIXME: Lookup context - it probably already exists (from the creation of the entity)
      
      if ((caP = orionldContextTreat(ciP, contextNodeP, (char*) entityIdP->id.c_str())) == NULL)
      {
        // Error payload set by orionldContextTreat
        mongoRequest.release();
        return false;
      }

      ceP->contextAttributeVector.push_back(caP);
      break;
    }    
  }

  // 3. Iterate over the object, to get all attributes
  for (KjNode* kNodeP = ciP->requestTree->children; kNodeP != NULL; kNodeP = kNodeP->next)
  {
    KjNode* attrTypeNodeP = NULL;

    if (kNodeP != contextNodeP)
    {
      ContextAttribute* caP = new ContextAttribute();

      // FIXME: Mode attributeTreat to separate file
      extern bool attributeTreat(ConnectionInfo* ciP, KjNode* kNodeP, ContextAttribute* caP, KjNode** typeNodePP);
      if (attributeTreat(ciP, kNodeP, caP, &attrTypeNodeP) == false)
      {
        mongoRequest.release();
        ciP->httpStatusCode = SccBadRequest;  // FIXME: Should be set inside 'attributeTreat' - could be 500, not 400 ...
        LM_TMP(("attributeTreat failed"));
        delete caP;
        return false;
      }

      //
      // URI Expansion for the attribute name, except if "location", "observationSpace", or "operationSpace"
      //
      if (SCOMPARE9(kNodeP->name,       'l', 'o', 'c', 'a', 't', 'i', 'o', 'n', 0))
        caP->name = kNodeP->name;
      else if (SCOMPARE17(kNodeP->name, 'o', 'b', 's', 'e', 'r', 'v', 'a', 't', 'i', 'o', 'n', 'S', 'p', 'a', 'c', 'e', 0))
        caP->name = kNodeP->name;
      else if (SCOMPARE15(kNodeP->name, 'o', 'p', 'e', 'r', 'a', 't', 'i', 'o', 'n', 'S', 'p', 'a', 'c', 'e', 0))
        caP->name = kNodeP->name;
      else
      {
        char  longName[256];
        char* details;

        if (orionldUriExpand(ciP->contextP, kNodeP->name, longName, sizeof(longName), &details) == false)
        {
          delete caP;
          mongoRequest.release();
          orionldErrorResponseCreate(ciP, OrionldBadRequestData, details, kNodeP->name, OrionldDetailsAttribute);
          return false;
        }

        caP->name = longName;
      }

      // NO URI Expansion for Attribute TYPE
      caP->type = attrTypeNodeP->value.s;

      // Add the attribute to the attr vector
      ceP->contextAttributeVector.push_back(caP);
    }
  }

  LM_TMP(("Calling mongoUpdateContext"));
  ciP->httpStatusCode = mongoUpdateContext(&mongoRequest,
                                           &mongoResponse,
                                           ciP->httpHeaders.tenant,
                                           ciP->servicePathV,
                                           ciP->uriParam,
                                           ciP->httpHeaders.xauthToken,
                                           ciP->httpHeaders.correlator,
                                           ciP->httpHeaders.ngsiv2AttrsFormat,
                                           ciP->apiVersion,
                                           NGSIV2_NO_FLAVOUR);
  LM_TMP(("mongoUpdateContext returned %d", ciP->httpStatusCode));
  mongoRequest.release();
  mongoResponse.release();

  if (ciP->httpStatusCode != SccOk)
  {
    LM_E(("mongoUpdateContext: HTTP Status Code: %d", ciP->httpStatusCode));
    orionldErrorResponseCreate(ciP, OrionldBadRequestData, "Internal Error", "Error from mongo backend", OrionldDetailsString);
    return false;
  }

  ciP->httpStatusCode = SccNoContent;
  httpHeaderLinkAdd(ciP, ciP->contextP, NULL);

  return true;
}