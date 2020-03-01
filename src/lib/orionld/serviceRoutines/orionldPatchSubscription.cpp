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
#include "kjson/kjLookup.h"                                    // kjLookup
#include "kjson/kjBuilder.h"                                   // kjChildAdd, ...
#include "kjson/kjRender.h"                                    // kjRender
}

#include "logMsg/logMsg.h"                                     // LM_*
#include "logMsg/traceLevels.h"                                // Lmt*

#include "rest/ConnectionInfo.h"                               // ConnectionInfo

#include "orionld/common/CHECK.h"                              // STRING_CHECK, ...
#include "orionld/common/orionldState.h"                       // orionldState
#include "orionld/common/orionldErrorResponse.h"               // orionldErrorResponseCreate
#include "orionld/context/orionldContextItemExpand.h"          // orionldContextItemExpand
#include "orionld/payloadCheck/pcheckEntityInfo.h"             // pcheckEntityInfo
#include "orionld/db/dbConfiguration.h"                        // dbSubscriptionGet
#include "orionld/serviceRoutines/orionldPatchSubscription.h"  // Own Interface



// -----------------------------------------------------------------------------
//
// ngsildCoordinatesToAPIv1Datamodel -
//
static bool ngsildCoordinatesToAPIv1Datamodel(ConnectionInfo* ciP, KjNode* coordinatesP, const char* fieldName, KjNode* geometryP)
{
  bool   isPoint = false;
  char*  buf;

  if (geometryP == NULL)
  {
    orionldErrorResponseCreate(OrionldBadRequestData, "Internal Error", "Unable to extract the geometry of a geoQ for coordinmate APIv1 fix");
    ciP->httpStatusCode = SccBadRequest;
    return false;
  }

  if (strcmp(geometryP->value.s, "Point") == 0)
    isPoint = true;

  if (isPoint)
  {
    // A point is an array ( [ 1, 2 ] ) in NGSI-LD, but in APIv1 database mode it is a string ( "1,2" )
    int    coords = 0;
    float  coordV[3];

    for (KjNode* coordP = coordinatesP->value.firstChildP; coordP != NULL; coordP = coordP->next)
    {
      if (coordP->type == KjFloat)
        coordV[coords] = coordP->value.f;
      else
        coordV[coords] = (float) coordP->value.i;

      ++coords;
    }

    buf = kaAlloc(&orionldState.kalloc, 128);
    if (coords == 2)
      snprintf(buf, 128, "%f,%f", coordV[0], coordV[1]);
    else
      snprintf(buf, 128, "%f,%f,%f", coordV[0], coordV[1], coordV[2]);
  }
  else
  {
    buf = kaAlloc(&orionldState.kalloc, 1024);
    kjRender(orionldState.kjsonP, coordinatesP, buf, 1024);
  }

  coordinatesP->name    = (char*) "coords";
  coordinatesP->type    = KjString;
  coordinatesP->value.s = buf;

  return true;
}



// -----------------------------------------------------------------------------
//
// orionldCheckGeoQ -
//
static bool orionldCheckGeoQ(ConnectionInfo* ciP, KjNode* geoqNodeP, const char* fieldName)
{
  //
  // Render the coordinates and convert it into a string - for the NGSIv1 database model
  //
  KjNode* geometryP    = kjLookup(geoqNodeP, "geometry");
  KjNode* coordinatesP = kjLookup(geoqNodeP, "coordinates");

  if ((coordinatesP != NULL) && (coordinatesP->type == KjArray))
  {
    if (ngsildCoordinatesToAPIv1Datamodel(ciP, coordinatesP, "geoQ::coordinates", geometryP) == false)
      return false;
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// orionldCheckEndpoint -
//
bool orionldCheckEndpoint(ConnectionInfo* ciP, KjNode* endpointP, const char* fieldName)
{
  KjNode* uriP    = NULL;
  KjNode* acceptP = NULL;

  for (KjNode* epItemP = endpointP->value.firstChildP; epItemP != NULL; epItemP = epItemP->next)
  {
    if (strcmp(epItemP->name, "uri") == 0)
    {
      DUPLICATE_CHECK(uriP, "endpoint::uri", epItemP);
      STRING_CHECK(uriP, "endpoint::uri");
      URI_CHECK(uriP, "endpoint::uri");
    }
    else if (strcmp(epItemP->name, "accept") == 0)
    {
      DUPLICATE_CHECK(acceptP, "endpoint::accept", epItemP);
      STRING_CHECK(acceptP, "endpoint::accept");
      if ((strcmp(acceptP->value.s, "application/json") != 0) && (strcmp(acceptP->value.s, "application/ld+json") != 0))
      {
        orionldErrorResponseCreate(OrionldBadRequestData, "Unsupported Mime-type in 'accept'", epItemP->value.s);
        ciP->httpStatusCode = SccBadRequest;
        return false;
      }
    }
    else
    {
      orionldErrorResponseCreate(OrionldBadRequestData, "Invalid field for 'endpoint'", epItemP->name);
      ciP->httpStatusCode = SccBadRequest;
      return false;
    }
  }

  if (uriP == NULL)
  {
    orionldErrorResponseCreate(OrionldBadRequestData, "Mandatory field missing in 'endpoint'", "uri");
    ciP->httpStatusCode = SccBadRequest;
    return false;
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// orionldCheckNotification -
//
bool orionldCheckNotification(ConnectionInfo* ciP, KjNode* notificationP, const char* fieldName)
{
  KjNode* attributesP = NULL;
  KjNode* formatP     = NULL;
  KjNode* endpointP   = NULL;

  for (KjNode* nItemP = notificationP->value.firstChildP; nItemP != NULL; nItemP = nItemP->next)
  {
    if (strcmp(nItemP->name, "attributes") == 0)
    {
      DUPLICATE_CHECK(attributesP, "attributes", nItemP);
      ARRAY_CHECK(nItemP, "attributes");

      for (KjNode* attrP = nItemP->value.firstChildP; attrP != NULL; attrP = attrP->next)
      {
        STRING_CHECK(attrP, "attributes array item");
        attrP->value.s = orionldContextItemExpand(orionldState.contextP, attrP->value.s, NULL, true, NULL);
      }
    }
    else if (strcmp(nItemP->name, "format") == 0)
    {
      DUPLICATE_CHECK(formatP, "format", nItemP);
      STRING_CHECK(formatP, "format");
      if ((strcmp(formatP->value.s, "keyValues") != 0) && (strcmp(formatP->value.s, "normalized") != 0))
      {
        orionldErrorResponseCreate(OrionldBadRequestData, "Invalid value of 'format' (must be either 'keyValues' or 'normalized'", formatP->value.s);
        ciP->httpStatusCode = SccBadRequest;
        return false;
      }
    }
    else if (strcmp(nItemP->name, "endpoint") == 0)
    {
      DUPLICATE_CHECK(endpointP, "endpoint", nItemP);
      OBJECT_CHECK(endpointP, "endpoint");
      if (orionldCheckEndpoint(ciP, endpointP, "endpoint") == false)
        return false;
    }
    else if (strcmp(nItemP->name, "status") == 0)
    {
      orionldErrorResponseCreate(OrionldBadRequestData, "Invalid field for notification", "'status' is read-only");
      ciP->httpStatusCode = SccBadRequest;
      return false;
    }
    else
    {
      orionldErrorResponseCreate(OrionldBadRequestData, "Invalid field for notification", nItemP->name);
      ciP->httpStatusCode = SccBadRequest;
      return false;
    }
  }

  if (endpointP == NULL)
  {
    orionldErrorResponseCreate(OrionldBadRequestData, "Mandatory field missing", "endpoint");
    ciP->httpStatusCode = SccBadRequest;
    return false;
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// subscriptionPayloadCheck -
//
extern bool qAliasCompact(KjNode* qP, bool compact);
static bool subscriptionPayloadCheck(ConnectionInfo* ciP, KjNode* subNodeP, bool idCanBePresent, KjNode** watchedAttributesPP, KjNode** timeIntervalPP, KjNode** qPP, KjNode** geoqPP)
{
  KjNode* idP                     = NULL;
  KjNode* typeP                   = NULL;
  KjNode* nameP                   = NULL;
  KjNode* descriptionP            = NULL;
  KjNode* entitiesP               = NULL;
  KjNode* watchedAttributesP      = NULL;
  KjNode* timeIntervalP           = NULL;
  KjNode* qP                      = NULL;
  KjNode* geoqP                   = NULL;
  KjNode* csfP                    = NULL;
  KjNode* isActiveP               = NULL;
  KjNode* notificationP           = NULL;
  KjNode* expiresP                = NULL;
  KjNode* throttlingP             = NULL;
  KjNode* temporalqP              = NULL;
  int64_t dateTime;

  if (subNodeP->type != KjObject)
  {
    orionldErrorResponseCreate(OrionldBadRequestData, "Invalid Subscription", "The payload data for updating a subscription must be a JSON Object");
    ciP->httpStatusCode = SccBadRequest;
    return false;
  }

  for (KjNode* nodeP = subNodeP->value.firstChildP; nodeP != NULL; nodeP = nodeP->next)
  {
    if (strcmp(nodeP->name, "id") == 0)
    {
      if (idCanBePresent == false)
      {
        orionldErrorResponseCreate(OrionldBadRequestData, "The Subscription ID cannot be modified", "Subscription::id");
        ciP->httpStatusCode = SccBadRequest;
        return false;
      }

      DUPLICATE_CHECK(idP, "Subscription::id", nodeP);
      STRING_CHECK(nodeP, nodeP->name);
      URI_CHECK(nodeP, nodeP->name);
    }
    else if (strcmp(nodeP->name, "type") == 0)
    {
      DUPLICATE_CHECK(typeP, "Subscription::type", nodeP);
      STRING_CHECK(nodeP, nodeP->name);

      if (strcmp(nodeP->value.s, "Subscription") != 0)
      {
        orionldErrorResponseCreate(OrionldBadRequestData, "Invalid value for Subscription Type", nodeP->value.s);
        ciP->httpStatusCode = SccBadRequest;
        return false;
      }
    }
    else if (strcmp(nodeP->name, "name") == 0)
    {
      DUPLICATE_CHECK(nameP, "Subscription::name", nodeP);
      STRING_CHECK(nodeP, nodeP->name);
    }
    else if (strcmp(nodeP->name, "description") == 0)
    {
      DUPLICATE_CHECK(descriptionP, "Subscription::description", nodeP);
      STRING_CHECK(nodeP, nodeP->name);
    }
    else if (strcmp(nodeP->name, "entities") == 0)
    {
      DUPLICATE_CHECK(entitiesP, "Subscription::entities", nodeP);
      ARRAY_CHECK(nodeP, nodeP->name);
      for (KjNode* entityP = nodeP->value.firstChildP; entityP != NULL; entityP = entityP->next)
      {
        OBJECT_CHECK(entityP, "Subscription::entities[X]");
        if (pcheckEntityInfo(ciP, entityP) == false)
          return false;
      }
    }
    else if (strcmp(nodeP->name, "watchedAttributes") == 0)
    {
      DUPLICATE_CHECK(watchedAttributesP, "Subscription::watchedAttributes", nodeP);
      ARRAY_CHECK(nodeP, nodeP->name);
      for (KjNode* itemP = nodeP->value.firstChildP; itemP != NULL; itemP = itemP->next)
      {
        STRING_CHECK(itemP, "watchedAttributes item");
        itemP->value.s = orionldContextItemExpand(orionldState.contextP, itemP->value.s, NULL, true, NULL);
      }
      *watchedAttributesPP = watchedAttributesP;
    }
    else if (strcmp(nodeP->name, "timeInterval") == 0)
    {
      DUPLICATE_CHECK(timeIntervalP, "Subscription::timeInterval", nodeP);
      INTEGER_CHECK(nodeP, "Subscription::timeInterval");
      *timeIntervalPP = timeIntervalP;
    }
    else if (strcmp(nodeP->name, "q") == 0)
    {
      DUPLICATE_CHECK(qP, "Subscription::q", nodeP);
      STRING_CHECK(nodeP, "Subscription::q");
      *qPP = qP;
      LM_TMP(("QP: qP at %p", qP));
      qAliasCompact(qP, false);
    }
    else if (strcmp(nodeP->name, "geoQ") == 0)
    {
      DUPLICATE_CHECK(geoqP, "Subscription::geoQ", nodeP);
      OBJECT_CHECK(nodeP, "Subscription::geoQ");
      if (orionldCheckGeoQ(ciP, nodeP, "Subscription::geoQ") == false)
        return false;
      *geoqPP = geoqP;
      LM_TMP(("QP: geoqP at %p", geoqP));
    }
    else if (strcmp(nodeP->name, "csf") == 0)
    {
      DUPLICATE_CHECK(csfP, "Subscription::csf", nodeP);
      STRING_CHECK(nodeP, "Subscription::csf");
    }
    else if (strcmp(nodeP->name, "isActive") == 0)
    {
      DUPLICATE_CHECK(isActiveP, "Subscription::isActive", nodeP);
      BOOL_CHECK(nodeP, "Subscription::isActive");
    }
    else if (strcmp(nodeP->name, "notification") == 0)
    {
      DUPLICATE_CHECK(notificationP, "Subscription::notification", nodeP);
      OBJECT_CHECK(nodeP, "Subscription::notification");
    }
    else if (strcmp(nodeP->name, "expires") == 0)
    {
      DUPLICATE_CHECK(expiresP, "Subscription::expires", nodeP);
      STRING_CHECK(nodeP, "Subscription::expires");
      DATETIME_CHECK(expiresP->value.s, dateTime, "Subscription::expires");
      LM_TMP(("DT: Subscription::expires: %d", dateTime));
    }
    else if (strcmp(nodeP->name, "throttling") == 0)
    {
      DUPLICATE_CHECK(throttlingP, "Subscription::throttling", nodeP);
      INTEGER_CHECK(nodeP, "Subscription::throttling");
    }
    else if (strcmp(nodeP->name, "temporalQ") == 0)
    {
      DUPLICATE_CHECK(temporalqP, "Subscription::temporalQ", nodeP);
      OBJECT_CHECK(nodeP, "Subscription::temporalQ");
    }
    else if (strcmp(nodeP->name, "status") == 0)
    {
      orionldErrorResponseCreate(OrionldBadRequestData, "Attempt to modify Read-Only attribute", "Subscription::status");
      ciP->httpStatusCode = SccBadRequest;
      return false;
    }
    else
    {
      LM_E(("Unknown field in Subscription fragment: '%s'", nodeP->name));
      orionldErrorResponseCreate(OrionldBadRequestData, "Unknown field in Subscription fragment", nodeP->name);
      ciP->httpStatusCode = SccBadRequest;
      return false;
    }
  }

  if ((notificationP != NULL) && (orionldCheckNotification(ciP, notificationP, "subscription::notification") == false))
      return false;

  return true;
}



// -----------------------------------------------------------------------------
//
// kjChildAddOrReplace -
//
// FIXME: move to kjson library - also used in orionldPatchRegistration.cpp
//
void kjChildAddOrReplace(KjNode* container, const char* itemName, KjNode* replacementP)
{
  KjNode* itemToReplace = kjLookup(container, itemName);

  if (itemToReplace == NULL)
  {
    LM_TMP(("QP: Adding '%s' to container '%s'", itemName, container->name));
    kjChildAdd(container, replacementP);
  }
  else
  {
    LM_TMP(("QP: Replacing '%s' in container '%s'", itemName, container->name));
    itemToReplace->type  = replacementP->type;
    itemToReplace->value = replacementP->value;
    // KjNode::cSum and KjNode::valueString aren't used
  }
}



// ----------------------------------------------------------------------------
//
// okToRemove -
//
static bool okToRemove(const char* fieldName)
{
  if (strcmp(fieldName, "id") == 0)
    return false;
  else if (strcmp(fieldName, "notification") == 0)
    return false;
  else if (strcmp(fieldName, "status") == 0)
    return false;

  return true;
}



// ----------------------------------------------------------------------------
//
// ngsildSubscriptionPatch -
//
// The 'q' and 'geoQ' of an NGSI-LD comes in like this:
// {
//   "q": "",
//   "geoQ": {
//     "geometry": "",
//     ""
// }
//
// In the DB, 'q' and 'geoQ' are inside "expression":
// {
//   "expression" : {
//     "q" : "https://uri=etsi=org/ngsi-ld/default-context/P2>10",
//     "mq" : "",
//     "geometry" : "circle",
//     "coords" : "1,2",
//     "georel" : "near"
//   }
// }
//
// So, if "geoQ" is present in the patch tree, then "geoQ" replaces "expression",
// by simply changing its name from "geoQ" to "expression".
// DON'T forget the "q", that is also part of "expression" but not a part of "geoQ".
// If "geoQ" replaces "expression", then we may need to maintain the "q" inside the old "expression".
// OR, if "q" is also in the patch tree, then we'll simply move it inside "expression" (former "geoQ").
//
//
//
//
//
static bool ngsildSubscriptionPatch(ConnectionInfo* ciP, KjNode* dbSubscriptionP, KjNode* patchTree, KjNode* qP, KjNode* expressionP)
{
  KjNode* fragmentP = patchTree->value.firstChildP;
  KjNode* next;

  while (fragmentP != NULL)
  {
    next = fragmentP->next;

    if (fragmentP->type == KjNull)
    {
      KjNode* toRemove = kjLookup(dbSubscriptionP, fragmentP->name);

      if (toRemove != NULL)
      {
        if (okToRemove(fragmentP->name) == false)
        {
          orionldErrorResponseCreate(OrionldBadRequestData, "Invalid Subscription Fragment - attempt to remove a mandatory field", fragmentP->name);
          ciP->httpStatusCode = SccBadRequest;
          return false;
        }

        LM_TMP(("SPAT: Calling kjChildRemove for '%s'", fragmentP->name));
        kjChildRemove(dbSubscriptionP, toRemove);
      }
      else
        LM_TMP(("SPAT: Can't remove '%s' - it's not present in the DB", fragmentP->name));
    }
    else
    {
      if ((fragmentP != qP) && (fragmentP != expressionP))
      {
        LM_TMP(("SPAT: Calling kjChildAddOrReplace for '%s'", fragmentP->name));
        kjChildAddOrReplace(dbSubscriptionP, fragmentP->name, fragmentP);
      }
    }

    fragmentP = next;
  }


  LM_TMP(("QP: qP at %p: %s", qP, (qP != NULL)? qP->value.s : "not present"));
  LM_TMP(("QP: geoqP at %p", expressionP));

  //
  // If geoqP/expressionP != NULL, then it replaces the "expression" in the DB
  // If also qP != NULL, then this qP is added to geoqP/expressionP
  // If not, we have to lookup 'q' in the old "expression" and add it to geoqP/expressionP
  //
  if (expressionP != NULL)
  {
    KjNode* dbExpressionP = kjLookup(dbSubscriptionP, "expression");

    if (dbExpressionP != NULL)
      kjChildRemove(dbSubscriptionP, dbExpressionP);
    kjChildRemove(patchTree, expressionP);
    kjChildAdd(dbSubscriptionP, expressionP);

    //
    // If 'q' is not present in the patch tree, and we have replaced the 'expression', then
    // we need to get the 'q' from the old 'expression' and add it to the new expression.
    //
    if ((qP == NULL) && (dbExpressionP != NULL))
      qP = kjLookup(dbExpressionP, "q");

    if (qP != NULL)
      kjChildAdd(expressionP, qP);
  }
  else if (qP != NULL)
  {
    KjNode* dbExpressionP = kjLookup(dbSubscriptionP, "expression");

    if (dbExpressionP != NULL)
      kjChildAddOrReplace(dbExpressionP, "q", qP);
    else
    {
      // A 'q' has been given but there is no "expression" - need to create one
      expressionP = kjObject(orionldState.kjsonP, "expression");

      kjChildAdd(expressionP, qP);
      kjChildAdd(dbSubscriptionP, expressionP);
    }
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// ngsildSubscriptionToAPIv1Datamodel -
//
// For the PATCH (step 5) the difference of the APIv1 database model and the NGSI-LD payload data for a Subscription:
//
// PAYLOAD of an NGSI-LD Subscription
// {
//   "id": "urn:ngsi-ld:subscriptions:01",     => "id" => "_id"
//   "type": "Subscription",                   => Not in DB and not needed
//   "name": "Test subscription S01",          => NEW and added to the datamodel
//   "description": "XXX",                     => NEW and added to the datamodel
//   "entities": [                             => SAME, but remember, "isTypePattern" : false (default value)
//     {
//       "id": "urn:ngsi-ld:E01",
//       "type": "T1"
//     }
//   ],
//   "watchedAttributes": [ "P2" ],           => "watchedAttributes" => "conditions"
//   "q": "P2>10",                            => "q" => "expression.q"
//   "geoQ": {                                => disappears: its children go into "expression"
//     "geometry": "circle",                  => "geoQ.geometry" => "expression.geometry"
//     "coordinates": "1,2",                  => "geoQ.coordinates" => "expression.coords"
//     "georel": "near",                      => "geoQ.georel" => "expression.georel"
//     "geoproperty": "not supported"         => MUST BE ADDED: expression.geoproperty
//   },
//   "csf": "not implemented",                => NEW and added to the datamodel
//   "isActive": false,                       => "isActive" => "status" (== "inactive" or "active")
//   "notification": {                        => disappears: its children go into other fields
//     "attributes": [ "P1", "P2", "A3" ],    => "notification.attributes" => "attrs"
//     "format": "keyValues",                 => "notification.format" => "format"
//     "endpoint": {                          => disappears: its children go into other fields
//       "uri": "http://valid.url/url",       => "endpoint.uri" => "reference"
//       "accept": "application/ld+json"      => "endpoint.accept" => "mimeType"
//     }
//   },
//   "expires": "2028-12-31T10:00:00",        => "expires" => "expiration"
//   "throttling": 5                          => SAME
// }
//
// The subscription saved in mongo (APIv1) looks like this:
//
// {
//   "_id" : "urn:ngsi-ld:subscriptions:01",
//   "expiration" : NumberLong(1861869600),
//   "reference" : "http://valid.url/url",
//   "custom" : false,
//   "mimeType" : "application/ld+json",
//   "throttling" : NumberLong(5),
//   "servicePath" : "/",
//   "status" : "inactive",
//   "entities" : [
//     {
//       "id" : "urn:ngsi-ld:E01",
//       "isPattern" : "false",
//       "type" : "https://uri.etsi.org/ngsi-ld/default-context/T1",
//       "isTypePattern" : false
//     }
//   ],
//   "attrs" : [
//     "https://uri.etsi.org/ngsi-ld/default-context/P1",
//     "https://uri.etsi.org/ngsi-ld/default-context/P2",
//     "https://uri.etsi.org/ngsi-ld/default-context/A3"
//   ],
//   "metadata" : [ ],
//   "blacklist" : false,
//   "name" : "Test subscription S01",
//   "ldContext" : "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
//   "conditions" : [
//     "https://uri.etsi.org/ngsi-ld/default-context/P2"
//   ],
//   "expression" : {
//     "q" : "https://uri=etsi=org/ngsi-ld/default-context/P2>10",
//     "mq" : "",
//     "geometry" : "circle",
//     "coords" : "1,2",
//     "georel" : "near"
//   },
//   "format" : "keyValues"
// }
//
static bool ngsildSubscriptionToAPIv1Datamodel(KjNode* patchTree)
{
  KjNode* qP            = NULL;
  KjNode* geoqP         = NULL;
  KjNode* notificationP = NULL;

  //
  // Loop over the patch-tree and modify to make it compatible with the database model for APIv1
  //
  for (KjNode* fragmentP = patchTree->value.firstChildP; fragmentP != NULL; fragmentP = fragmentP->next)
  {
    if (strcmp(fragmentP->name, "type") == 0)
    {
      // Just skip it - don't want "type: Subscription" in the DB. Not needed
    }
    else if (strcmp(fragmentP->name, "entities") == 0)
    {
      // Make sure there is an "id" and an "isPattern"
      for (KjNode* entityNodeP = fragmentP->value.firstChildP; entityNodeP != NULL; entityNodeP = entityNodeP->next)
      {
        KjNode* isTypePatternP = kjBoolean(orionldState.kjsonP, "isTypePattern", false);
        KjNode* idP            = kjLookup(entityNodeP, "id");
        KjNode* idPatternP     = kjLookup(entityNodeP, "idPattern");

        if ((idP == NULL) && (idPatternP == NULL))
        {
          KjNode* idNodeP        = kjString(orionldState.kjsonP, "id", ".*");
          KjNode* isPatternNodeP = kjString(orionldState.kjsonP, "isPattern", "true");

          kjChildAdd(entityNodeP, idNodeP);
          kjChildAdd(entityNodeP, isPatternNodeP);
        }
        else if (idP == NULL)
        {
          KjNode* isPatternNodeP = kjString(orionldState.kjsonP, "isPattern", "true");
          kjChildAdd(entityNodeP, isPatternNodeP);
          idPatternP->name = (char*) "id";
        }
        else if (idPatternP == NULL)
        {
          KjNode* isPatternNodeP = kjString(orionldState.kjsonP, "isPattern", "false");
          kjChildAdd(entityNodeP, isPatternNodeP);
        }

        kjChildAdd(entityNodeP, isTypePatternP);
      }
    }
    else if (strcmp(fragmentP->name, "watchedAttributes") == 0)
      fragmentP->name = (char*) "conditions";
    else if (strcmp(fragmentP->name, "q") == 0)
      qP = fragmentP;
    else if (strcmp(fragmentP->name, "geoQ") == 0)
      geoqP = fragmentP;
    else if (strcmp(fragmentP->name, "isActive") == 0)
    {
      //
      // Must change name to "status" and change type from Bool to String
      //
      fragmentP->name = (char*) "status";
      fragmentP->type = KjString;

      fragmentP->value.s = (fragmentP->value.b == true)? (char*) "active" : (char*) "inactive";
    }
    else if (strcmp(fragmentP->name, "notification") == 0)
      notificationP = fragmentP;
    else if (strcmp(fragmentP->name, "expires") == 0)
    {
      fragmentP->name    = (char*) "expiration";
      fragmentP->type    = KjInt;
      fragmentP->value.i = parse8601Time(fragmentP->value.s);  // FIXME: Already done in subscriptionPayloadCheck() ...
    }
  }

  if (geoqP != NULL)
    geoqP->name = (char*) "expression";

  if (qP != NULL)
    kjChildRemove(patchTree, qP);


  //
  // The "notification" field is also treated after the loop, just to make the loop "nicer"
  // As the "notification" object must be removed from the tree and all its children moved elsewhere
  // it's much better to not do this inside the loop. Especially as the tree must be modified and a for-loop
  // would no longer be possible
  //
  if (notificationP != NULL)
  {
    KjNode* nItemP = notificationP->value.firstChildP;
    KjNode* next;

    // Loop over the "notification" fields and put them where they should be (according to the APIv1 database model)
    while (nItemP != NULL)
    {
      next = nItemP->next;

      if (strcmp(nItemP->name, "attributes") == 0)
      {
        // Change name to "attrs" and move up to toplevel
        nItemP->name = (char*) "attrs";
        kjChildAdd(patchTree, nItemP);
      }
      else if (strcmp(nItemP->name, "format") == 0)
      {
        // Keep the name, just move the node up to toplevel
        kjChildAdd(patchTree, nItemP);
      }
      else if (strcmp(nItemP->name, "endpoint") == 0)
      {
        KjNode* uriP    = kjLookup(nItemP, "uri");
        KjNode* acceptP = kjLookup(nItemP, "accept");

        kjChildRemove(notificationP, nItemP);
        if (uriP != NULL)
        {
          uriP->name = (char*) "reference";
          kjChildAdd(patchTree, uriP);
        }

        if (acceptP != NULL)
        {
          acceptP->name = (char*) "mimeType";
          kjChildAdd(patchTree, acceptP);
        }
      }

      nItemP = next;
    }

    // Finally, remove the "notification" item from the patch-tree
    kjChildRemove(patchTree, notificationP);
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// fixDbSubscription -
//
// As long long members are respresented as "xxx": { "$numberLong": "1234565678901234" }
// and this gives an error when trying to Update this, we simply change the object to an int.
//
// In a Subscription, this must be done for "expiration", and "throttling".
//
static void fixDbSubscription(KjNode* dbSubscriptionP)
{
  KjNode* nodeP;

  //
  // If 'expiration' is an Object, it means it's a NumberLong and it is then changed to a 32 bit integer
  //
  if ((nodeP = kjLookup(dbSubscriptionP, "expiration")) != NULL)
  {
    if (nodeP->type == KjObject)
    {
      char*      expirationString = nodeP->value.firstChildP->value.s;
      long long  expiration       = strtol(expirationString, NULL, 10);

      nodeP->type    = KjInt;
      nodeP->value.i = expiration;
    }
  }

  //
  // If 'throttling' is an Object, it means it's a NumberLong and it is then changed to a 32 bit integer
  //
  if ((nodeP = kjLookup(dbSubscriptionP, "throttling")) != NULL)
  {
    if (nodeP->type == KjObject)
    {
      char*      throttlingString = nodeP->value.firstChildP->value.s;
      long long  throttling       = strtol(throttlingString, NULL, 10);

      nodeP->type    = KjInt;
      nodeP->value.i = throttling;
    }
  }
}



// ----------------------------------------------------------------------------
//
// orionldPatchSubscription -
//
// 1. Check that orionldState.wildcard[0] is a valid subscription ID (a URI) - 400 Bad Request ?
// 2. Make sure the payload data is a correct Subscription fragment
//    - No values can be NULL
//    - Expand attribute names ans entity types if present
// 3. GET the subscription from mongo, by callinbg dbSubscriptionGet(orionldState.wildcard[0])
// 4. If not found - 404
//
// 5. Go over the fragment (incoming payload data) and modify the 'subscription from mongo':
//    * If the provided Fragment (merge patch) contains members that do not appear within the target (their URIs do
//      not match), those members are added to the target.
//    * the target member value is replaced by value given in the Fragment, if non-null values.
//    * If null values in the Fragment, then remove in the target
//
// 6. Call dbSubscriptionReplace(char* subscriptionId, KjNode* subscriptionTree) to replace the old sub with the new
//    Or, dbSubscriptionUpdate(char* subscriptionId, KjNode* toAddP, KjNode* toRemoveP, KjNode* toUpdate)
//
bool orionldPatchSubscription(ConnectionInfo* ciP)
{
  char* subscriptionId = orionldState.wildcard[0];
  char buffer[1024];

  kjRender(orionldState.kjsonP, orionldState.requestTree, buffer, sizeof(buffer));
  LM_TMP(("patch: '%s'", buffer));

  LM_TMP(("SPAT: orionldPatchSubscription: subscriptionId == '%s'", subscriptionId));

  if ((urlCheck(subscriptionId, NULL) == false) && (urnCheck(subscriptionId, NULL) == false))
  {
    ciP->httpStatusCode = SccBadRequest;
    orionldErrorResponseCreate(OrionldBadRequestData, "Subscription ID must be a valid URI", subscriptionId);
    return false;
  }

  KjNode* watchedAttributesNodeP = NULL;
  KjNode* timeIntervalNodeP      = NULL;
  KjNode* qP                     = NULL;
  KjNode* geoqP                  = NULL;

  LM_TMP(("SPAT: Calling subscriptionPayloadCheck"));
  if (subscriptionPayloadCheck(ciP, orionldState.requestTree, false, &watchedAttributesNodeP, &timeIntervalNodeP, &qP, &geoqP) == false)
  {
    LM_E(("subscriptionPayloadCheck FAILED"));
    return false;
  }

  LM_TMP(("SPAT: Calling dbSubscriptionGet"));
  KjNode* dbSubscriptionP = dbSubscriptionGet(subscriptionId);

  // <DEBUG>
  kjRender(orionldState.kjsonP, dbSubscriptionP, buffer, sizeof(buffer));
  LM_TMP(("SPAT: DB tree: '%s'", buffer));
  // </DEBUG>
  LM_TMP(("SPAT: dbSubscriptionGet returned: %p", dbSubscriptionP));

  if (dbSubscriptionP == NULL)
  {
    ciP->httpStatusCode = SccNotFound;
    orionldErrorResponseCreate(OrionldBadRequestData, "Subscription not found", subscriptionId);
    return false;
  }

  //
  // Make sure we don't get both watchedAttributed AND timeInterval
  // If so, the PATCH is invalid
  //
  if ((watchedAttributesNodeP != NULL) && (timeIntervalNodeP != NULL))
  {
    LM_W(("Bad Input (Both 'watchedAttributes' and 'timeInterval' given in Subscription Payload Data)"));
    ciP->httpStatusCode = SccBadRequest;
    orionldErrorResponseCreate(OrionldBadRequestData, "Invalid Subscription Payload Data", "Both 'watchedAttributes' and 'timeInterval' given");
    return false;
  }
  else if (watchedAttributesNodeP != NULL)
  {
    KjNode* dbTimeIntervalNodeP = kjLookup(dbSubscriptionP, "timeInterval");

    if ((dbTimeIntervalNodeP != NULL) && (dbTimeIntervalNodeP->value.i != -1))
    {
      LM_W(("Bad Input (Attempt to set 'watchedAttributes' to a Subscription that is of type 'timeInterval'"));
      ciP->httpStatusCode = SccBadRequest;
      orionldErrorResponseCreate(OrionldBadRequestData, "Invalid Subscription Payload Data", "Attempt to set 'watchedAttributes' to a Subscription that is of type 'timeInterval'");
      return false;
    }
  }
  else if (timeIntervalNodeP != NULL)
  {
    KjNode* dbConditionsNodeP = kjLookup(dbSubscriptionP, "conditions");

    if ((dbConditionsNodeP != NULL) && (dbConditionsNodeP->value.firstChildP != NULL))
    {
      LM_W(("Bad Input (Attempt to set 'timeInterval' to a Subscription that is of type 'watchedAttributes')"));
      ciP->httpStatusCode = SccBadRequest;
      orionldErrorResponseCreate(OrionldBadRequestData, "Invalid Subscription Payload Data", "Attempt to set 'timeInterval' to a Subscription that is of type 'watchedAttributes'");
      return false;
    }
  }


  //
  // Remove Occurrences of $numberLong, i.e. "expiration"
  //
  // FIXME: This is BAD ... shouldn't change the type of these fields
  //
  fixDbSubscription(dbSubscriptionP);


  LM_TMP(("SPAT: Converting the NGSI-LD Subscription into the APIv1 database model"));
  ngsildSubscriptionToAPIv1Datamodel(orionldState.requestTree);

  //
  // After calling ngsildSubscriptionToAPIv1Datamodel, the incoming payload data has beed structured just as the
  // API v1 database model and the original tree (obtained calling dbSubscriptionGet()) can easily be
  // modified.
  // ngsildSubscriptionPatch() performs that modification
  //
  LM_TMP(("SPAT: Going over the payload data to patch the subscription as a KjNode tree"));
  if (ngsildSubscriptionPatch(ciP, dbSubscriptionP, orionldState.requestTree, qP, geoqP) == false)
    return false;

  // <DEBUG>
  kjRender(orionldState.kjsonP, dbSubscriptionP, buffer, sizeof(buffer));
  LM_TMP(("SPAT: PATCHed tree: '%s'", buffer));
  // </DEBUG>

  //
  // Overwrite the current Subscription in the database
  //
  dbSubscriptionReplace(subscriptionId, dbSubscriptionP);

  // All OK? 204
  ciP->httpStatusCode = SccNoContent;

  return true;
}
