/*
*
* Copyright 2019 Telefonica Investigacion y Desarrollo, S.A.U
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
* Author: Larysse Savanna
*/
#include "mongo/client/dbclient.h"                                       // MongoDB C++ Client Legacy Driver

extern "C"
{
#include "kjson/KjNode.h"                                                // KjNode
#include "kjson/kjRender.h"                                              // kjRender - TMP
#include "kjson/kjBuilder.h"                                             // kjArray, ...
}

#include "logMsg/logMsg.h"                                               // LM_*
#include "logMsg/traceLevels.h"                                          // Lmt*

#include "mongoBackend/MongoGlobal.h"                                    // getMongoConnection, releaseMongoConnection, ...
#include "orionld/common/orionldState.h"                                 // orionldState, dbName, mongoEntitiesCollectionP
#include "orionld/db/dbCollectionPathGet.h"                              // dbCollectionPathGet
#include "orionld/db/dbConfiguration.h"                                  // dbDataToKjTree, dbDataFromKjTree
#include "orionld/mongoCppLegacy/mongoCppLegacyKjTreeFromBsonObj.h"      // mongoCppLegacyKjTreeFromBsonObj
#include "orionld/mongoCppLegacy/mongoCppLegacyQueryEntitiesAsKjTree.h"  // Own interface



// -----------------------------------------------------------------------------
//
// mongoCppLegacyQueryEntitiesAsKjTree -
//
KjNode* mongoCppLegacyQueryEntitiesAsKjTree(KjNode* entityIdsArray)
{
  char collectionPath[256];

  if (dbCollectionPathGet(collectionPath, sizeof(collectionPath), "entities") == -1)
  {
    LM_E(("Internal Error (dbCollectionPathGet returned -1)"));
    return NULL;
  }

  // LM_TMP(("Collection Path: %s", collectionPath));

  // Build the filter for the query
  mongo::BSONObjBuilder    filter;
  mongo::BSONObjBuilder    inObj;
  mongo::BSONArrayBuilder  idList;

  for (KjNode* idNodeP = entityIdsArray->value.firstChildP; idNodeP != NULL; idNodeP = idNodeP->next)
  {
    idList.append(idNodeP->value.s);
  }
  inObj.append("$in", idList.arr());
  filter.append("_id.id", inObj.obj());

  // Especify the filds to return
  mongo::BSONObjBuilder    fields;
  fields.append("_id.id", 1);
  fields.append("type", 1);
  fields.append("creDate", 1);

  mongo::BSONObj fieldsToReturn = fields.obj();


  mongo::DBClientBase* connectionP = getMongoConnection();
  std::auto_ptr<mongo::DBClientCursor>  cursorP;
  mongo::Query         query(filter.obj());

  // Debugging
  // LM_TMP(("LARYQUERY: filter: %s", query.toString().c_str()));

  cursorP = connectionP->query(collectionPath, query, 0, 0, &fieldsToReturn);

  // Now convert bson result to kjtree
  KjNode* entitiesTree = kjArray(orionldState.kjsonP, NULL);
  int limitOp = 0;

  while (cursorP->more())
  {
    mongo::BSONObj  bsonObj;
    KjNode*         entityObj;
    char*           title;
    char*           detail;

    bsonObj = cursorP->nextSafe();

    // LM_TMP(("BSONN: %s", bsonObj.toString().c_str()));
    entityObj = mongoCppLegacyKjTreeFromBsonObj(&bsonObj, &title, &detail);

    if (entityObj == NULL)
    {
      LM_E(("Unable to create KjNode tree from mongo::BSONObj '%s'", bsonObj.toString().c_str()));
      continue;
    }

    kjChildAdd(entitiesTree, entityObj);

    //
    // A limit of 100 entities has been established.
    //
    ++limitOp;
    if (limitOp >= 100)
    {
      LM_W(("Too many entities - breaking loop at 100"));
      break;
    }
    
  }

  releaseMongoConnection(connectionP);

  return entitiesTree;
}
