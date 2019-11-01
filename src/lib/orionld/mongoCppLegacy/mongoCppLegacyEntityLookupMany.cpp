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
* Author: Gabriel Quaresma
*/
#include "mongo/client/dbclient.h"                               // MongoDB C++ Client Legacy Driver

extern "C"
{
#include "kjson/KjNode.h"                                        // KjNode
#include "kjson/kjBuilder.h"                                     // kjArray
}

#include "logMsg/logMsg.h"                                       // LM_*
#include "logMsg/traceLevels.h"                                  // Lmt*

#include "mongoBackend/MongoGlobal.h"                            // getMongoConnection, releaseMongoConnection, ...
#include "orionld/common/orionldState.h"                         // orionldState, dbName, mongoEntitiesCollectionP
#include "orionld/db/dbCollectionPathGet.h"                      // dbCollectionPathGet
#include "orionld/db/dbConfiguration.h"                          // dbDataToKjTree
#include "orionld/mongoCppLegacy/mongoCppLegacyEntityLookupMany.h"   // Own interface



// ----------------------------------------------------------------------------
//
// mongoCppLegacyEntityLookupMany -
//
KjNode* mongoCppLegacyEntityLookupMany(KjNode* requestTree)
{
  char                        collectionPath[256];
  KjNode*                     resultArray = kjArray(orionldState.kjsonP, NULL);
  mongo::BSONObjBuilder       queryBuilderP;
  mongo::BSONObjBuilder       bsonInExpression;
  mongo::BSONArrayBuilder     bsonArray;

  dbCollectionPathGet(collectionPath, sizeof(collectionPath), "entities");
  LM_TMP(("DB: Collection Path: %s", collectionPath));
  
  for (KjNode* entityNodeP = requestTree->value.firstChildP; entityNodeP != NULL; entityNodeP = entityNodeP->next)
  {
    LM_TMP(("ENTITY ID: %s", entityNodeP->value.s));
    bsonArray.append(entityNodeP->value.s);
  }

  bsonInExpression.append("$in", bsonArray.arr());
  queryBuilderP.append("_id.id", bsonInExpression.obj());

  // semTake()
  mongo::DBClientBase*                  connectionP = getMongoConnection();
  std::auto_ptr<mongo::DBClientCursor>  cursorP;
  mongo::Query                          query(queryBuilderP.obj());

  // Debugging - see the query
  LM_TMP(("QUERY: %s", query.toString().c_str()));

  cursorP = connectionP->query(collectionPath, query);

  while (cursorP->more())
  {
    mongo::BSONObj  bsonObj;
    char*           title;
    char*           details;
    KjNode*         kjTree;

    bsonObj = cursorP->nextSafe();

    LM_TMP(("UPSERT: Creating a kjTree from BSONObj '%s'", bsonObj.toString().c_str()));

    kjTree = dbDataToKjTree(&bsonObj, &title, &details);
    if (kjTree == NULL)
      LM_E(("%s: %s", title, details));
    else
      kjChildAdd(resultArray, kjTree);
  }

  releaseMongoConnection(connectionP);
  // semGive()
  return  resultArray;
}
