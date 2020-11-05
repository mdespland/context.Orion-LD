#ifndef SRC_LIB_ORIONLD_TYPES_ORIONLDNOTIFICATION_H_
#define SRC_LIB_ORIONLD_TYPES_ORIONLDNOTIFICATION_H_

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
#include "kjson/KjNode.h"                                        // KjNode
}

#include "cache/subCache.h"                                      // CachedSubscription



// -----------------------------------------------------------------------------
//
// OrionldNotification -
//
// Members:
//   subP:        the matching subscription (q processing not included)
//   changeTree:  used to match subscriptions to the update and to filter out using 'q'
//   resultTree:  used to extract what is to be notified
//   fd:          the file descriptor used for sending the notification
//   allOk:       flag about success
//   next:        it's a linked list, 'next' points to the next item in the list
//
typedef struct OrionldNotification
{
  CachedSubscription*          subP;             // Pointer to the matched subscription
  KjNode*                      changeTree;       // Request tree, with errors removed
  KjNode*                      resultTree;       // Result of the merge between 'changeTree' and what was found in the database
  int                          fd;               // File descriptor used during notification
  bool                         allOK;            // The response was read and it's a 200 OK
  struct OrionldNotification*  next;             // Next OrionldNotification in the list
} OrionldNotification;

#endif  // SRC_LIB_ORIONLD_TYPES_ORIONLDNOTIFICATION_H_
