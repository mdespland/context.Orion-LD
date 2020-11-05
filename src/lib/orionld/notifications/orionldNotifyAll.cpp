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
#include <string.h>                                              // strlen
#include <sys/uio.h>                                             // writev
#include <sys/select.h>                                          // select

#include <string>                                                // std::string
#include <vector>                                                // std::vector

extern "C"
{
#include "kbase/kMacros.h"                                       // K_FT
#include "kalloc/kaAlloc.h"                                      // kaAlloc
#include "kalloc/kaStrdup.h"                                     // kaStrdup
#include "kjson/kjRender.h"                                      // kjRender
#include "kjson/kjBuilder.h"                                     // kjObject, kjArray, kjString, kjChildAdd, ...
#include "kjson/kjLookup.h"                                      // kjLookup
}

#include "logMsg/logMsg.h"
#include "logMsg/traceLevels.h"

#include "orionld/common/orionldState.h"                         // orionldState
#include "orionld/common/numberToDate.h"                         // numberToDate
#include "orionld/common/uuidGenerate.h"                         // uuidGenerate
#include "orionld/common/orionldServerConnect.h"                 // orionldServerConnect
#include "orionld/common/eqForDot.h"                             // eqForDot
#include "orionld/context/orionldCoreContext.h"                  // ORIONLD_CORE_CONTEXT_URL
#include "orionld/context/orionldContextItemAliasLookup.h"       // orionldContextItemAliasLookup
#include "orionld/notifications/orionldNotifyAll.h"              // Own interface



// -----------------------------------------------------------------------------
//
// ipPortAndRest - extract ip, port and URL-PATH from a 'reference' string
//
// FIXME
//   This function is generic and should be moved to its own module in orionld/common
//   However, I think I have a function doing exactly this already ...
//
static void ipPortAndRest(char* ipport, char** ipP, unsigned short* portP, char** restP)
{
  char*            colon;
  char*            ip;
  unsigned short   portNo  = 80;  // What should be the default port?
  char*            rest;

  //
  // Starts with http:// ...
  //
  ip = strchr(ipport, '/');
  ip += 2;
  rest = ip;

  colon = strchr(ip, ':');
  if (colon != NULL)
  {
    *colon = 0;
    portNo = atoi(&colon[1]);
    rest = &colon[1];
  }

  *ipP   = ip;
  *portP = portNo;

  rest = strchr(rest, '/');
  *restP = rest;
}



// -----------------------------------------------------------------------------
//
// attrIn -
//
static bool attrIn(const std::vector<std::string>& attrV, const char* attrName)
{
  unsigned int noOfAttrs = attrV.size();

  if (noOfAttrs == 0)
    return true;

  for (unsigned int ix = 0; ix < noOfAttrs; ix++)
  {
    LM_TMP(("NOTIF: Comparing attr: '%s' to array item '%s'", attrName, attrV[ix].c_str()));
    if (strcmp(attrV[ix].c_str(), attrName) == 0)
      return true;
  }

  return false;
}



// -----------------------------------------------------------------------------
//
// dbEntityToNgsildEntity -
//
KjNode* dbEntityToNgsildEntityWithAttrsFilter(KjNode* entityNodeP, const std::vector<std::string>& notificationAttrs)
{
  LM_KTREE("NOTIF: db entity node: ", entityNodeP);

  KjNode* _idP = kjLookup(entityNodeP, "_id");
  KjNode* outP = kjObject(orionldState.kjsonP, NULL);

  if ((_idP == NULL) || (_idP->type != KjObject))
    return NULL;

  KjNode* idP   = kjLookup(_idP, "id");
  KjNode* typeP = kjLookup(_idP, "type");

  if ((idP == NULL) || (typeP == NULL))
    return NULL;

  kjChildAdd(outP, idP);

  eqForDot(typeP->value.s);
  typeP->value.s = orionldContextItemAliasLookup(orionldState.contextP, typeP->value.s, NULL, NULL);
  kjChildAdd(outP, typeP);

  // Entity sysAttrs?

  KjNode* attrsP = kjLookup(entityNodeP, "attrs");

  if (attrsP == NULL)
    return NULL;

  KjNode* next;
  KjNode* attrP = attrsP->value.firstChildP;

  while (attrP)
  {
    next = attrP->next;

    eqForDot(attrP->name);

    if (attrIn(notificationAttrs, attrP->name) == true)
    {
      attrP->name = orionldContextItemAliasLookup(orionldState.contextP, attrP->name, NULL, NULL);
      kjChildAdd(outP, attrP);
      // Attribute sysAttrs?
    }

    attrP = next;
  }

  return outP;
}



// -----------------------------------------------------------------------------
//
// notifyData -
//
static void notifyData(CachedSubscription* subP, KjNode* dataNodeP, KjNode* entityNodeP)
{
  KjNode* ngsildEntityP = dbEntityToNgsildEntityWithAttrsFilter(entityNodeP, subP->attributes);

  if (ngsildEntityP != NULL)
    kjChildAdd(dataNodeP, ngsildEntityP);
}



// -----------------------------------------------------------------------------
//
// orionldNotificationSend
//
// Message Lines:
//   $VERB $URL HTTP/1.1
//   Content-Length: XXX
//   [Extra headers from the subscription]
//   Content-Type: application/[ld+]json
//   User-Agent: orionld
//
//   $PAYLOAD_DATA
//
bool orionldNotificationSend(OrionldNotification* niP, char* payloadBody)
{
  char     requestHeader[512];
  char     contentLenHeader[32];
  char*    jsonEnding              = (char*) "Content-Type: application/json\r\nUser-Agent: orionld\r\n\r\n";
  char*    jsonldEnding            = (char*) "Content-Type: application/ld+json\r\nUser-Agent: orionld\r\n\r\n";
  char*    ending                  = (niP->subP->httpInfo.mimeType == JSONLD)? jsonldEnding : jsonEnding;

  //
  // In the very first notification for a subscription, its IP, port and url-path is extracted from
  // the HttpInfo field of the cached subscription.
  // After that, the three fields are stored in the cached subscription
  //
  // To not destroy the url field of HttpInfo (used by APIv2), the field is cloned before calling ipPortAndRest,
  // that destroys the field.
  //
  if (niP->subP->port == 0)
  {
    LM_TMP(("NFD: niP->subP->httpInfo.url: '%s'", niP->subP->httpInfo.url.c_str()));
    char*           ip;
    unsigned short  port;
    char*           rest;
    char*           url = kaStrdup(&orionldState.kalloc, niP->subP->httpInfo.url.c_str());

    ipPortAndRest(url, &ip, &port, &rest);
    LM_TMP(("NFD: ip: '%s', port: %d, rest: %s", ip, port, rest));
    LM_TMP(("NFD: niP->subP->httpInfo.url: '%s'", niP->subP->httpInfo.url.c_str()));

    niP->subP->port = port;
    strncpy(niP->subP->ip,      ip,                                 sizeof(niP->subP->ip));
    strncpy(niP->subP->urlPath, rest,                               sizeof(niP->subP->urlPath));
    strncpy(niP->subP->verb,    verbName(niP->subP->httpInfo.verb), sizeof(niP->subP->verb));
  }
  snprintf(requestHeader, sizeof(requestHeader), "%s %s HTTP/1.1\r\n", niP->subP->verb, niP->subP->urlPath);


  size_t   payloadBodyLen          = strlen(payloadBody);
  snprintf(contentLenHeader, sizeof(contentLenHeader), "Content-Length: %lu\r\n", payloadBodyLen);


  //
  // struct iovec
  // {
  //   void  *iov_base;    /* Starting address */
  //   size_t iov_len;     /* Number of bytes to transfer */
  // };
  //
  struct iovec  ioVec[10];
  int           ioVecIx = 2;  // Skipping over requestHeader and contentLenHeader

  ioVec[0] = { requestHeader,    strlen(requestHeader)    };
  ioVec[1] = { contentLenHeader, strlen(contentLenHeader) };

  if (niP->subP->httpInfo.mimeType == JSON)
  {
    char    linkHeader[512];
    size_t  linkHeaderLen;

    if (niP->subP->ldContext != "")
    {
      snprintf(linkHeader, sizeof(linkHeader), "Link: <%s>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"\r\n", niP->subP->ldContext.c_str());
      linkHeaderLen = strlen(linkHeader);
    }
    else
    {
      // FIXME: I could hardcode the entire "Link: url-to-core-context" to save some time, it's length as well
      snprintf(linkHeader, sizeof(linkHeader), "Link: <%s>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"\r\n", ORIONLD_CORE_CONTEXT_URL);
      linkHeaderLen = ORIONLD_CORE_CONTEXT_URL_LEN + 8;
    }

    ioVec[ioVecIx++] = { linkHeader,  linkHeaderLen };
  }

  //
  // Notification-specific headers go HERE (before 'ending')
  //

  ioVec[ioVecIx++] = { ending,      strlen(ending) };  // FIXME: I could hardcode the length of 'ending' - no need to call strlen
  ioVec[ioVecIx++] = { payloadBody, payloadBodyLen };

  //
  // Data ready to send
  //
  LM_TMP(("NFD: Connecting to %s:%d", niP->subP->ip, niP->subP->port));
  niP->fd = orionldServerConnect(niP->subP->ip, niP->subP->port);
  LM_TMP(("NFD: Connected with fd %d", niP->fd));
  if (niP->fd == -1)
  {
    LM_E(("Internal Error (unable to connent to server for notification for subscription '%s': %s)", niP->subP->subscriptionId, strerror(errno)));
    niP->allOK = false;
    return false;
  }

  int nb;
  int totalSize = 0;

  for (int ix = 0; ix < ioVecIx; ix++)
  {
    LM_TMP(("NFD: line %d (%d chars): %s", ix, ioVec[ix].iov_len, (char*) ioVec[ix].iov_base));
    totalSize += ioVec[ix].iov_len;
  }

  LM_TMP(("NFD: writing to fd %d (%d bytes)", niP->fd, totalSize));
  nb = writev(niP->fd, ioVec, ioVecIx);

  if (nb == -1)
  {
    LM_E(("Internal Error (unable to send to server for notification for subscription '%s' - fd == %d): %s", niP->subP->subscriptionId, niP->fd, strerror(errno)));
    LM_TMP(("NFD: write to fd %d failed: %s", niP->fd, strerror(errno)));
    close(niP->fd);
    niP->fd    = -1;
    niP->allOK = false;
    return false;
  }
  else if (nb != totalSize)
  {
    LM_E(("Internal Error (written only %d bytes of %d of notification for sub '%s')", nb, totalSize, niP->subP->subscriptionId));
    close(niP->fd);
    niP->fd    = -1;
    niP->allOK = false;
    return false;
  }

  LM_TMP(("NFD: write to fd %d OK (%d bytes written)", niP->fd, nb));
  return true;
}



// -----------------------------------------------------------------------------
//
// responseTreat -
//
static bool responseTreat(OrionldNotification* niP, char* buf, int bufLen)
{
  char*  firstLine      = NULL;
  char*  endOfFirstLine = NULL;
  int    nb             = read(niP->fd, buf, bufLen);

  if (nb == -1)
  {
    LM_E(("Internal Error (error reading from notification endpoint: %s)", strerror(errno)));
    return false;
  }

  firstLine      = buf;
  endOfFirstLine = strchr(firstLine, '\n');

  if (endOfFirstLine == NULL)
  {
    LM_E(("Internal Error (unable to find end of first line from notification endpoint)"));
    return false;
  }

  *endOfFirstLine = 0;

  //
  // FIXME: Read the rest of the message, using select?
  //
  if (strstr(firstLine, "200 OK") != NULL)
    return true;

  return false;
}



void notificationResponsesRead()
{
  //
  // Reading responses
  //
  int             fds;
  fd_set          rFds;
  int             fdMax      = 0;
  struct timeval  tv;
  time_t          startTime  = time(NULL);

  int loops = 0;
  while (1)
  {
    char payload[1024];

    tv.tv_sec  = 0;
    tv.tv_usec = 100000;

    FD_ZERO(&rFds);
    for (OrionldNotification* niP = orionldState.notificationHead; niP != NULL; niP = niP->next)
    {
      if (niP->fd >= 0)
      {
        FD_SET(niP->fd, &rFds);
        fdMax = MAX(fdMax, niP->fd);
      }
    }

    fds = select(fdMax, &rFds, NULL, NULL, &tv);

    if ((fds == -1) && (errno != EINTR))
      LM_X(1, ("select error: %s\n", strerror(errno)));
    else if (fds > 0)
    {
      LM_TMP(("NOTIF: Reading responses - got something"));
      for (OrionldNotification* niP = orionldState.notificationHead; niP != NULL; niP = niP->next)
      {
        if (FD_ISSET(niP->fd, &rFds))
        {
          if (responseTreat(niP, payload, sizeof(payload)) == true)
            niP->allOK = true;
          else
            niP->allOK = false;

          LM_TMP(("NOTIF: Read response from fd %d (allOK: %s)", niP->fd, K_FT(niP->allOK)));
          close(niP->fd);
          niP->fd = -1;
        }
      }
    }

    //
    // Timeout after 10 seconds, which cannot possible occur before 100 loops have passed, as
    // the timeout of the select is of 0.1 second;
    //
    ++loops;
    if (loops > 100)
    {
      if (time(NULL) > startTime + 10)
        break;
    }
  }
}



// -----------------------------------------------------------------------------
//
// notificationSubscriptionActions -
//
static void notificationSubscriptionActions(void)
{
  LM_TMP(("NOTIF: throttling: In notificationSubscriptionActions"));
  //
  // Close file descriptors and set lastFailure/lastSuccess
  //
  LM_TMP(("NOTIF: Close file descriptors and set lastFailure/lastSuccess"));
  for (OrionldNotification* niP = orionldState.notificationHead; niP != NULL; niP = niP->next)
  {
    if (niP->fd >= 0)
      close(niP->fd);

    niP->subP->lastNotificationTime  = orionldState.requestTime;
    LM_TMP(("NOTIF: throttling: set lastNotificationTime to %f", niP->subP->lastNotificationTime));

    if (niP->allOK == true)
    {
      niP->subP->count       += 1;
      niP->subP->lastSuccess  = orionldState.requestTime;
      // dbSubscriptionLastSuccessSet(niP->subscriptionId);
    }
    else
    {
      niP->subP->lastFailure = orionldState.requestTime;
      // dbSubscriptionLastFailureSet(niP->subscriptionId);
    }
  }
}



// -----------------------------------------------------------------------------
//
// orionldNotifyAll -
//
// This function loops through the list of matching subscriptions, in the form OrionldNotification:
//  - subP            subscription
//  - changeTree      Request tree with errors removed
//  - resultTree      Result of the merge between what was in the DB and the changeTree == what's to be used to notify
//
// The subscription has a field called 'attributes' that is an array of attribute names.
// If this array is empty, then all attributes are included in the notification.
// If non-empty, only those present are included in the notification.
//
void orionldNotifyAll(void)
{
  for (OrionldNotification* niP = orionldState.notificationHead; niP != NULL; niP = niP->next)
  {
    LM_TMP(("NOTIF: ------------------------------------"));
    LM_KTREE("NOTIF: ", niP->resultTree);
    LM_TMP(("NOTIF: ------------------------------------"));
    LM_KTREE("NOTIF: ", niP->changeTree);
    LM_TMP(("NOTIF: ------------------------------------"));

    KjNode* notificationTree = kjObject(orionldState.kjsonP, NULL);
    char    notificationId[64];

    strncpy(notificationId, "urn:ngsi-ld:Notification:", sizeof(notificationId));
    uuidGenerate(&notificationId[25]);

    KjNode* notificationIdNodeP   = kjString(orionldState.kjsonP, "id", notificationId);
    KjNode* typeNodeP             = kjString(orionldState.kjsonP, "type", "Notification");
    KjNode* subscriptionIdNodeP   = kjString(orionldState.kjsonP, "subscriptionId", niP->subP->subscriptionId);

    char    requestTimeV[64];
    char*   detail;

    if (numberToDate(orionldState.requestTime, requestTimeV, sizeof(requestTimeV), &detail) == false)
    {
      LM_E(("Internal Error (converting timestamp to DateTime string: %s)", detail));
      snprintf(requestTimeV, sizeof(requestTimeV), "1970-01-01T00:00:00Z");
    }
    KjNode* notifiedAtNodeP      = kjString(orionldState.kjsonP, "notifiedAt", requestTimeV);
    KjNode* dataNodeP            = kjArray(orionldState.kjsonP,  "data");

    kjChildAdd(notificationTree, notificationIdNodeP);
    kjChildAdd(notificationTree, typeNodeP);
    kjChildAdd(notificationTree, subscriptionIdNodeP);
    kjChildAdd(notificationTree, notifiedAtNodeP);
    kjChildAdd(notificationTree, dataNodeP);

    int entities = 0;
    if (niP->resultTree->type == KjArray)
    {
      KjNode* entityNodeP = niP->resultTree->value.firstChildP;
      KjNode* next;

      while (entityNodeP != NULL)
      {
        next = entityNodeP->next;
        notifyData(niP->subP, dataNodeP, entityNodeP);
        ++entities;
        entityNodeP = next;
      }
    }
    else
    {
      notifyData(niP->subP, dataNodeP, niP->resultTree);
      ++entities;
    }

    bool  freePayload = false;
    char* payload;

    if (entities < 100)
      payload = kaAlloc(&orionldState.kalloc, entities * 2048);
    else
    {
      payload     = (char*) malloc(entities * 2048);
      freePayload = true;
    }

    kjRender(orionldState.kjsonP, notificationTree, payload, entities * 2048);
    LM_TMP(("NOTIF: notification payload body: %s", payload));

    LM_TMP(("NOTIF: Time to send notification"));
    orionldNotificationSend(niP, payload);

    if (freePayload == true)
      free(payload);

    //
    // FIXME - this entire kaAlloc/malloc could be smarter - reuse between loops
    //
  }

  notificationSubscriptionActions();

  //
  // If I call notificationResponsesRead, it might take 10 seconds before all
  // responses are read. Need to call notificationSubscriptionActions first.
  // It might be that I decide to not use notificationResponsesRead
  // notificationResponsesRead();
}
