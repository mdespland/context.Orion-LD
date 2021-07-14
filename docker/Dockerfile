FROM marcdespland/orion-ld-base:1.0 AS build-stage

ARG PATH_TO_SRC='opt/orion/'
ARG DEBUG=false

RUN cp -a /usr/local/lib/. /usr/lib/aarch64-linux-gnu/
RUN rm -r /usr/local/lib/

COPY . ${PATH_TO_SRC}

WORKDIR ${PATH_TO_SRC}

RUN  if [ "$DEBUG" = "true" ] ; then make debug install ; else make release install ; fi

RUN strip /usr/bin/orionld

FROM debian:buster-slim

RUN mkdir -p /var/{log,run}/orionld

COPY --from=build-stage /etc/ssl/ /etc/ssl/
COPY --from=build-stage /opt/orion/ldcontexts/ /opt/orion/ldcontexts/
COPY --from=build-stage /lib/aarch64-linux-gnu/ /lib/aarch64-linux-gnu/
COPY --from=build-stage /usr/lib/aarch64-linux-gnu/ /usr/lib/aarch64-linux-gnu/
COPY --from=build-stage /usr/bin/ /usr/bin/

ENTRYPOINT ["orionld", "-fg", "-multiservice", "-ngsiv1Autocast"]

EXPOSE 1026
