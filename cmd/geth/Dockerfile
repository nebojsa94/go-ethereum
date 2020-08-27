FROM golang:1.14-alpine AS build-env

RUN apk add --no-cache make gcc musl-dev linux-headers git curl ca-certificates

ENV FDB_VERSION 6.2.19

RUN curl -k -SLO "https://www.foundationdb.org/downloads/${FDB_VERSION}/ubuntu/installers/foundationdb-clients_${FDB_VERSION}-1_amd64.deb" \
 && dpkg -i foundationdb-clients_${FDB_VERSION}-1_amd64.deb \
 && rm foundationdb-clients_${FDB_VERSION}-1_amd64.deb

ADD . /app
RUN cd /app && make geth

FROM alpine:latest

RUN apk add --no-cache ca-certificates

COPY --from=build-env /app/build/bin/geth /app/geth

ENTRYPOINT ["/app/geth"]
CMD ["--goerli", "--syncmode full", "--gcmode archive"]
