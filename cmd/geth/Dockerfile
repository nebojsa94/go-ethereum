FROM gcr.io/tenderly-project/build-env:latest AS build-env

RUN apk update && apk add build-base dumb-init curl

ENV FDB_VERSION 6.2.19

RUN curl -SLO "https://www.foundationdb.org/downloads/${FDB_VERSION}/ubuntu/installers/foundationdb-clients_${FDB_VERSION}-1_amd64.deb" \
 && dpkg -i foundationdb-clients_${FDB_VERSION}-1_amd64.deb \
 && rm foundationdb-clients_${FDB_VERSION}-1_amd64.deb

WORKDIR /app

ADD . /app

RUN make geth

FROM golang:1.13-alpine

WORKDIR /app

COPY --from=build-env /app/build/bin/geth .

ENTRYPOINT ./geth --goerli --syncmode "full" --gcmode "archive"
