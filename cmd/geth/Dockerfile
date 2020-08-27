FROM gcr.io/tenderly-project/build-env:latest AS build-env

RUN apk update && apk add build-base dumb-init curl

ENV FDB_VERSION 6.2.19

RUN curl -SLO "https://foundationdb.com/downloads/I_accept_the_FoundationDB_Community_License_Agreement/key-value-store/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_amd64.deb" \
 && dpkg -i foundationdb-clients_${FDB_VERSION}-1_amd64.deb \
 && rm foundationdb-clients_${FDB_VERSION}-1_amd64.deb

WORKDIR /app

ADD . /app

RUN make geth

FROM golang:1.13-alpine

WORKDIR /app

COPY --from=build-env /app/build/bin/geth .

ENTRYPOINT ./geth --goerli --syncmode "full" --gcmode "archive"
