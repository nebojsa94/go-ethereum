FROM gcr.io/tenderly-project/build-env:latest AS build-env

WORKDIR /app

ADD . /app

RUN make geth

FROM golang:1.13-alpine

WORKDIR /app

COPY --from=build-env /app/build/bin/geth .

ENTRYPOINT ./geth --goerli --syncmode "full" --gcmode "archive"
