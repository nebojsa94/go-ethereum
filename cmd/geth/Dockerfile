FROM gcr.io/tenderly-project/build-env:latest AS build-env

WORKDIR /app

ADD . /app

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -tags nocgo -o account-service ./src/cmd/account-service

FROM golang:1.13-alpine

WORKDIR /app

COPY --from=build-env /app/geth .

ENTRYPOINT ./geth --goerli --syncmode "full" --gcmode "archive"
