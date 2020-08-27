FROM golang:1.14

RUN apt-get install make gcc git curl ca-certificates

ENV FDB_VERSION 6.2.19

RUN curl -k -SLO "https://www.foundationdb.org/downloads/${FDB_VERSION}/ubuntu/installers/foundationdb-clients_${FDB_VERSION}-1_amd64.deb" \
 && dpkg -i foundationdb-clients_${FDB_VERSION}-1_amd64.deb \
 && rm foundationdb-clients_${FDB_VERSION}-1_amd64.deb

ADD . /app
RUN cd /app && make geth

ENTRYPOINT ["/app/build/bin/geth"]
CMD ["--goerli", "--syncmode \"full\"", "--gcmode \"archive\""]
