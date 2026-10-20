FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /ledgerd ./cmd/ledgerd
RUN mkdir -p /ledgerdata && chown 65532:65532 /ledgerdata

FROM scratch
COPY --from=build /ledgerd /ledgerd
COPY --from=build --chown=65532:65532 /ledgerdata /data
COPY config/docker.json /etc/ledger/cluster.json
USER 65532:65532
EXPOSE 8100
ENTRYPOINT ["/ledgerd"]
CMD ["-config", "/etc/ledger/cluster.json", "-id", "n1", "-data", "/data"]
