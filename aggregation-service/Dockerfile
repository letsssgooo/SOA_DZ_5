FROM golang:1.25.0-alpine3.22 AS build

WORKDIR /src
COPY go.mod go.sum ./
COPY aggregation-service/ aggregation-service/
RUN CGO_ENABLED=0 go build -trimpath -o /out/aggregation-service ./aggregation-service/cmd/aggregation-service

FROM scratch
COPY --from=build /out/aggregation-service /aggregation-service
USER 65532:65532
ENTRYPOINT ["/aggregation-service"]
