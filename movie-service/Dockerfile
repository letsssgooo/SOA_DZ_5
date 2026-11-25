FROM golang:1.25.0-alpine3.22 AS generate

WORKDIR /src
COPY go.mod go.sum ./
RUN apk add --no-cache protoc \
    && go install google.golang.org/protobuf/cmd/protoc-gen-go

COPY schemas/movie-event.proto schemas/
RUN module="$(go list -m)" \
    && protoc --proto_path=schemas --go_out=. \
        --go_opt="module=${module}" \
        --go_opt="Mmovie-event.proto=${module}/gen/go/cinema/events/v1" \
        movie-event.proto

FROM scratch AS protobuf
COPY --from=generate /src/gen /gen

FROM generate AS build
COPY movie-service/ movie-service/
RUN CGO_ENABLED=0 go build -trimpath -o /out/movie-service ./movie-service/cmd/movie

FROM scratch
COPY --from=build /out/movie-service /movie-service
COPY --from=generate /src/schemas/movie-event.proto /schemas/movie-event.proto
USER 65532:65532
ENTRYPOINT ["/movie-service"]
