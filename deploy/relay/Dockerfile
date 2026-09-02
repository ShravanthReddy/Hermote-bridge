# hermes-relay: static binary in a distroless image.
#   docker build -f deploy/relay/Dockerfile -t hermes-relay .
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/hermes-relay ./cmd/hermes-relay

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/hermes-relay /hermes-relay
EXPOSE 8080
USER nonroot
ENTRYPOINT ["/hermes-relay", "-listen", ":8080"]
