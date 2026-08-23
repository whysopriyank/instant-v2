# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/instantd ./cmd/instantd

FROM scratch
COPY --from=build /out/instantd /instantd
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/instantd"]
