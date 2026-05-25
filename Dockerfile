FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/kubectl-scmigrate ./cmd/kubectl-scmigrate

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/kubectl-scmigrate /kubectl-scmigrate
ENTRYPOINT ["/kubectl-scmigrate"]
