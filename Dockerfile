FROM golang:1.21 as build-env

WORKDIR /go/src/app
COPY . /go/src/app

RUN make
RUN strip cmd/conduit/conduit

FROM gcr.io/distroless/base-debian12

COPY --from=build-env /go/src/app/cmd/conduit/conduit /app/conduit
CMD ["/app/conduit","-d","/data"]
