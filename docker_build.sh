#!/bin/bash

docker build . -t urtho/conduit-online-clickhouse:latest
docker push urtho/conduit-online-clickhouse:latest
