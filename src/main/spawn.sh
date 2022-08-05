#!/bin/bash

count=10
go build -buildmode=plugin wc.go
for i in $(seq $count); do
    (go run mrworker.go wc.so &)
done