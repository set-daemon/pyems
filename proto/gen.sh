#!/bin/bash
protoc -I./ --python_out=./ msg.proto
