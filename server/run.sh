#!/bin/bash

/etc/init.d/consul start
exec java -cp /starkiller.jar:/logback-classic-1.3.0-alpha10.jar:/logback-core-1.3.0-alpha10.jar:/ clojure.main -m starkiller.server.main "$@"