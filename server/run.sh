#!/bin/bash

/etc/init.d/consul start
exec java -cp /skywalker.jar:/logback-classic-1.2.3.jar:/logback-core-1.2.3.jar:/ clojure.main -m skywalker.server.main "$@"