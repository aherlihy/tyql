#!/bin/bash
set -euo pipefail

command="$1"

case $command in
    "db-start")
        docker-compose --profile dbs start
        ;;
    "db-stop")
        docker-compose --profile dbs stop
        ;;
    "test")
        mkdir -p test-results
        docker-compose --profile dbs --profile tests up main
        ;;
    *)
        echo "Unknown command: $command"
        echo "Usage: ./dev.sh [up|down|test]"
        exit 1
        ;;
esac
