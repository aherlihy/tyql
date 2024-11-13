#!/bin/bash
wait_for_port() {
    local service="$1"
    local port="$2"
    echo "Waiting for ${service}..."
    while ! nc -z localhost "$port"; do
        sleep 1
    done
    echo "${service} at port ${port} ready"
}

wait_for_port "PostgreSQL" 5433
wait_for_port "MySQL" 3307
wait_for_port "MariaDB" 3308
echo "All databases ready"

TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)
TEST_OUTPUT_FILE="/test-results/test-output_${TIMESTAMP}.log"
sbt "test" < /dev/null 2>&1 | tee >(sed 's/\x1b\[[0-9;]*m//g' > "${TEST_OUTPUT_FILE}")

EXIT_CODE=${PIPESTATUS[0]}
exit $EXIT_CODE

