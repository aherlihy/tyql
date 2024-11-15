# tyql

## Development
### Running Tests
Tests are untagged by default or tagged as expensive.
```scala
import test.expensiveTest
test("PostgreSQL responds".tag(expensiveTest)) { ??? }
```

```bash
# Run all tests (both cheap and expensive)
sbt test
# Run only expensive tests
sbt "testOnly -- --include-tags=Expensive"
# Run only cheap tests
sbt "testOnly -- --exclude-tags=Expensive"
```

## Containerization

We provide a `dev.sh` script to manage the development environment using Docker Compose.
```bash
# Start all required databases
./dev.sh db-start
# Stop all databases
./dev.sh db-stop
# Run all tests
./dev.sh test
```
For convenience, bash completion is provided for the `dev.sh` script. To enable it:
```bash
# Add this to your ~/.bashrc or ~/.bash_profile
source /path/to/project/dev.sh.completion
```
After enabling completion, you can use Tab to autocomplete `dev.sh` commands:
```bash
./dev.sh <TAB>
# Shows: db-start db-stop test
```
Test results from Docker are automatically saved to the `test-results` directory with timestamps.

The containerized environment includes:
- PostgreSQL (port 5433)
- MySQL (port 3307)
- MariaDB (port 3308)
- SQLite, DuckDB, H2 (in-memory from the main container)
