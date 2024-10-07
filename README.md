# tyql

### Docker for running tests
```bash
$ docker build --network host -t tyql-test .
  # on my laptop Docker containers sometimes lose the Internet access,
  # --network host fixes that
$ docker run -t tyql-test
```
