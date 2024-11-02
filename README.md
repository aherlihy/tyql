# tyql

### Docker for running tests
```bash
$ docker build --platform linux/x86_64 --network host -t tyql-test .
  # on my laptop Docker containers sometimes lose the Internet access,
  # `--network host` fixes that
  # `--platform linux/x86_64` is needed on Macs with M-series processors
$ docker run -t tyql-test
```
