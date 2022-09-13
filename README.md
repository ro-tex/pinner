# pinner

Ensures that relevant skyfiles will be properly pinned by the portal even when individual servers are removed

## Environment variables

### Required

- `SERVER_DOMAIN`: current server name, e.g. `eu-ger-1.siasky.net`
- `SKYNET_DB_USER`: database user
- `SKYNET_DB_PASS`: database password
- `SKYNET_DB_HOST`: database host, e.g. `mongo`
- `SKYNET_DB_PORT`: database port, e.g. `27017`
- `SIA_API_PASSWORD`: API password for the local `skyd` instance. Typically available in
  the `/home/user/skynet-webportal/docker/data/sia/apipassword` file.

### Optional

- `SKYNET_ACCOUNTS_HOST`: the host where `accounts` service is running, e.g. `10.10.10.70`
- `SKYNET_ACCOUNTS_PORT`: the port on which `accounts` service is running, e.g. `3000`
- `PINNER_LOG_FILE`: path to `pinner`'s log file, relative to `/home/user/skynet-webportal/docker/data/pinner/logs/`
  directory. Directory traversal (e.g. `../`) is not allowed. If this value is empty, `pinner` won't log to disk!
- `PINNER_LOG_LEVEL`: log [level](https://github.com/sirupsen/logrus#level-logging), defaults to `info`
- `PINNER_SCANNER_THREADS`: number of parallel threads pinning files, defaults to 5
- `PINNER_SLEEP_BETWEEN_SCANS`: defines the time to wait between *initiating* new scans, e.g. if this value is 10 hours
  and a scan was triggered at 10:00, the next scan will be triggered at 20:00 regardless of how long the first scan
  took. If there is a scan running when the next scan is scheduled to start the new scan doesn't start (we don't overlap
  scans). This value is given in seconds and it defaults to 19 hours.
- `API_HOST`: host on which `skyd` is running, e.g. `10.10.10.10`
- `API_PORT`: port on which `skyd` is running, e.g. `9980`
