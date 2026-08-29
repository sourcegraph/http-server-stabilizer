# http-server-stabilizer

Here's the situation:

- You've got an HTTP server that generally works quite well.
- Sometimes, it goes rogue and consumes 100% of a CPU as it gets stuck into a loop. You will track down and fix this, but in the meantime it shouldn't harm other requests.

http-server-stabilizer solves this problem by doing three simple things:

1. Running multiple copies of your HTTP server and acting as a reverse-proxy to them.
2. Dividing requests do the copies randomly.
3. When a request takes longer than a specified amount of time, that server subprocess is restarted.

## Installation

[Install Go](https://golang.org/doc/install), then:

```sh
go get -u github.com/slimsag/http-server-stabilizer
```

See the releases tab for prebuilt linux/amd64 binaries.

## Usage

```
http-server-stabilizer [options] -- yourcommand -youroption true
```

Consult `http-server-stabilizer -h` for options.

## Demo

The following starts an HTTP server which responds to `GET /` requests and randomly consumes 100% CPU:

```sh
http-server-stabilizer -demo -demo-listen ':9091'
```

If you make multiple `curl http://localhost:9091` requests, you'll find the web server quickly consumes all available CPUs. Making use of `http-server-stabilizer`, we can prevent this:

```sh
http-server-stabilizer -- http-server-stabilizer -demo -demo-listen ':{{.Port}}'
```

The `-timeout=10s` flag can be used to control how long rogue requests can go for. You can also control the timeout via a request header: `X-Stabilize-Timeout: 20s`.

## Graceful shutdown

On SIGTERM or SIGINT, the stabilizer continues accepting requests for
`-pre-shutdown-pause` (default `0s`). It then closes its listener and drains in-flight
requests until the total `-graceful-shutdown-timeout` (default `10s`) measured from signal
receipt expires. Workers are stopped only after the HTTP drain completes. Worker cleanup
has a final five-second bound. A second signal skips the remaining pause and drain.

## Debugging

All responses include a `X-Worker` header which is a PID correlating to the `http-server-stabilizer` worker PID for debugging purposes (so you can trace a specific request back to a specific worker process).

A Prometheus metric indicating how many worker restarts occur is also exposed at `:6060/metrics`. For example, with `-prometheus-app-name="myapp"` the metric `myapp_hss_worker_restarts` will be exposed.
