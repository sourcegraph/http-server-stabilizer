package main

import "os"

var envHostname = os.Getenv("HOSTNAME")

// hostname derives an OS hostname to return. If the `HOSTNAME` env var
// is set, it will return that, else falling back to `os.Hostname()`
func hostname() string {
	if envHostname != "" {
		return envHostname
	}
	h, _ := os.Hostname()
	return h
}
