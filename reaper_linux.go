//go:build linux
// +build linux

package main

import (
	"fmt"
	"syscall"
)

const prSetChildSubreaper = 36

func enableChildSubreaper() error {
	_, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, prSetChildSubreaper, 1, 0, 0, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func reapProcessGroup(pgid int) error {
	for {
		var status syscall.WaitStatus
		_, err := syscall.Wait4(-pgid, &status, 0, nil)
		switch err {
		case nil, syscall.EINTR:
			continue
		case syscall.ECHILD:
			return nil
		default:
			return fmt.Errorf("wait for process group %d: %w", pgid, err)
		}
	}
}
