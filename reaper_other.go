//go:build !linux
// +build !linux

package main

func enableChildSubreaper() error { return nil }
func reapProcessGroup(int) error  { return nil }
