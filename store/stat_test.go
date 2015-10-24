package main

import (
	"testing"
)

func TestStat(t *testing.T) {
	var (
		s  = &Stats{}
		s1 = &Stats{}
	)
	s.TotalAddProcessed = 10
	s.TotalWriteProcessed = 15
	s.TotalDelProcessed = 20
	s.TotalGetProcessed = 25
	s.TotalFlushProcessed = 30
	s.TotalCompressProcessed = 35
	s1.TotalAddProcessed = 10
	s1.TotalWriteProcessed = 15
	s1.TotalDelProcessed = 20
	s1.TotalGetProcessed = 25
	s1.TotalFlushProcessed = 30
	s1.TotalCompressProcessed = 35
	s.Calc()
	s1.Merge(s)
	s1.Calc()
	if s.TotalAddTPS != 10 {
		t.Errorf("TotalAddTPS: %d not match", s.TotalAddTPS)
		t.FailNow()
	}
	if s.TotalWriteTPS != 15 {
		t.Errorf("TotalWriteTPS: %d not match", s.TotalWriteTPS)
		t.FailNow()
	}
	if s.TotalDelTPS != 20 {
		t.Errorf("TotalDelTPS: %d not match", s.TotalDelTPS)
		t.FailNow()
	}
	if s.TotalGetQPS != 25 {
		t.Errorf("TotalGetQPS: %d not match", s.TotalGetQPS)
		t.FailNow()
	}
	if s.TotalFlushTPS != 30 {
		t.Errorf("TotalFlushTPS: %d not match", s.TotalFlushTPS)
		t.FailNow()
	}
	if s.TotalCommandsProcessed != 135 {
		t.Errorf("TotalCommandsProcessed: %d not match", s.TotalCommandsProcessed)
		t.FailNow()
	}
	if s1.TotalAddProcessed != 20 {
		t.Errorf("TotalAddProcessed: %d not match", s1.TotalAddProcessed)
		t.FailNow()
	}
	if s1.TotalWriteProcessed != 30 {
		t.Errorf("TotalWriteProcessed: %d not match", s1.TotalWriteProcessed)
		t.FailNow()
	}
	if s1.TotalDelProcessed != 40 {
		t.Errorf("TotalDelProcessed: %d not match", s1.TotalDelProcessed)
		t.FailNow()
	}
	if s1.TotalGetProcessed != 50 {
		t.Errorf("TotalGetProcessed: %d not match", s1.TotalGetProcessed)
		t.FailNow()
	}
	if s1.TotalFlushProcessed != 60 {
		t.Errorf("TotalFlushProcessed: %d not match", s1.TotalFlushProcessed)
		t.FailNow()
	}
	if s1.TotalCompressProcessed != 70 {
		t.Errorf("TotalCompressProcessed: %d not match", s1.TotalCompressProcessed)
		t.FailNow()
	}
	if s1.TotalAddTPS != 20 {
		t.Errorf("TotalAddTPS: %d not match", s1.TotalAddTPS)
		t.FailNow()
	}
	if s1.TotalWriteTPS != 30 {
		t.Errorf("TotalWriteTPS: %d not match", s1.TotalWriteTPS)
		t.FailNow()
	}
	if s1.TotalDelTPS != 40 {
		t.Errorf("TotalDelTPS: %d not match", s1.TotalDelTPS)
		t.FailNow()
	}
	if s1.TotalGetQPS != 50 {
		t.Errorf("TotalGetQPS: %d not match", s1.TotalGetQPS)
		t.FailNow()
	}
	if s1.TotalFlushTPS != 60 {
		t.Errorf("TotalFlushTPS: %d not match", s1.TotalFlushTPS)
		t.FailNow()
	}
	if s1.TotalCommandsProcessed != 270 {
		t.Errorf("TotalCommandsProcessed: %d not match", s1.TotalCommandsProcessed)
		t.FailNow()
	}
}
