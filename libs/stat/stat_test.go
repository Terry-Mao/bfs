package stat

import (
	"testing"
)

func TestStat(t *testing.T) {
	var (
		s  = &Stats{}
		s1 = &Stats{}
	)
	// tps & qps
	s.TotalAddProcessed = 10
	s.TotalWriteProcessed = 15
	s.TotalDelProcessed = 20
	s.TotalGetProcessed = 25
	s.TotalFlushProcessed = 30
	s.TotalCompactProcessed = 35
	s1.TotalAddProcessed = 10
	s1.TotalWriteProcessed = 15
	s1.TotalDelProcessed = 20
	s1.TotalGetProcessed = 25
	s1.TotalFlushProcessed = 30
	s1.TotalCompactProcessed = 35
	// bytes
	// delay
	s.Calc()
	s1.Merge(s)
	s1.Calc()
	if s.AddTPS != 10 {
		t.Errorf("TotalAddTPS: %d not match", s.AddTPS)
		t.FailNow()
	}
	if s.WriteTPS != 15 {
		t.Errorf("TotalWriteTPS: %d not match", s.WriteTPS)
		t.FailNow()
	}
	if s.DelTPS != 20 {
		t.Errorf("TotalDelTPS: %d not match", s.DelTPS)
		t.FailNow()
	}
	if s.GetQPS != 25 {
		t.Errorf("TotalGetQPS: %d not match", s.GetQPS)
		t.FailNow()
	}
	if s.FlushTPS != 30 {
		t.Errorf("TotalFlushTPS: %d not match", s.FlushTPS)
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
	if s1.TotalCompactProcessed != 70 {
		t.Errorf("TotalCompactProcessed: %d not match", s1.TotalCompactProcessed)
		t.FailNow()
	}
	if s1.AddTPS != 20 {
		t.Errorf("TotalAddTPS: %d not match", s1.AddTPS)
		t.FailNow()
	}
	if s1.WriteTPS != 30 {
		t.Errorf("TotalWriteTPS: %d not match", s1.WriteTPS)
		t.FailNow()
	}
	if s1.DelTPS != 40 {
		t.Errorf("TotalDelTPS: %d not match", s1.DelTPS)
		t.FailNow()
	}
	if s1.GetQPS != 50 {
		t.Errorf("TotalGetQPS: %d not match", s1.GetQPS)
		t.FailNow()
	}
	if s1.FlushTPS != 60 {
		t.Errorf("TotalFlushTPS: %d not match", s1.FlushTPS)
		t.FailNow()
	}
	if s1.TotalCommandsProcessed != 270 {
		t.Errorf("TotalCommandsProcessed: %d not match", s1.TotalCommandsProcessed)
		t.FailNow()
	}
}
