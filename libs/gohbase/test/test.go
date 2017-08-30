// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"

	"bfs/libs/gohbase"
	"bfs/libs/gohbase/hrpc"
)

// This error is returned when the HBASE_HOME environment variable is unset
var errHomeUnset = errors.New("Environment variable HBASE_HOME is not set")

// getShellCmd returns a new shell subprocess (already started) along with its
// stdin
func getShellCmd() (*exec.Cmd, io.WriteCloser, error) {
	hbaseHome := os.Getenv("HBASE_HOME")
	if len(hbaseHome) == 0 {
		return nil, nil, errHomeUnset
	}
	hbaseShell := path.Join(hbaseHome, "bin", "hbase")
	cmd := exec.Command(hbaseShell, "shell")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, err
	}

	err = cmd.Start()
	if err != nil {
		stdin.Close()
		return nil, nil, err
	}
	return cmd, stdin, nil
}

// CreateTable finds the HBase shell via the HBASE_HOME environment variable,
// and creates the given table with the given families
func CreateTable(host, table string, cFamilies []string) error {
	// If the table exists, delete it
	DeleteTable(host, table)
	// Don't check the error, since one will be returned if the table doesn't
	// exist

	cmd, stdin, err := getShellCmd()
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	buf.WriteString("create '" + table + "'")

	for _, f := range cFamilies {
		buf.WriteString(", '")
		buf.WriteString(f)
		buf.WriteString("'")
	}
	buf.WriteString("\n")

	stdin.Write(buf.Bytes())
	stdin.Write([]byte("exit\n"))

	return cmd.Wait()
}

// DeleteTable finds the HBase shell via the HBASE_HOME environment variable,
// and disables and drops the given table
func DeleteTable(host, table string) error {
	// TODO: We leak this client.
	ac := gohbase.NewAdminClient(host)
	dit := hrpc.NewDisableTable(context.Background(), []byte(table))
	_, err := ac.DisableTable(dit)
	if err != nil {
		if !strings.Contains(err.Error(), "TableNotEnabledException") {
			return err
		}
	}

	det := hrpc.NewDeleteTable(context.Background(), []byte(table))
	_, err = ac.DeleteTable(det)
	if err != nil {
		return err
	}
	return nil
}

// LaunchRegionServers uses the script local-regionservers.sh to create new
// RegionServers. Fails silently if server already exists.
// Ex. LaunchRegions([]string{"2", "3"}) launches two servers with id=2,3
func LaunchRegionServers(servers []string) {
	hh := os.Getenv("HBASE_HOME")
	servers = append([]string{"start"}, servers...)
	exec.Command(hh+"/bin/local-regionservers.sh", servers...).Run()
}

// StopRegionServers uses the script local-regionservers.sh to stop existing
// RegionServers. Fails silently if server isn't running.
func StopRegionServers(servers []string) {
	hh := os.Getenv("HBASE_HOME")
	servers = append([]string{"stop"}, servers...)
	exec.Command(hh+"/bin/local-regionservers.sh", servers...).Run()
}
