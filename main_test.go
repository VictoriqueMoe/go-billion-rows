package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMustRun(t *testing.T) {
	p := makeFile(t, `stationA;10.00
stationB;20.00
stationA;30.00
stationC;-1.00
stationD;-99.99
stationE;-999.99
stationF;1.00
`)

	var stdout, stderr bytes.Buffer
	err := MustRun([]string{"gobillion", "-f", p, "-w", "1"}, &stdout, &stderr)
	require.NoError(t, err)

	strOut, strErr := stdout.String(), stderr.String()

	require.Contains(t, strOut, "stationA=10.00/20.00/30.00")
	require.Contains(t, strOut, "stationB=20.00/20.00/20.00")
	require.Contains(t, strOut, "stationC=-1.00/-1.00/-1.00")
	require.Contains(t, strOut, "stationD=-99.99/-99.99/-99.99")
	require.Contains(t, strOut, "stationE=-999.99/-999.99/-999.99")
	require.Contains(t, strOut, "stationF=1.00/1.00/1.00")
	require.Contains(t, strErr, "RESULTS")
}

func TestMustRunMalformedNumber(t *testing.T) {
	p := makeFile(t, `stationA;NaN
stationB;20.00
stationA;30.00
`)
	err := MustRun([]string{"gobillion", "-f", p, "-w", "1"}, io.Discard, io.Discard)
	require.ErrorContains(t, err, `malformed number: "NaN"`)
}

func TestMustRun_FailsOnMissingFile(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := MustRun([]string{"cmd", "-f", "nonexistent.txt"}, &stdout, &stderr)
	require.ErrorContains(t, err, `file nonexistent.txt does not exist`)
}

func makeFile(t *testing.T, contents string) (path string) {
	t.Helper()
	dir := t.TempDir()
	path = filepath.Join(dir, "test.txt")
	err := os.WriteFile(path, []byte(contents), 0644)
	require.NoError(t, err)
	return path
}
