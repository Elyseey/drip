package cli

import (
	"reflect"
	"testing"
)

func TestValidateDaemonTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tunnelType string
		port       int
		wantErr    bool
	}{
		{name: "http", tunnelType: "http", port: 3000},
		{name: "https", tunnelType: "https", port: 443},
		{name: "tcp", tunnelType: "tcp", port: 22},
		{name: "invalid type", tunnelType: "../http", port: 3000, wantErr: true},
		{name: "invalid port low", tunnelType: "http", port: 0, wantErr: true},
		{name: "invalid port high", tunnelType: "http", port: 70000, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateDaemonTarget(tt.tunnelType, tt.port)
			if tt.wantErr && err == nil {
				t.Fatalf("validateDaemonTarget(%q, %d) expected error", tt.tunnelType, tt.port)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("validateDaemonTarget(%q, %d) unexpected error: %v", tt.tunnelType, tt.port, err)
			}
		})
	}
}

func TestSanitizeDaemonArgs(t *testing.T) {
	t.Parallel()

	args := []string{
		"http", "3000",
		"--daemon",
		"-d",
		"--daemon=true",
		"--daemon-child",
		"--verbose",
		"--transport", "wss",
	}

	got := sanitizeDaemonArgs(args)
	want := []string{
		"http", "3000",
		"--daemon-child",
		"--verbose",
		"--transport", "wss",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sanitizeDaemonArgs() = %#v, want %#v", got, want)
	}
}
