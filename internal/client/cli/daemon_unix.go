//go:build !windows

package cli

import (
	"os"
	"os/exec"
	"syscall"
	"time"
)

// getSysProcAttr returns platform-specific process attributes for daemonization
func getSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Setsid: true, // Create new session (Unix only)
	}
}

// isProcessRunningOS checks if a process is running using OS-specific method
func isProcessRunningOS(process *os.Process) bool {
	// On Unix, FindProcess always succeeds, so we need to send signal 0 to check
	err := process.Signal(syscall.Signal(0))
	return err == nil
}

// killProcessOS kills a process using OS-specific signals
func killProcessOS(process *os.Process) error {
	// First try SIGTERM for graceful shutdown
	if err := process.Signal(syscall.SIGTERM); err != nil {
		// If SIGTERM fails, try SIGKILL
		return process.Signal(syscall.SIGKILL)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !isProcessRunningOS(process) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	if err := process.Signal(syscall.SIGKILL); err != nil {
		return err
	}

	time.Sleep(100 * time.Millisecond)
	return nil
}

// setupDaemonCmd configures the command for daemon mode
func setupDaemonCmd(cmd *exec.Cmd) {
	cmd.SysProcAttr = getSysProcAttr()
}
