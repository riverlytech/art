package cmd

import (
	"fmt"
	"os"
	"strings"

	"art/pkg/supervisor"

	"github.com/spf13/cobra"
)

var (
	mountDir      string
	interactive   bool
	dbPath        string
	enableTrace   bool
	traceLogPath  string
	traceSyscalls string
)

var RootCmd = &cobra.Command{
	Use:   "art",
	Short: "ART: Agent Runtime",
	Long:  `A supervisor for sandboxed agents using bubblewrap and ptrace.`,
	Run: func(cmd *cobra.Command, args []string) {
		var syscalls []string
		if traceSyscalls != "" {
			for _, s := range strings.Split(traceSyscalls, ",") {
				s = strings.TrimSpace(s)
				if s != "" {
					syscalls = append(syscalls, s)
				}
			}
		}

		cfg := supervisor.Config{
			MountDir:      mountDir,
			Interactive:   interactive,
			DBPath:        dbPath,
			EnableTracer:  enableTrace,
			TraceLogPath:  traceLogPath,
			TraceSyscalls: syscalls,
			Command:       args,
		}
		if err := supervisor.Run(cfg); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	RootCmd.PersistentFlags().StringVarP(&mountDir, "mount", "m", ".", "Host directory to mount as the agent's workspace (read-only base for overlay)")
	RootCmd.PersistentFlags().BoolVarP(&interactive, "interactive", "i", true, "Run in interactive mode with full PTY support (use -i=false to disable)")
	RootCmd.PersistentFlags().StringVarP(&dbPath, "db", "d", "", "Path to SQLite database for persistent FUSE filesystem")
	RootCmd.PersistentFlags().BoolVar(&enableTrace, "trace", false, "Enable ptrace-based syscall tracing")
	RootCmd.PersistentFlags().StringVar(&traceLogPath, "trace-log", "", "Path to log file for ptrace syscalls (default: stderr)")
	RootCmd.PersistentFlags().StringVar(&traceSyscalls, "trace-syscalls", "", "Comma-separated list of syscalls to log (default: all)")
}
