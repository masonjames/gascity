package main

import (
	"fmt"
	"io"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/clock"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
)

// autoSuspendChatSessions scans active chat sessions and suspends any that
// have been detached (no human attached) longer than idleTimeout.
// Called on each controller reconciliation tick when [chat_sessions] idle_timeout is set.
func autoSuspendChatSessions(store beads.Store, sp runtime.Provider, idleTimeout time.Duration, clk clock.Clock, stdout, stderr io.Writer, configs ...*config.City) {
	if store == nil {
		return // no store — nothing to suspend
	}

	var cfg *config.City
	if len(configs) > 0 {
		cfg = configs[0]
	} else {
		cityPath, _ := resolveCity()
		if cityPath != "" {
			cfg, _ = loadCityConfig(cityPath, io.Discard)
		}
	}
	rows, err := sessionFrontDoor(store).ListAllForReconcile(sessionpkg.ListAllOptions{Sort: beads.SortCreatedDesc})
	if err != nil {
		fmt.Fprintf(stderr, "gc start: auto-suspend list: %v\n", err) //nolint:errcheck // best-effort stderr
		return
	}

	now := clk.Now()
	for _, row := range rows {
		s := row.Info
		boundary := captureReconcilerMutationBoundary(s, cfg, row.Persisted.Revision).lifecycleMutation()
		if err := boundary.legacyAutomaticRuntimeEffectError(); err != nil {
			fmt.Fprintf(stderr, "gc start: refusing strict auto-suspend for %s: %v\n", s.ID, err) //nolint:errcheck
			continue
		}
		lastActive := time.Time{}
		ran, err := boundary.run(store, func(current sessionpkg.Info) (bool, error) {
			name := current.SessionNameMetadata
			if name == "" || sp.IsAttached(name) {
				return false, nil
			}
			switch current.State {
			case sessionpkg.StateNone, sessionpkg.StateActive, sessionpkg.StateAwake:
			default:
				return false, nil
			}
			var err error
			lastActive, err = sp.GetLastActivity(name)
			if err != nil || lastActive.IsZero() {
				return false, err
			}
			return now.Sub(lastActive) >= idleTimeout, nil
		}, func(current sessionpkg.Info, front *sessionpkg.Store) error {
			name := current.SessionNameMetadata
			wasRunning := sp.IsRunning(name)
			if err := sp.Stop(name); err != nil && wasRunning {
				return err
			}
			_, err := front.ApplyPatchInfo(current, sessionpkg.MetadataPatch{
				"state":        string(sessionpkg.StateSuspended),
				"suspended_at": now.UTC().Format(time.RFC3339),
			})
			return err
		})
		if err != nil {
			fmt.Fprintf(stderr, "gc start: auto-suspend session %s: %v\n", s.ID, err) //nolint:errcheck // best-effort stderr
		} else if ran {
			fmt.Fprintf(stdout, "Session %s auto-suspended (idle %s).\n", s.ID, formatDuration(now.Sub(lastActive))) //nolint:errcheck // best-effort stdout
		}
	}
}
