package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/clock"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	sessions "github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/telemetry"
	"github.com/gastownhall/gascity/internal/worker"
)

// errTokenMismatch indicates the running session's instance token
// doesn't match the expected one — the session was re-woken by a
// different incarnation and this drain/stop is stale.
var errTokenMismatch = errors.New("instance token mismatch")

// preWakeCommit persists a new incarnation (generation + token) BEFORE
// starting the process. This is Phase 1 of the two-phase wake protocol.
// Returns the new generation, instance token, and the PreWakePatch batch it
// persisted so the caller can fold it onto its coherent typed snapshot
// (write-returns-Info) instead of re-projecting the bead. It reads the current
// persisted state off the caller's typed Info (session_name, generation,
// continuation epoch, sleep_reason, wake_mode, and the continuation-reset
// signals) — every field a verbatim raw mirror — so no raw bead crosses in.
func preWakeCommit(
	info sessions.Info,
	sessFront *sessions.Store,
	clk clock.Clock,
) (newGen int, token string, fold sessions.MetadataPatch, err error) {
	name := info.SessionNameMetadata
	if !sessions.IsSessionNameSyntaxValid(name) {
		return 0, "", nil, fmt.Errorf("invalid session_name %q", name)
	}

	gen, _ := strconv.Atoi(info.Generation)
	newGen = gen + 1
	token = sessions.NewInstanceToken()
	continuationEpoch, _ := strconv.Atoi(info.ContinuationEpoch)
	if continuationEpoch <= 0 {
		continuationEpoch = sessions.DefaultContinuationEpoch
	}
	if shouldBumpContinuationEpoch(info) {
		continuationEpoch++
	}

	sleepReason := ""
	if info.SleepReason == string(sessions.SleepReasonIdleTimeout) {
		// Preserve the idle-timeout wake override until the replacement
		// session has actually started. Failed starts must retry next tick.
		sleepReason = string(sessions.SleepReasonIdleTimeout)
	}

	freshWake := info.WakeMode == "fresh" || pendingContinuationResetNeedsFreshStart(info)
	batch := sessions.PreWakePatch(sessions.PreWakePatchInput{
		Generation:        newGen,
		InstanceToken:     token,
		ContinuationEpoch: continuationEpoch,
		Now:               clk.Now(),
		SleepReason:       sleepReason,
		FreshWake:         freshWake,
	})
	if writeErr := sessFront.ApplyPatch(info.ID, batch); writeErr != nil {
		return 0, "", nil, fmt.Errorf("pre-wake metadata commit: %w", writeErr)
	}
	traceFreshWakeMetadataReset(name, freshWakeResetPriorValues(info), batch, freshWake)

	return newGen, token, batch, nil
}

// freshWakeResetPriorValues reconstructs the pre-reset values of the fresh-wake
// conversation-reset keys off the typed Info so traceFreshWakeMetadataReset can
// report which durable provider markers a fresh wake cleared without the raw
// bead. The keys mirror sessions.FreshWakeConversationResetKeys().
func freshWakeResetPriorValues(info sessions.Info) map[string]string {
	return map[string]string{
		"session_key":             info.SessionKey,
		"started_config_hash":     info.StartedConfigHash,
		"started_live_hash":       info.StartedLiveHash,
		"live_hash":               info.LiveHash,
		"startup_dialog_verified": info.StartupDialogVerified,
		// Priming markers share the fresh-wake reset (S19 Stage 2), so their prior
		// values come off the verbatim raw Info mirrors — otherwise the trace's
		// before[key] lookup reads "" and the cleared list omits them even though
		// FreshWakeConversationResetKeys() clears them. Written as raw string keys
		// (matching the sibling entries) so this read-only prior-value map is not
		// mistaken for a store write by the compared-key write-site gate.
		"primed_at":            info.PrimedAtMetadata,
		"priming_attempted_at": info.PrimingAttemptedAtMetadata,
		"prompt_hash":          info.PromptHashMetadata,
	}
}

func traceFreshWakeMetadataReset(name string, before map[string]string, batch sessions.MetadataPatch, freshWake bool) {
	if !freshWake || os.Getenv("GC_TMUX_TRACE") != "1" {
		return
	}
	cleared := make([]string, 0, len(sessions.FreshWakeConversationResetKeys()))
	for _, key := range sessions.FreshWakeConversationResetKeys() {
		if strings.TrimSpace(before[key]) == "" || batch[key] != "" {
			continue
		}
		cleared = append(cleared, key)
	}
	if len(cleared) == 0 {
		return
	}
	log.Printf(
		"[WAKE-TRACE] preWakeCommit session=%s wake_mode=fresh cleared_provider_metadata=%s",
		name,
		strings.Join(cleared, ","),
	)
}

func shouldBumpContinuationEpoch(info sessions.Info) bool {
	if info.ContinuationResetPending != "" {
		return true
	}
	return info.WakeMode == "fresh" && info.LastWokeAt != ""
}

func pendingContinuationResetNeedsFreshStart(info sessions.Info) bool {
	switch sessions.State(strings.TrimSpace(info.MetadataState)) {
	case sessions.StateStartPending, sessions.StateCreating:
		return false
	}
	return strings.TrimSpace(info.ContinuationResetPending) != "" &&
		strings.TrimSpace(info.StartedConfigHash) != ""
}

// validateWorkDir ensures the path is safe to use as a working directory.
func validateWorkDir(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if abs != filepath.Clean(abs) {
		return fmt.Errorf("non-canonical path")
	}
	info, err := os.Stat(abs)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("not a directory")
	}
	return nil
}

// beginSessionDrainInfo initiates an async drain. Returns immediately.
// The drainTracker stores in-memory state; advanceSessionDrainsWithSessionsTraced progresses it.
//
// Returns true when this call enqueued a new drain (a state transition) and
// false when a drain was already enqueued for this session (no-op). Callers
// that emit user-visible log lines or convergence events tied to the drain
// MUST gate on the return value — otherwise those emissions fire every
// reconciler tick for the life of a stuck drain.
//
// The interrupt signal (Ctrl-C) is NOT sent immediately. It is deferred to
// the next reconciler tick via advanceSessionDrainsWithSessionsTraced. This gives the drain
// one full tick to be canceled (e.g., if the session was falsely orphaned
// due to a transient store failure) before any signal reaches the process.
// Without this, a single bad tick can interrupt a working agent mid-tool-call.
//
// It reads only session_name, generation, and id — all carried verbatim on Info.
func beginSessionDrainInfo(
	info sessions.Info,
	_ runtime.Provider, // kept for caller compatibility; interrupt deferred to advanceSessionDrainsWithSessionsTraced
	dt *drainTracker,
	reason string,
	clk clock.Clock,
	timeout time.Duration,
) bool {
	name := info.SessionNameMetadata
	if dt.get(info.ID) != nil {
		if os.Getenv("GC_TMUX_TRACE") == "1" {
			log.Printf("[DRAIN-TRACE] beginSessionDrain session=%s reason=%s noop=already-draining", name, reason)
		}
		return false
	}
	gen, _ := strconv.Atoi(info.Generation)

	dt.set(info.ID, &drainState{
		startedAt:  clk.Now(),
		deadline:   clk.Now().Add(timeout),
		reason:     reason,
		generation: gen,
	})

	if os.Getenv("GC_TMUX_TRACE") == "1" {
		log.Printf("[DRAIN-TRACE] beginSessionDrain session=%s reason=%s", name, reason)
	}
	telemetry.RecordDrainTransition(context.Background(), name, reason, "begin")
	return true
}

// beginSessionDrainInfoAtBoundary starts an automatic drain only while the
// trigger ownership captured by the reconciler decision is still current. The
// tracker mutation is kept under the same per-session lock as the persisted
// reload so a clear/repoint cannot leave a drain belonging to a newer owner.
func beginSessionDrainInfoAtBoundary(
	info sessions.Info,
	cfg *config.City,
	store beads.Store,
	sp runtime.Provider,
	dt *drainTracker,
	reason string,
	clk clock.Clock,
	timeout time.Duration,
	boundaries ...reconcilerMutationBoundary,
) bool {
	var began bool
	boundary := selectedReconcilerMutationBoundary(info, cfg, boundaries...).lifecycleMutation()
	_, err := boundary.run(store, nil, func(current sessions.Info, _ *sessions.Store) error {
		began = beginSessionDrainInfo(current, sp, dt, reason, clk, timeout)
		return nil
	})
	return err == nil && began
}

func drainReasonCancelable(reason string) bool {
	return reason != "config-drift" && reason != "orphaned" && reason != "suspended"
}

func pendingDrainReasonCancelable(reason string) bool {
	return reason != "orphaned" && reason != "suspended"
}

const (
	reconcilerDrainAckSourceKey     = "GC_DRAIN_ACK_SOURCE"
	reconcilerDrainAckSourceValue   = "reconciler"
	drainAckSourceAgentValue        = "agent"
	reconcilerDrainAckReasonKey     = "GC_DRAIN_REASON"
	reconcilerDrainAckGenerationKey = "GC_DRAIN_GENERATION"
)

func setReconcilerDrainAckMetadata(sp runtime.Provider, name string, ds *drainState) error {
	if ds == nil {
		return nil
	}
	if err := sp.SetMeta(name, reconcilerDrainAckSourceKey, reconcilerDrainAckSourceValue); err != nil {
		return err
	}
	if err := sp.SetMeta(name, reconcilerDrainAckReasonKey, ds.reason); err != nil {
		_ = clearReconcilerDrainAckMetadata(sp, name)
		return err
	}
	if err := sp.SetMeta(name, reconcilerDrainAckGenerationKey, strconv.Itoa(ds.generation)); err != nil {
		_ = clearReconcilerDrainAckMetadata(sp, name)
		return err
	}
	if err := sp.SetMeta(name, "GC_DRAIN_ACK", "1"); err != nil {
		_ = clearReconcilerDrainAckMetadata(sp, name)
		return err
	}
	return nil
}

func clearReconcilerDrainAckMetadata(sp runtime.Provider, name string) error {
	if sp == nil {
		return fmt.Errorf("session provider is nil")
	}
	var errs []error
	for _, key := range []string{"GC_DRAIN_ACK", reconcilerDrainAckSourceKey, reconcilerDrainAckReasonKey, reconcilerDrainAckGenerationKey} {
		if err := sp.RemoveMeta(name, key); err != nil {
			log.Printf("session wake: clearing reconciler drain ack metadata %s for %s: %v", key, name, err)
			errs = append(errs, fmt.Errorf("removing %s: %w", key, err))
		}
	}
	return errors.Join(errs...)
}

func clearReconcilerDrainAckMetadataAtBoundary(
	info sessions.Info,
	cfg *config.City,
	store beads.Store,
	sp runtime.Provider,
	boundaries ...reconcilerMutationBoundary,
) bool {
	cleared := false
	boundary := selectedReconcilerMutationBoundary(info, cfg, boundaries...).lifecycleMutation()
	if boundary.strict || boundary.policyErr != nil {
		if boundary.policyErr != nil {
			return false
		}
		var err error
		for _, key := range []string{"GC_DRAIN_ACK", reconcilerDrainAckSourceKey, reconcilerDrainAckReasonKey, reconcilerDrainAckGenerationKey} {
			boundary, err = mutateStrictRuntimeMetadataAtBoundary(store, sp, cfg, boundary, key, "", true)
			if err != nil {
				return false
			}
		}
		return true
	}
	_, err := boundary.run(store, nil, func(current sessions.Info, _ *sessions.Store) error {
		name := strings.TrimSpace(current.SessionNameMetadata)
		if name == "" {
			return nil
		}
		if err := clearReconcilerDrainAckMetadata(sp, name); err != nil {
			return err
		}
		cleared = true
		return nil
	})
	return err == nil && cleared
}

func mutateStrictRuntimeMetadataAtBoundary(
	store beads.Store,
	sp runtime.Provider,
	cfg *config.City,
	boundary reconcilerMutationBoundary,
	key string,
	value string,
	remove bool,
) (reconcilerMutationBoundary, error) {
	if boundary.policyErr != nil || !boundary.strict {
		return reconcilerMutationBoundary{}, fmt.Errorf("strict exact runtime metadata mutation requires an unambiguous strict decision")
	}
	decision, err := boundary.automaticRuntimeDecision()
	if err != nil {
		return reconcilerMutationBoundary{}, err
	}
	factory, err := workerFactoryWithConfig("", store, sp, cfg, store)
	if err != nil {
		return reconcilerMutationBoundary{}, err
	}
	var commit sessions.AutomaticRuntimeCommit
	if remove {
		commit, err = factory.RemoveRuntimeMetadataForReconciler(context.Background(), boundary.captured.ID, key, decision)
	} else {
		commit, err = factory.SetRuntimeMetadataForReconciler(context.Background(), boundary.captured.ID, key, value, decision)
	}
	if err != nil {
		return reconcilerMutationBoundary{}, err
	}
	return boundary.afterAutomaticRuntimeCommit(commit)
}

// cancelSessionDrainInfo removes a cancelable drain if wake reasons reappeared
// for the same generation. If GC_DRAIN_ACK was already set by the reconciler
// (deferred drain signal), it is cleared so the Phase 1 drain-ack check doesn't
// kill the session. It reads the session id/generation/name off the Info snapshot.
func cancelSessionDrainInfo(info sessions.Info, sp runtime.Provider, dt *drainTracker) bool {
	return cancelSessionDrainIfInfo(info, sp, dt, drainReasonCancelable)
}

// cancelSessionDrainAtBoundary applies an automatic drain cancellation only to
// the exact session incarnation that led to the decision. The supplied
// cancellation callback may also clear provider drain metadata; it therefore
// executes under the same ownership lock as the raw persisted reload.
func cancelSessionDrainAtBoundary(
	info sessions.Info,
	cfg *config.City,
	store beads.Store,
	cancel func(sessions.Info) bool,
	boundaries ...reconcilerMutationBoundary,
) bool {
	if cancel == nil {
		return false
	}
	var canceled bool
	boundary := selectedReconcilerMutationBoundary(info, cfg, boundaries...).lifecycleMutation()
	if boundary.legacyAutomaticRuntimeEffectError() != nil {
		return false
	}
	_, err := boundary.run(store, nil, func(current sessions.Info, _ *sessions.Store) error {
		canceled = cancel(current)
		return nil
	})
	return err == nil && canceled
}

// cancelSessionDrainForPendingInfo cancels a pending-drain-cancelable drain for
// the reconciler's Phase-2 drain scan, working off the Info snapshot.
func cancelSessionDrainForPendingInfo(info sessions.Info, sp runtime.Provider, dt *drainTracker) bool {
	return cancelSessionDrainIfInfo(info, sp, dt, pendingDrainReasonCancelable)
}

// cancelSessionDrainForAssignedWorkInfo cancels an assigned-work-cancelable drain
// for the reconciler's Phase-2 drain scan, working off the Info snapshot.
func cancelSessionDrainForAssignedWorkInfo(info sessions.Info, sp runtime.Provider, dt *drainTracker) bool {
	return cancelSessionDrainIfInfo(info, sp, dt, assignedWorkDrainReasonCancelable)
}

func assignedWorkDrainReasonCancelable(reason string) bool {
	switch reason {
	case "orphaned", "no-wake-reason":
		return true
	default:
		return false
	}
}

// cancelSessionConfigDriftDrainInfo cancels a config-drift drain off the Info
// snapshot, threading Info straight into the typed drain-cancel core.
func cancelSessionConfigDriftDrainInfo(info sessions.Info, sp runtime.Provider, dt *drainTracker) bool {
	if dt == nil {
		return false
	}
	return cancelSessionDrainIfInfo(info, sp, dt, func(reason string) bool {
		return reason == "config-drift"
	})
}

// cancelSessionDrainIfInfo is the typed core of the drain-cancel helpers. It
// reads only the session id, generation, and session_name — all carried raw and
// verbatim on Info — so it is byte-identical to the raw-bead form it backs.
func cancelSessionDrainIfInfo(info sessions.Info, sp runtime.Provider, dt *drainTracker, canCancel func(string) bool) bool {
	ds := dt.get(info.ID)
	if ds == nil {
		return false
	}
	if !canCancel(ds.reason) {
		return false
	}
	gen, _ := strconv.Atoi(info.Generation)
	if gen == ds.generation {
		dt.clearIdleProbe(info.ID)
		dt.remove(info.ID)
		name := info.SessionNameMetadata
		// Clear GC_DRAIN_ACK if it was set — prevents stale ack from
		// killing the session on the next Phase 1 drain-ack check.
		if ds.ackSet {
			_ = clearReconcilerDrainAckMetadata(sp, name)
		}
		telemetry.RecordDrainTransition(context.Background(), name, ds.reason, "cancel")
		return true
	}
	return false
}

// cancelReconcilerAckedDrainInfo cancels a reconciler-owned drain ack off the
// Info snapshot: it reads the session_name (Info.SessionNameMetadata), generation
// (via reconcilerDrainAckMatchesSessionInfo) and id (dt keying) — all carried
// verbatim on Info — and routes the cancel through the typed drain-cancel core.
func cancelReconcilerAckedDrainInfo(info sessions.Info, sp runtime.Provider, dt *drainTracker) bool {
	if dt == nil {
		return false
	}
	name := strings.TrimSpace(info.SessionNameMetadata)
	reason, ok := reconcilerDrainAckMatchesSessionInfo(info, sp, name)
	if !ok || !pendingDrainReasonCancelable(reason) {
		return false
	}
	ds := dt.get(info.ID)
	if ds == nil || !ds.ackSet {
		return false
	}
	return cancelSessionDrainForPendingInfo(info, sp, dt)
}

func reconcilerDrainAckMatchesSession(session beads.Bead, sp runtime.Provider, name string) (string, bool) {
	if sp == nil || name == "" {
		return "", false
	}
	source, err := sp.GetMeta(name, reconcilerDrainAckSourceKey)
	if err != nil || source != reconcilerDrainAckSourceValue {
		return "", false
	}
	reason, err := sp.GetMeta(name, reconcilerDrainAckReasonKey)
	if err != nil || reason == "" {
		return "", false
	}
	expectedGeneration, err := sp.GetMeta(name, reconcilerDrainAckGenerationKey)
	if err != nil || expectedGeneration == "" {
		return "", false
	}
	currentGeneration := strings.TrimSpace(session.Metadata["generation"])
	if currentGeneration == "" || currentGeneration != expectedGeneration {
		return "", false
	}
	return reason, true
}

// reconcilerDrainAckMatchesSessionInfo is the session.Info sibling of
// reconcilerDrainAckMatchesSession for the reconciler forward pass. The only
// session-bead read is the generation (Info.Generation); everything else is
// provider metadata (sp) and the caller-supplied name, shared verbatim with the
// raw form — so it is byte-identical, pinned by the sessionGeneration oracle row.
func reconcilerDrainAckMatchesSessionInfo(info sessions.Info, sp runtime.Provider, name string) (string, bool) {
	if sp == nil || name == "" {
		return "", false
	}
	source, err := sp.GetMeta(name, reconcilerDrainAckSourceKey)
	if err != nil || source != reconcilerDrainAckSourceValue {
		return "", false
	}
	reason, err := sp.GetMeta(name, reconcilerDrainAckReasonKey)
	if err != nil || reason == "" {
		return "", false
	}
	expectedGeneration, err := sp.GetMeta(name, reconcilerDrainAckGenerationKey)
	if err != nil || expectedGeneration == "" {
		return "", false
	}
	currentGeneration := strings.TrimSpace(info.Generation)
	if currentGeneration == "" || currentGeneration != expectedGeneration {
		return "", false
	}
	return reason, true
}

func staleReconcilerDrainAck(session beads.Bead, sp runtime.Provider, name string) bool {
	if sp == nil || name == "" {
		return false
	}
	source, err := sp.GetMeta(name, reconcilerDrainAckSourceKey)
	if err != nil || source != reconcilerDrainAckSourceValue {
		return false
	}
	expectedGeneration, err := sp.GetMeta(name, reconcilerDrainAckGenerationKey)
	if err != nil || expectedGeneration == "" {
		return true
	}
	currentGeneration := strings.TrimSpace(session.Metadata["generation"])
	return currentGeneration == "" || currentGeneration != expectedGeneration
}

// staleReconcilerDrainAckInfo is the session.Info sibling of
// staleReconcilerDrainAck: the only session-bead read is the generation
// (Info.Generation), matching the raw form byte-for-byte (sessionGeneration
// oracle row).
func staleReconcilerDrainAckInfo(info sessions.Info, sp runtime.Provider, name string) bool {
	if sp == nil || name == "" {
		return false
	}
	source, err := sp.GetMeta(name, reconcilerDrainAckSourceKey)
	if err != nil || source != reconcilerDrainAckSourceValue {
		return false
	}
	expectedGeneration, err := sp.GetMeta(name, reconcilerDrainAckGenerationKey)
	if err != nil || expectedGeneration == "" {
		return true
	}
	currentGeneration := strings.TrimSpace(info.Generation)
	return currentGeneration == "" || currentGeneration != expectedGeneration
}

func staleOrLegacyDrainAckBeforeStart(session beads.Bead, sp runtime.Provider, name string) bool {
	if sp == nil || name == "" {
		return false
	}
	source, err := sp.GetMeta(name, reconcilerDrainAckSourceKey)
	if err == nil && source == drainAckSourceAgentValue {
		return false
	}
	if err == nil && source == reconcilerDrainAckSourceValue {
		return staleReconcilerDrainAck(session, sp, name)
	}
	acked, err := sp.GetMeta(name, "GC_DRAIN_ACK")
	return err == nil && acked == "1"
}

// staleOrLegacyDrainAckBeforeStartInfo is the session.Info sibling of
// staleOrLegacyDrainAckBeforeStart: it defers to staleReconcilerDrainAckInfo for
// the reconciler-owned branch (the only session-bead read, Info.Generation) and
// otherwise reads provider metadata only, so it is byte-identical to the raw form.
func staleOrLegacyDrainAckBeforeStartInfo(info sessions.Info, sp runtime.Provider, name string) bool {
	if sp == nil || name == "" {
		return false
	}
	source, err := sp.GetMeta(name, reconcilerDrainAckSourceKey)
	if err == nil && source == drainAckSourceAgentValue {
		return false
	}
	if err == nil && source == reconcilerDrainAckSourceValue {
		return staleReconcilerDrainAckInfo(info, sp, name)
	}
	acked, err := sp.GetMeta(name, "GC_DRAIN_ACK")
	return err == nil && acked == "1"
}

// cancelRecoveredReconcilerAckedDrainInfo clears a reconciler-owned drain ack
// whose in-memory tracker entry did not survive (recovered from provider
// metadata alone). Off the Info snapshot: the only session-bead read is the
// generation via reconcilerDrainAckMatchesSessionInfo.
func cancelRecoveredReconcilerAckedDrainInfo(info sessions.Info, sp runtime.Provider, name string) bool {
	reason, ok := reconcilerDrainAckMatchesSessionInfo(info, sp, name)
	if !ok || !pendingDrainReasonCancelable(reason) {
		return false
	}
	_ = clearReconcilerDrainAckMetadata(sp, name)
	telemetry.RecordDrainTransition(context.Background(), name, reason, "cancel")
	return true
}

// cancelRecoveredDrainForAssignedWorkInfo is the assigned-work counterpart of
// cancelRecoveredReconcilerAckedDrainInfo, off the Info snapshot.
func cancelRecoveredDrainForAssignedWorkInfo(info sessions.Info, sp runtime.Provider, name string) bool {
	reason, ok := reconcilerDrainAckMatchesSessionInfo(info, sp, name)
	if !ok || !assignedWorkDrainReasonCancelable(reason) {
		return false
	}
	_ = clearReconcilerDrainAckMetadata(sp, name)
	telemetry.RecordDrainTransition(context.Background(), name, reason, "cancel")
	return true
}

func advanceSessionDrainsWithSessionsTraced(
	dt *drainTracker,
	sp runtime.Provider,
	store beads.Store,
	infoLookup func(id string) (sessions.Info, bool),
	wakeEvals map[string]wakeEvaluation,
	cfg *config.City,
	clk clock.Clock,
	trace *sessionReconcilerTraceCycle,
	boundaryResolvers ...func(sessions.Info) reconcilerMutationBoundary,
) {
	for id := range dt.all() {
		captured, ok := infoLookup(id)
		if !ok {
			// The durable owner is gone. Removing controller-only drain state is
			// containment, not a live/session/work mutation.
			dt.clearIdleProbe(id)
			dt.remove(id)
			continue
		}
		boundary := captureReconcilerMutationBoundary(captured, cfg)
		if len(boundaryResolvers) > 0 && boundaryResolvers[0] != nil {
			boundary = boundaryResolvers[0](captured)
		}
		boundary = boundary.lifecycleMutation()
		// Strict Phase-2 drain progression includes provider Stop/Kill and a
		// session-state commit. The worker boundary currently exposes exact leased
		// metadata effects, but not an automatic destructive Stop/Kill/Close
		// operation. Do not mix those exact metadata writes with the legacy
		// name-only destructive tail: leave the strict drain parked until the whole
		// transition can share one attested conditional decision. Policy ambiguity
		// is the same fail-closed outcome.
		if boundary.strict || boundary.policyErr != nil {
			if trace != nil {
				err := boundary.policyErr
				if err == nil {
					err = sessions.ErrAutomaticRuntimeExactEffectUnsupported
				}
				trace.RecordDecision(
					TraceSiteDrainStale, TraceReasonUnknown, TraceOutcomeDeferred,
					normalizedSessionTemplateInfo(captured, cfg), captured.SessionNameMetadata,
					traceRecordPayload{"error": err.Error(), "strict_provider_effect_refused": true},
				)
			}
			continue
		}
		_, boundaryErr := boundary.run(store, nil, func(current sessions.Info, _ *sessions.Store) error {
			currentLookup := func(requested string) (sessions.Info, bool) {
				if requested == id {
					return current, true
				}
				return infoLookup(requested)
			}
			advanceSessionDrainsWithSessionsTracedCurrent(
				dt, sp, store, currentLookup, wakeEvals, cfg, clk, trace, id,
			)
			return nil
		})
		if boundaryErr != nil && trace != nil {
			trace.RecordDecision(
				TraceSiteDrainStale, TraceReasonStaleGeneration, TraceOutcomeDeferred,
				normalizedSessionTemplateInfo(captured, cfg), captured.SessionNameMetadata,
				traceRecordPayload{"error": boundaryErr.Error(), "witness_refused": true},
			)
		}
	}
}

func advanceSessionDrainsWithSessionsTracedCurrent(
	dt *drainTracker,
	sp runtime.Provider,
	store beads.Store,
	infoLookup func(id string) (sessions.Info, bool),
	wakeEvals map[string]wakeEvaluation,
	cfg *config.City,
	clk clock.Clock,
	trace *sessionReconcilerTraceCycle,
	onlyID string,
) {
	// wakeEvals is required. The reconciler builds it from the coherent infoByID
	// snapshot via ComputeAwakeSet -> awakeSetToWakeEvals; tests supply explicit
	// wakeEvals encoding the premise they exercise. Step 5d dropped the raw-bead
	// wakeEvals==nil fallback and its now-unused sessionBeads/poolDesired/workSet/
	// readyWaitSet inputs from this prod core — the scan runs entirely off infoLookup.
	// Session front door constructed once from the same store; nil when store is
	// nil so completeDrain keeps its store==nil short-circuit.
	sessFront := sessionFrontDoor(store)
	if store == nil {
		sessFront = nil
	}
	for id, ds := range dt.all() {
		if onlyID != "" && id != onlyID {
			continue
		}
		info, ok := infoLookup(id)
		if !ok {
			dt.clearIdleProbe(id)
			dt.remove(id)
			continue
		}
		// The whole scan runs off the typed Info: decision reads (session_name,
		// generation, template), the drain-complete write (completeDrain → store),
		// the cancel checks (cancelSessionDrainFor*Info), verifiedStop, and the
		// process-running probe (by info.ID). Nothing reads the raw bead.
		name := info.SessionNameMetadata

		// Stale check: if session was re-woken (generation changed), cancel drain.
		gen, _ := strconv.Atoi(info.Generation)
		if gen != ds.generation {
			dt.clearIdleProbe(id)
			if ds.ackSet {
				_ = clearReconcilerDrainAckMetadata(sp, name)
			}
			dt.remove(id)
			if trace != nil {
				trace.RecordDecision(TraceSiteDrainStale, TraceReasonStaleGeneration, TraceOutcomeCancel, normalizedSessionTemplateInfo(info, cfg), name, traceRecordPayload{
					"drain_reason":       ds.reason,
					"drain_generation":   ds.generation,
					"session_generation": gen,
				})
			}
			continue
		}

		// Check if process exited.
		running, _ := observeRuntimeProviderLiveness(sp, name, nil)
		if !running {
			// Process exited — drain complete.
			completeDrain(info, sessFront, ds, clk)
			dt.clearIdleProbe(id)
			dt.remove(id)
			telemetry.RecordDrainTransition(context.Background(), name, ds.reason, "complete")
			if trace != nil {
				trace.RecordDecision(TraceSiteDrainComplete, TraceReasonCode(ds.reason), TraceOutcomeComplete, normalizedSessionTemplateInfo(info, cfg), name, traceRecordPayload{
					"drain_started_at": ds.startedAt,
				})
			}
			continue
		}

		if eval, ok := wakeEvals[info.ID]; ok &&
			containsWakeReason(eval.Reasons, WakePending) &&
			pendingDrainReasonCancelable(ds.reason) {
			if cancelSessionDrainForPendingInfo(info, sp, dt) {
				if trace != nil {
					trace.RecordDecision(TraceSiteDrainCancel, TraceReasonCode(ds.reason), TraceOutcomeCancelPending, normalizedSessionTemplateInfo(info, cfg), name, nil)
				}
				continue
			}
		}

		if eval, ok := wakeEvals[info.ID]; ok &&
			eval.Reason == "assigned-work" &&
			containsWakeReason(eval.Reasons, WakeWork) &&
			assignedWorkDrainReasonCancelable(ds.reason) {
			if cancelSessionDrainForAssignedWorkInfo(info, sp, dt) {
				if trace != nil {
					trace.RecordDecision(TraceSiteDrainCancel, TraceReasonCode(ds.reason), TraceOutcomeCancelAssignedWork, normalizedSessionTemplateInfo(info, cfg), name, nil)
				}
				continue
			}
		}

		// Cancellation check: if wake reasons reappeared, cancel the in-memory
		// drain. Orphaned, suspended, and ordinary config-drift drains are not
		// canceled here.
		if drainReasonCancelable(ds.reason) {
			if eval, ok := wakeEvals[info.ID]; ok && len(eval.Reasons) > 0 {
				dt.clearIdleProbe(id)
				// Clear GC_DRAIN_ACK if it was set — prevents stale ack
				// from killing the session on the next Phase 1 check.
				if ds.ackSet {
					_ = clearReconcilerDrainAckMetadata(sp, name)
				}
				dt.remove(id)
				if trace != nil {
					trace.RecordDecision(TraceSiteDrainCancel, TraceReasonCode(ds.reason), TraceOutcomeCancel, normalizedSessionTemplateInfo(info, cfg), name, nil)
				}
				continue
			}
		}

		// Deferred drain signal: set GC_DRAIN_ACK after the drain has survived
		// at least one full tick without being canceled. This prevents a
		// single transient store failure from interrupting a working agent
		// — the false-orphan drain is canceled on the next tick when the
		// store recovers, before any signal is set.
		//
		// Uses the same GC_DRAIN_ACK env var that agents set via
		// `gc runtime drain-ack`. The reconciler's Phase 1 drain-ack check
		// sees it on the next tick and calls sp.Stop() for a clean
		// SIGTERM/SIGKILL — no Ctrl-C keystroke injection into the pane.
		if !ds.ackSet {
			if os.Getenv("GC_TMUX_TRACE") == "1" {
				log.Printf("[DRAIN-TRACE] advanceSessionDrainsWithSessionsTraced: setting GC_DRAIN_ACK session=%s reason=%s", name, ds.reason)
			}
			err := setReconcilerDrainAckMetadata(sp, name, ds)
			if err == nil {
				ds.ackSet = true
				ds.followUp = true
			}
			if trace != nil {
				outcome := TraceOutcomeSuccess
				fields := traceRecordPayload{
					"reason":          ds.reason,
					"deferred_signal": true,
				}
				if err != nil {
					outcome = TraceOutcomeFailed
					fields["error"] = err.Error()
				}
				fields["template"] = normalizedSessionTemplateInfo(info, cfg)
				fields["before"] = ""
				fields["after"] = "1"
				fields["field"] = "GC_DRAIN_ACK"
				trace.RecordMutation(TraceSiteMutationRuntimeMeta, TraceReasonUnknown, outcome, "provider_meta", name, "GC_DRAIN_ACK", fields)
			}
		}

		// Pending-interaction guards and wake-based cancellation run before this
		// timeout path. Preserve that ordering if this block is refactored.
		if clk.Now().After(ds.deadline) {
			// Drain timed out — force stop.
			if err := verifiedStop(info, store, sp, cfg); err != nil {
				if errors.Is(err, errTokenMismatch) {
					// Session was re-woken by a different incarnation.
					// This drain is stale — cancel it.
					dt.clearIdleProbe(id)
					dt.remove(id)
				}
				// Other errors (transient stop failure): keep drain
				// active for retry on next tick.
				if trace != nil {
					trace.RecordDecision(TraceSiteDrainTimeout, TraceReasonCode(ds.reason), TraceOutcomeRetry, normalizedSessionTemplateInfo(info, cfg), name, traceRecordPayload{
						"error": err.Error(),
					})
				}
				continue
			}
			// Re-probe after stop to confirm process actually exited
			// before marking metadata as asleep.
			running, _ := observeRuntimeProviderLiveness(sp, name, nil)
			if !running {
				completeDrain(info, sessFront, ds, clk)
				dt.clearIdleProbe(id)
				dt.remove(id)
				telemetry.RecordDrainTransition(context.Background(), name, ds.reason, "timeout")
				if trace != nil {
					trace.RecordDecision(TraceSiteDrainTimeout, TraceReasonCode(ds.reason), TraceOutcomeComplete, normalizedSessionTemplateInfo(info, cfg), name, nil)
				}
			}
			// If still running after stop, keep drain for next tick.
		}
		// Else: still draining, check again next tick.
	}
}

// completeDrain writes drain-complete metadata to the store for the drained
// session. It reads only the typed Info (id + raw wake_mode); the raw-bead
// mirror the reconciler used to keep is dropped. Nothing reads a drained
// session's metadata later in the tick — the awake scan runs before
// advanceSessionDrainsWithSessionsTraced, and completeDrain is always followed by dt.remove +
// continue — so the store write is the sole observable effect (all completeDrain
// tests assert on store.Get). With no store there is nothing to persist.
func completeDrain(info sessions.Info, sessFront *sessions.Store, ds *drainState, clk clock.Clock) {
	if sessFront == nil {
		return
	}
	batch := sessions.CompleteDrainPatch(clk.Now(), ds.reason, info.WakeMode == "fresh")
	_ = sessFront.ApplyPatch(info.ID, batch)
}

// verifiedStop stops a session after verifying the instance_token matches.
// Prevents stale drain operations from targeting a re-woken session.
// Returns errTokenMismatch if the running process has a different token.
//
// NOTE: On composite providers (auto/hybrid), GetMeta and Stop may route
// to different backends if the route table is stale. This is a pre-existing
// routing limitation — when the reconciler is wired in, consider a
// provider-level VerifiedStop that atomically verifies+stops on the same backend.
func verifiedStop(info sessions.Info, store beads.Store, sp runtime.Provider, cfg *config.City) error {
	name := info.SessionNameMetadata
	expectedToken := info.InstanceToken
	if expectedToken != "" {
		actualToken, _ := sp.GetMeta(name, "GC_INSTANCE_TOKEN")
		if actualToken != "" && actualToken != expectedToken {
			return fmt.Errorf("%w for session %s", errTokenMismatch, info.ID)
		}
	}
	handle, err := workerHandleForSessionWithConfig("", store, sp, cfg, info.ID)
	if err != nil {
		return err
	}
	return handle.Kill(context.Background())
}

// verifiedInterrupt sends an interrupt signal after verifying instance_token.
func verifiedInterrupt(session beads.Bead, store beads.Store, sp runtime.Provider, cfg *config.City) error {
	name := session.Metadata["session_name"]
	expectedToken := session.Metadata["instance_token"]
	if expectedToken != "" {
		actualToken, _ := sp.GetMeta(name, "GC_INSTANCE_TOKEN")
		if actualToken != "" && actualToken != expectedToken {
			return fmt.Errorf("%w for session %s", errTokenMismatch, session.ID)
		}
	}
	handle, err := workerHandleForSessionWithConfig("", store, sp, cfg, session.ID)
	if err != nil {
		return err
	}
	return handle.Interrupt(context.Background(), worker.InterruptRequest{})
}
