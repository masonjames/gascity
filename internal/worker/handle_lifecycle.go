package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gastownhall/gascity/internal/runtime"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
)

// Start ensures the worker exists and its runtime is live.
func (h *SessionHandle) Start(ctx context.Context) (err error) {
	event := h.beginOperationEvent(ctx, workerOperationStart)
	defer func() { event.finish(err) }()

	if h.currentSessionID() == "" {
		if _, err := h.authorizeLiveBoundary(ctx, ""); err != nil {
			return err
		}
	}
	id, err := h.ensureSessionID()
	if err != nil {
		return err
	}
	startCommand, err := h.startCommand(id)
	if err != nil {
		return err
	}
	authorization, err := h.authorizeLiveBoundary(ctx, id)
	if err != nil {
		return err
	}
	err = h.manager.StartWithWitness(
		ctx,
		id,
		startCommand,
		h.runtimeHintsWithAuthorization(h.runtimeHints(), authorization),
		liveBoundaryWitness(authorization),
	)
	return normalizeLiveBoundaryError(err)
}

// StartResolved starts or resumes the worker using a caller-supplied runtime
// command and hints. This is a migration bridge for higher layers that already
// materialize provider-specific runtime config but should still delegate the
// provider-specific runtime bring-up through the worker boundary.
func (h *SessionHandle) StartResolved(ctx context.Context, startCommand string, hints runtime.Config) (err error) {
	event := h.beginOperationEvent(ctx, workerOperationStartResolved)
	defer func() { event.finish(err) }()

	if h.currentSessionID() == "" {
		if _, err := h.authorizeLiveBoundary(ctx, ""); err != nil {
			return err
		}
	}
	id, err := h.ensureSessionID()
	if err != nil {
		return err
	}
	command := strings.TrimSpace(startCommand)
	if command == "" {
		command, err = h.startCommand(id)
		if err != nil {
			return err
		}
	}
	startHints := hints
	if strings.TrimSpace(startHints.Command) == "" {
		startHints = h.runtimeHints()
	}
	authorization, err := h.authorizeLiveBoundary(ctx, id)
	if err != nil {
		return err
	}
	err = h.manager.StartRuntimeOnlyWithWitness(
		ctx,
		id,
		command,
		h.runtimeHintsWithAuthorization(startHints, authorization),
		liveBoundaryWitness(authorization),
	)
	return normalizeLiveBoundaryError(err)
}

// StartPreparedResolved starts a reconciler-prepared session only while the
// exact trigger captured with that candidate remains authoritative. Unlike the
// ordinary StartResolved bridge, it also keeps runtime observation and zombie
// recycling behind the same under-lock witness validation.
func (h *SessionHandle) StartPreparedResolved(
	ctx context.Context,
	startCommand string,
	hints runtime.Config,
	expected sessionpkg.LiveBoundaryWitness,
) (result PreparedStartResult, err error) {
	event := h.beginOperationEvent(ctx, workerOperationStartResolved)
	defer func() { event.finish(err) }()

	id := h.currentSessionID()
	if id == "" {
		return PreparedStartResult{}, fmt.Errorf("%w: prepared start requires an existing bead-backed session", ErrOperationUnsupported)
	}
	authorization, err := h.authorizeLiveBoundary(ctx, id)
	if err != nil {
		return PreparedStartResult{}, err
	}
	if err := requireReconcilerExpectedWitness(expected, authorization); err != nil {
		return PreparedStartResult{}, err
	}
	command := strings.TrimSpace(startCommand)
	if command == "" {
		command, err = h.startCommand(id)
		if err != nil {
			return PreparedStartResult{}, err
		}
	}
	startHints := hints
	if strings.TrimSpace(startHints.Command) == "" {
		startHints = h.runtimeHints()
	}
	started, err := h.manager.StartPreparedRuntimeOnlyWithWitness(
		ctx,
		id,
		command,
		h.runtimeHintsWithAuthorization(startHints, authorization),
		expected,
	)
	result = PreparedStartResult{
		Attempted:       started.Attempted,
		Recycled:        started.Recycled,
		RecycleDuration: started.RecycleDuration,
	}
	return result, normalizeLiveBoundaryError(err)
}

// Attach ensures the worker runtime is live and then attaches the caller's
// terminal using the underlying session transport.
func (h *SessionHandle) Attach(ctx context.Context) (err error) {
	event := h.beginOperationEvent(ctx, workerOperationAttach)
	defer func() { event.finish(err) }()

	if h.currentSessionID() == "" {
		if _, err := h.authorizeLiveBoundary(ctx, ""); err != nil {
			return err
		}
	}
	id, err := h.ensureSessionID()
	if err != nil {
		return err
	}
	resumeCommand, err := h.startCommand(id)
	if err != nil {
		return err
	}
	authorization, err := h.authorizeLiveBoundary(ctx, id)
	if err != nil {
		return err
	}
	err = h.manager.AttachWithWitness(
		ctx,
		id,
		resumeCommand,
		h.runtimeHintsWithAuthorization(h.runtimeHints(), authorization),
		liveBoundaryWitness(authorization),
	)
	return normalizeLiveBoundaryError(err)
}

// Create materializes the worker session without requiring API callers to
// invoke session.Manager lifecycle methods directly.
func (h *SessionHandle) Create(ctx context.Context, mode CreateMode) (info sessionpkg.Info, err error) {
	event := h.beginOperationEvent(ctx, workerOperationCreate)
	defer func() { event.finish(err) }()

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.sessionID != "" {
		info, err = h.manager.Get(h.sessionID)
		return info, err
	}

	switch mode {
	case CreateModeDeferred:
		info, err = h.createDeferredLocked()
		return info, err
	case CreateModeStarted:
		if _, err = h.authorizeLiveBoundary(ctx, ""); err != nil {
			return sessionpkg.Info{}, err
		}
		info, err = h.createStartedLocked(ctx)
		return info, err
	default:
		err = fmt.Errorf("%w: unknown create mode %q", ErrHandleConfig, mode)
		return sessionpkg.Info{}, err
	}
}

// Reset requests a fresh restart for the worker while preserving the bead.
func (h *SessionHandle) Reset(ctx context.Context) (err error) {
	event := h.beginOperationEvent(ctx, workerOperationReset)
	defer func() { event.finish(err) }()

	id := h.currentSessionID()
	if id == "" {
		err = fmt.Errorf("%w: reset requires an existing bead-backed session", ErrOperationUnsupported)
		return err
	}
	err = h.manager.RequestFreshRestart(id)
	return err
}

// Stop suspends the worker runtime while preserving conversation state.
func (h *SessionHandle) Stop(ctx context.Context) (err error) {
	event := h.beginOperationEvent(ctx, workerOperationStop)
	defer func() { event.finish(err) }()

	id := h.currentSessionID()
	if id == "" {
		return nil
	}
	err = h.manager.Suspend(id)
	return err
}

// Kill terminates the live runtime without mutating the persisted lifecycle.
func (h *SessionHandle) Kill(ctx context.Context) (err error) {
	event := h.beginOperationEvent(ctx, workerOperationKill)
	defer func() { event.finish(err) }()

	id := h.currentSessionID()
	if id == "" {
		return nil
	}
	err = h.manager.Kill(id)
	return err
}

// Close permanently ends the worker session.
func (h *SessionHandle) Close(ctx context.Context) (err error) {
	_, err = h.CloseDetailed(ctx)
	return err
}

// CloseDetailed permanently ends the worker session and reports cleanup artifacts.
func (h *SessionHandle) CloseDetailed(ctx context.Context) (result sessionpkg.CloseResult, err error) {
	event := h.beginOperationEvent(ctx, workerOperationClose)
	defer func() { event.finish(err) }()

	id := h.currentSessionID()
	if id == "" {
		return result, nil
	}
	result, err = h.manager.CloseDetailed(id)
	return result, err
}

// Rename updates the user-facing session title.
func (h *SessionHandle) Rename(ctx context.Context, title string) (err error) {
	event := h.beginOperationEvent(ctx, workerOperationRename)
	defer func() { event.finish(err) }()

	id := h.currentSessionID()
	if id == "" {
		return nil
	}
	err = h.manager.Rename(id, strings.TrimSpace(title))
	return err
}

// Peek captures recent provider output without attaching.
func (h *SessionHandle) Peek(_ context.Context, lines int) (string, error) {
	id := h.currentSessionID()
	if id == "" {
		return "", sessionpkg.ErrSessionInactive
	}
	return h.manager.Peek(id, lines)
}

// State returns the worker-level lifecycle view.
func (h *SessionHandle) State(ctx context.Context) (State, error) {
	id := h.currentSessionID()
	if id == "" {
		return State{Phase: PhaseStopped, Provider: h.providerLabel()}, nil
	}

	info, err := h.manager.Get(id)
	if err != nil {
		return State{}, err
	}
	state := State{
		SessionID:   info.ID,
		SessionName: info.SessionName,
		Provider:    h.providerLabel(),
		Detail:      string(info.State),
	}

	switch info.State {
	case sessionpkg.StateStartPending, sessionpkg.StateCreating:
		state.Phase = PhaseStarting
		return state, nil
	case sessionpkg.StateDraining:
		state.Phase = PhaseStopping
		return state, nil
	case sessionpkg.StateAsleep, sessionpkg.StateSuspended, sessionpkg.StateDrained, sessionpkg.StateArchived:
		state.Phase = PhaseStopped
		return state, nil
	case sessionpkg.StateQuarantined:
		pending, err := h.Pending(ctx)
		if err != nil {
			return State{}, err
		}
		state.Phase = PhaseBlocked
		state.Pending = pending
		return state, nil
	case sessionpkg.StateActive, sessionpkg.StateAwake:
		pending, err := h.Pending(ctx)
		if err != nil {
			return State{}, err
		}
		if pending != nil {
			state.Phase = PhaseBlocked
			state.Pending = pending
			return state, nil
		}
		state.Phase = PhaseReady
		if strings.TrimSpace(info.SessionKey) == "" {
			if history, histErr := h.historyWithRequest(HistoryRequest{TailCompactions: 1}); histErr == nil && history != nil {
				if history.TailState.Activity == TailActivityInTurn {
					state.Phase = PhaseBusy
				}
			}
			return state, nil
		}
		if path, pathErr := h.manager.TranscriptPath(id, h.adapter.SearchPaths); pathErr == nil && strings.TrimSpace(path) != "" {
			if activity, actErr := h.adapter.TailActivity(path); actErr == nil && activity == TailActivityInTurn {
				state.Phase = PhaseBusy
			}
		}
		return state, nil
	default:
		if info.Closed {
			state.Phase = PhaseStopped
			return state, nil
		}
		state.Phase = PhaseUnknown
	}

	return state, nil
}

// Message sends a user turn to the worker.
func (h *SessionHandle) Message(ctx context.Context, req MessageRequest) (result MessageResult, err error) {
	event := h.beginOperationEvent(ctx, workerOperationMessage)
	defer func() {
		event.payload.Queued = boolPointer(result.Queued)
		event.finish(err)
		if err == nil {
			h.recordInvocationTelemetry(ctx)
		}
	}()

	if strings.TrimSpace(req.Text) == "" {
		err = fmt.Errorf("message text is required")
		return MessageResult{}, err
	}
	if h.currentSessionID() == "" {
		if _, err := h.authorizeLiveBoundary(ctx, ""); err != nil {
			return MessageResult{}, err
		}
	}
	id, err := h.ensureSessionID()
	if err != nil {
		return MessageResult{}, err
	}
	resumeCommand, err := h.startCommand(id)
	if err != nil {
		return MessageResult{}, err
	}
	authorization, err := h.authorizeLiveBoundary(ctx, id)
	if err != nil {
		return MessageResult{}, err
	}
	outcome, err := h.manager.SubmitWithWitness(
		ctx,
		id,
		req.Text,
		resumeCommand,
		h.runtimeHintsWithAuthorization(h.runtimeHints(), authorization),
		submitIntent(req.Delivery),
		liveBoundaryWitness(authorization),
	)
	if err != nil {
		return MessageResult{}, normalizeLiveBoundaryError(err)
	}
	result = MessageResult{Queued: outcome.Queued}
	return result, nil
}

// Interrupt soft-stops any in-flight worker turn.
func (h *SessionHandle) Interrupt(ctx context.Context, _ InterruptRequest) (err error) {
	event := h.beginOperationEvent(ctx, workerOperationInterrupt)
	defer func() { event.finish(err) }()

	id := h.currentSessionID()
	if id == "" {
		return nil
	}
	err = h.manager.StopTurn(id)
	return err
}

// Nudge sends a best-effort redirect message to the worker.
func (h *SessionHandle) Nudge(ctx context.Context, req NudgeRequest) (result NudgeResult, err error) {
	event := h.beginOperationEvent(ctx, workerOperationNudge)
	defer func() {
		event.payload.Delivered = boolPointer(result.Delivered)
		event.finish(err)
		if err == nil {
			h.recordInvocationTelemetry(ctx)
		}
	}()

	if strings.TrimSpace(req.Text) == "" {
		err = fmt.Errorf("nudge text is required")
		return NudgeResult{}, err
	}
	if h.currentSessionID() == "" {
		if _, err := h.authorizeLiveBoundary(ctx, ""); err != nil {
			return NudgeResult{}, err
		}
	}
	id, err := h.ensureSessionID()
	if err != nil {
		return NudgeResult{}, err
	}
	resumeCommand, err := h.startCommand(id)
	if err != nil {
		return NudgeResult{}, err
	}
	authorization, err := h.authorizeLiveBoundary(ctx, id)
	if err != nil {
		return NudgeResult{}, err
	}
	if err := requireExpectedTriggerAuthorization(req.ExpectedTriggerBeadID, req.ExpectedTriggerBeadStoreRef, authorization); err != nil {
		return NudgeResult{}, err
	}
	witness := liveBoundaryWitness(authorization)
	hints := h.runtimeHintsWithAuthorization(h.runtimeHints(), authorization)
	switch req.Delivery {
	case "", NudgeDeliveryDefault:
		if normalizeNudgeWakePolicy(req.Wake) == NudgeWakeLiveOnly {
			delivered, err := h.manager.SendLiveOnlyWithWitness(ctx, id, req.Text, witness)
			if err != nil {
				return NudgeResult{}, normalizeLiveBoundaryError(err)
			}
			result = NudgeResult{Delivered: delivered}
			return result, nil
		}
		if err := h.manager.SendWithWitness(ctx, id, req.Text, resumeCommand, hints, witness); err != nil {
			return NudgeResult{}, normalizeLiveBoundaryError(err)
		}
		result = NudgeResult{Delivered: true}
		return result, nil
	case NudgeDeliveryImmediate:
		if normalizeNudgeWakePolicy(req.Wake) == NudgeWakeLiveOnly {
			delivered, err := h.manager.SendImmediateLiveOnlyWithWitness(ctx, id, req.Text, witness)
			if err != nil {
				return NudgeResult{}, normalizeLiveBoundaryError(err)
			}
			result = NudgeResult{Delivered: delivered}
			return result, nil
		}
		if err := h.manager.SendImmediateWithWitness(ctx, id, req.Text, resumeCommand, hints, witness); err != nil {
			return NudgeResult{}, normalizeLiveBoundaryError(err)
		}
		result = NudgeResult{Delivered: true}
		return result, nil
	case NudgeDeliveryWaitIdle:
		if normalizeNudgeWakePolicy(req.Wake) == NudgeWakeLiveOnly {
			delivered, err := h.manager.TryWaitIdleNudgeLiveOnlyWithWitness(ctx, id, req.Source, req.Text, witness)
			if err != nil {
				return NudgeResult{}, normalizeLiveBoundaryError(err)
			}
			result = NudgeResult{Delivered: delivered}
			return result, nil
		}
		delivered, err := h.manager.TryWaitIdleNudgeWithWitness(ctx, id, req.Source, req.Text, resumeCommand, hints, witness)
		if err != nil {
			return NudgeResult{}, normalizeLiveBoundaryError(err)
		}
		result = NudgeResult{Delivered: delivered}
		return result, nil
	default:
		err = fmt.Errorf("unknown nudge delivery %q", req.Delivery)
		return NudgeResult{}, err
	}
}

func requireExpectedTriggerAuthorization(expectedID, expectedStoreRef string, authorization LaunchAuthorization) error {
	expectedID = strings.TrimSpace(expectedID)
	expectedStoreRef = strings.TrimSpace(expectedStoreRef)
	if expectedID == "" && expectedStoreRef == "" {
		return nil
	}
	if expectedID == "" || expectedStoreRef == "" ||
		strings.TrimSpace(authorization.TriggerBeadID) != expectedID ||
		strings.TrimSpace(authorization.TriggerBeadStoreRef) != expectedStoreRef {
		return fmt.Errorf("%w: live delivery expected trigger (%q, %q) does not match current authorization", ErrLaunchUnauthorized, expectedID, expectedStoreRef)
	}
	return nil
}

// authorizeLiveBoundary presents either a proposed new session (id empty) or
// an authoritative, freshly reloaded persisted record to the configured
// authorizer. It deliberately performs no write and no provider operation.
func (h *SessionHandle) authorizeLiveBoundary(ctx context.Context, id string) (LaunchAuthorization, error) {
	if h.authorizeLaunch == nil {
		return LaunchAuthorization{}, nil
	}
	req := LaunchAuthorizationRequest{
		Session:            cloneSessionSpec(h.session),
		Metadata:           cloneStringMap(h.session.Metadata),
		SessionStore:       h.sessionStore,
		CanonicalCityStore: h.canonicalCityStore,
	}
	if strings.TrimSpace(id) != "" {
		// Authorization is a persisted-authority read, not a live observation.
		// The ordinary worker read model enriches Info by routing ACP and probing
		// the provider; either would be an unauthorized side effect here.
		info, persisted, err := h.manager.PersistedStore().GetPersistedResponse(id)
		if err != nil {
			return LaunchAuthorization{}, bridgeSessionRecordError(id, err)
		}
		req.Info = &info
		req.Metadata = cloneStringMap(persisted.Metadata)
	}
	return h.authorizeLaunch(ctx, req)
}

func (h *SessionHandle) ensureSessionID() (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sessionID != "" {
		return h.sessionID, nil
	}
	info, err := h.createDeferredLocked()
	if err != nil {
		return "", err
	}
	return info.ID, nil
}

func (h *SessionHandle) createDeferredLocked() (sessionpkg.Info, error) {
	info, err := h.manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
		BeadOnly:     true,
		Alias:        h.session.Alias,
		ExplicitName: h.session.ExplicitName,
		Template:     h.session.Template,
		Title:        h.session.Title,
		Command:      h.session.Command,
		WorkDir:      h.session.WorkDir,
		Provider:     h.session.Provider,
		Transport:    h.session.Transport,
		Resume:       h.session.Resume,
		ExtraMeta:    cloneStringMap(h.session.Metadata),
	})
	if err != nil {
		return sessionpkg.Info{}, err
	}
	h.sessionID = info.ID
	return info, nil
}

func (h *SessionHandle) createStartedLocked(ctx context.Context) (sessionpkg.Info, error) {
	info, err := h.manager.CreateSession(ctx, sessionpkg.CreateOptions{
		Alias:        h.session.Alias,
		ExplicitName: h.session.ExplicitName,
		Template:     h.session.Template,
		Title:        h.session.Title,
		Command:      h.session.Command,
		WorkDir:      h.session.WorkDir,
		Provider:     h.session.Provider,
		Transport:    h.session.Transport,
		Env:          cloneStringMap(h.session.Env),
		Resume:       h.session.Resume,
		Hints:        cloneRuntimeConfig(h.session.Hints),
		ExtraMeta:    cloneStringMap(h.session.Metadata),
	})
	if err != nil {
		return sessionpkg.Info{}, err
	}
	h.sessionID = info.ID
	return info, nil
}

func (h *SessionHandle) currentSessionID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sessionID
}

func (h *SessionHandle) startCommand(id string) (string, error) {
	// Command construction is part of the authorization prelude. Read only the
	// persisted row here: the enriched worker read model can route ACP and probe
	// the provider before strict launch authority has been established.
	info, pr, err := h.manager.PersistedStore().GetPersistedResponse(id)
	if err != nil {
		return "", bridgeSessionRecordError(id, err)
	}
	if firstProviderSessionStart(info.State, pr.Metadata) &&
		h.session.Resume.SessionIDFlag != "" &&
		strings.TrimSpace(info.SessionKey) != "" {
		command := strings.TrimSpace(info.Command)
		if command == "" {
			command = strings.TrimSpace(h.session.Command)
		}
		if command == "" {
			command = strings.TrimSpace(info.Provider)
		}
		if command == "" {
			command = strings.TrimSpace(h.session.Provider)
		}
		if command == "" {
			return "", fmt.Errorf("%w: command is required for first start", ErrHandleConfig)
		}
		return command + " " + h.session.Resume.SessionIDFlag + " " + info.SessionKey, nil
	}
	resumeInfo := info
	if command := strings.TrimSpace(h.session.Command); command != "" {
		resumeInfo.Command = command
	}
	if provider := strings.TrimSpace(h.session.Provider); provider != "" {
		resumeInfo.Provider = provider
	}
	if resumeFlag := strings.TrimSpace(h.session.Resume.ResumeFlag); resumeFlag != "" {
		resumeInfo.ResumeFlag = resumeFlag
	}
	if resumeStyle := strings.TrimSpace(h.session.Resume.ResumeStyle); resumeStyle != "" {
		resumeInfo.ResumeStyle = resumeStyle
	}
	if resumeCommand := strings.TrimSpace(h.session.Resume.ResumeCommand); resumeCommand != "" {
		resumeInfo.ResumeCommand = resumeCommand
	}
	return sessionpkg.BuildResumeCommand(resumeInfo), nil
}

func firstProviderSessionStart(state sessionpkg.State, metadata map[string]string) bool {
	switch state {
	case sessionpkg.StateStartPending, sessionpkg.StateCreating:
	default:
		return false
	}
	if strings.TrimSpace(metadata["creation_complete_at"]) != "" {
		return false
	}
	return strings.TrimSpace(metadata["started_config_hash"]) == ""
}

func (h *SessionHandle) providerLabel() string {
	if h.session.Profile != "" {
		return string(h.session.Profile)
	}
	return h.session.Provider
}

func (h *SessionHandle) historyProvider(info sessionpkg.Info) string {
	if h.session.Profile != "" {
		return string(h.session.Profile)
	}
	if strings.TrimSpace(info.Provider) != "" {
		return info.Provider
	}
	return h.session.Provider
}

func (h *SessionHandle) runtimeHints() runtime.Config {
	cfg := cloneRuntimeConfig(h.session.Hints)
	cfg.Env = mergeStringMaps(cfg.Env, h.session.Env)
	return cfg
}

func (h *SessionHandle) runtimeHintsWithAuthorization(hints runtime.Config, authorization LaunchAuthorization) runtime.Config {
	cfg := cloneRuntimeConfig(hints)
	if enforcement := authorization.RuntimeEnforcement; enforcement != nil {
		cfg.ProjectHooksForbidden = enforcement.ProjectHooksForbidden
		cfg.ProviderName = strings.TrimSpace(enforcement.ProviderName)
		cfg.ProviderOverlayName = strings.TrimSpace(enforcement.ProviderOverlayName)
		cfg.WorkDir = strings.TrimSpace(enforcement.WorkDir)
	}
	triggerID := strings.TrimSpace(authorization.TriggerBeadID)
	storeRef := strings.TrimSpace(authorization.TriggerBeadStoreRef)
	if triggerID == "" || storeRef == "" {
		return cfg
	}
	if cfg.Env == nil {
		cfg.Env = make(map[string]string)
	}
	cfg.Env["GC_TRIGGER_BEAD_ID"] = triggerID
	cfg.Env["GC_TRIGGER_WORK_BEAD_ID"] = triggerID
	cfg.Env["GC_TRIGGER_BEAD_STORE_REF"] = storeRef
	cfg.Env["GC_TRIGGER_WORK_STORE_REF"] = storeRef
	return cfg
}

func liveBoundaryWitness(authorization LaunchAuthorization) sessionpkg.LiveBoundaryWitness {
	return sessionpkg.LiveBoundaryWitness{
		TriggerBeadID:       authorization.TriggerBeadID,
		TriggerBeadStoreRef: authorization.TriggerBeadStoreRef,
	}
}

func normalizeLiveBoundaryError(err error) error {
	if errors.Is(err, sessionpkg.ErrLiveBoundaryWitnessMismatch) {
		return fmt.Errorf("%w: %w", ErrLaunchUnauthorized, err)
	}
	return err
}

func submitIntent(intent DeliveryIntent) sessionpkg.SubmitIntent {
	switch intent {
	case DeliveryIntentFollowUp:
		return sessionpkg.SubmitIntentFollowUp
	case DeliveryIntentInterruptNow:
		return sessionpkg.SubmitIntentInterruptNow
	default:
		return sessionpkg.SubmitIntentDefault
	}
}
