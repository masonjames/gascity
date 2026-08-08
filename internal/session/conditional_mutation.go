package session

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/gastownhall/gascity/internal/beads"
)

// ConditionalMutationLeaseMetadataKey holds the cross-process ownership token
// for one authority-coupled session mutation. An empty value means no lease.
const ConditionalMutationLeaseMetadataKey = "session_mutation_lease"

var (
	// ErrConditionalMutationUnsupported reports that the session store cannot
	// provide an atomic mutation fence and exact metadata value-CAS release.
	ErrConditionalMutationUnsupported = errors.New("conditional session mutation unsupported")
	// ErrConditionalMutationHeld reports that another operation owns the
	// persisted mutation lease.
	ErrConditionalMutationHeld = errors.New("conditional session mutation lease held")
	// ErrConditionalMutationLost reports revision drift, a stale token, or a
	// replaced operation nonce.
	ErrConditionalMutationLost = errors.New("conditional session mutation lease lost")
	// ErrConditionalMutationStoreMismatch reports a token presented to a
	// different physical store, even when that store has the same session ID.
	ErrConditionalMutationStoreMismatch = errors.New("conditional session mutation store mismatch")
	// ErrConditionalMutationInvalid reports an incomplete request or a reserved
	// metadata-key mutation.
	ErrConditionalMutationInvalid = errors.New("invalid conditional session mutation")
)

// ConditionalMutationPredicate is a caller-owned authority or state
// constraint. The session layer assigns no meaning to zero, partial, or
// complete caller witnesses: Validate defines that truth table. Identity is
// persisted in the lease intent for exact attribution and diagnostics.
type ConditionalMutationPredicate struct {
	Identity string
	Validate func(Info, PersistedResponse) error
}

// ConditionalMutationRequest captures the exact decision snapshot that an
// authority-coupled mutation was based on. ExpectedRevision must come from the
// same persisted read as the caller's predicates; acquiring at a later re-read
// would adopt intervening drift instead of fencing it.
type ConditionalMutationRequest struct {
	SessionID        string
	ExpectedRevision int64
	Action           string
	Predicates       []ConditionalMutationPredicate
}

type conditionalMutationIntent struct {
	Version    int      `json:"version"`
	Nonce      string   `json:"nonce"`
	Action     string   `json:"action"`
	Predicates []string `json:"predicates"`
}

// ConditionalMutationLease is a process-local handle for one persisted
// cross-process mutation intent. It is usable only during its
// [Store.WithConditionalMutation] callback. Each mutation revalidates the
// exact nonce, revision, physical store, and caller predicates before writing.
type ConditionalMutationLease struct {
	mu sync.Mutex

	front           *Store
	writer          beads.ConditionalWriter
	predicateWriter beads.PredicateConditionalWriter
	metadataWriter  beads.MetadataCASWriter
	storeIdentity   beads.Store
	sessionID       string
	capturedRev     int64
	revision        int64
	action          string
	nonce           string
	encodedIntent   string
	predicates      []ConditionalMutationPredicate
	current         beads.Bead
	active          bool
	usePredicate    bool
	preserved       bool
	preserveErr     error
}

// WithConditionalMutation reserves an exact persisted mutation intent, runs
// fn while holding the existing in-process session lock, and releases any
// still-owned intent on return. The callback never runs when the captured
// revision or a predicate has drifted. Unsupported stores fail closed.
func (s *Store) WithConditionalMutation(req ConditionalMutationRequest, fn func(*ConditionalMutationLease) error) error {
	if fn == nil {
		return fmt.Errorf("%w: nil callback", ErrConditionalMutationInvalid)
	}
	req, err := normalizeConditionalMutationRequest(req)
	if err != nil {
		return err
	}
	return withSessionMutationLock(req.SessionID, func() (returnErr error) {
		lease, err := s.acquireConditionalMutationLocked(req)
		if err != nil {
			return err
		}
		defer func() {
			releaseErr := lease.Release()
			if releaseErr != nil {
				returnErr = errors.Join(returnErr, releaseErr)
			}
		}()
		return fn(lease)
	})
}

func normalizeConditionalMutationRequest(req ConditionalMutationRequest) (ConditionalMutationRequest, error) {
	req.SessionID = strings.TrimSpace(req.SessionID)
	req.Action = strings.TrimSpace(req.Action)
	if req.SessionID == "" {
		return ConditionalMutationRequest{}, fmt.Errorf("%w: empty session ID", ErrConditionalMutationInvalid)
	}
	if req.Action == "" {
		return ConditionalMutationRequest{}, fmt.Errorf("%w: empty action", ErrConditionalMutationInvalid)
	}
	predicates := make([]ConditionalMutationPredicate, len(req.Predicates))
	seen := make(map[string]struct{}, len(req.Predicates))
	for i, predicate := range req.Predicates {
		predicate.Identity = strings.TrimSpace(predicate.Identity)
		if predicate.Identity == "" || predicate.Validate == nil {
			return ConditionalMutationRequest{}, fmt.Errorf("%w: predicate %d needs identity and validator", ErrConditionalMutationInvalid, i)
		}
		if _, duplicate := seen[predicate.Identity]; duplicate {
			return ConditionalMutationRequest{}, fmt.Errorf("%w: duplicate predicate identity %q", ErrConditionalMutationInvalid, predicate.Identity)
		}
		seen[predicate.Identity] = struct{}{}
		predicates[i] = predicate
	}
	req.Predicates = predicates
	return req, nil
}

func (s *Store) acquireConditionalMutationLocked(req ConditionalMutationRequest) (*ConditionalMutationLease, error) {
	if s == nil || s.store.Store == nil {
		return nil, fmt.Errorf("%w: nil store", ErrConditionalMutationUnsupported)
	}
	predicateWriter, hasPredicateWriter := beads.PredicateConditionalWriterFor(s.store.Store)
	writer, ok := beads.ConditionalWriterFor(s.store.Store)
	if !ok {
		var resolveErr error
		writer, _, resolveErr = beads.ResolveConditionalWriter(s.store)
		if resolveErr != nil {
			return nil, fmt.Errorf("%w: %w", ErrConditionalMutationUnsupported, resolveErr)
		}
	}
	if writer == nil && !hasPredicateWriter {
		return nil, fmt.Errorf("%w: store exposes neither revision nor predicate conditional writes", ErrConditionalMutationUnsupported)
	}
	metadataWriter, hasMetadataWriter := beads.MetadataCASWriterFor(s.store.Store)
	if !hasMetadataWriter {
		return nil, fmt.Errorf("%w: store does not expose exact metadata value-CAS", ErrConditionalMutationUnsupported)
	}
	identity := beads.ResolveStoreIdentity(s.store.Store)
	if identity == nil {
		return nil, fmt.Errorf("%w: store has no physical identity", ErrConditionalMutationUnsupported)
	}

	current, err := s.validatedBead(req.SessionID)
	if err != nil {
		return nil, err
	}
	if current.Revision != req.ExpectedRevision {
		return nil, conditionalMutationRevisionLost(req.SessionID, req.ExpectedRevision, current.Revision)
	}
	if value := strings.TrimSpace(current.Metadata[ConditionalMutationLeaseMetadataKey]); value != "" {
		return nil, fmt.Errorf("%w: session %q", ErrConditionalMutationHeld, req.SessionID)
	}
	if err := validateConditionalMutationPredicates(current, req.Predicates); err != nil {
		return nil, err
	}

	nonce, err := newConditionalMutationNonce()
	if err != nil {
		return nil, fmt.Errorf("generating conditional mutation nonce: %w", err)
	}
	predicateIDs := make([]string, len(req.Predicates))
	for i := range req.Predicates {
		predicateIDs[i] = req.Predicates[i].Identity
	}
	encodedBytes, err := json.Marshal(conditionalMutationIntent{
		Version:    1,
		Nonce:      nonce,
		Action:     req.Action,
		Predicates: predicateIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("encoding conditional mutation intent: %w", err)
	}
	encoded := string(encodedBytes)
	lease := &ConditionalMutationLease{
		front:           s,
		writer:          writer,
		predicateWriter: predicateWriter,
		metadataWriter:  metadataWriter,
		storeIdentity:   identity,
		sessionID:       req.SessionID,
		capturedRev:     req.ExpectedRevision,
		action:          req.Action,
		nonce:           nonce,
		encodedIntent:   encoded,
		predicates:      req.Predicates,
		current:         current,
		revision:        current.Revision,
		active:          true,
		usePredicate:    writer == nil,
	}
	acquireOpts := beads.UpdateOpts{
		Metadata: map[string]string{ConditionalMutationLeaseMetadataKey: encoded},
	}
	if lease.usePredicate {
		return lease.acquireWithPredicateLocked(current, acquireOpts)
	}
	acquireErr := writer.UpdateIfMatch(req.SessionID, req.ExpectedRevision, acquireOpts)
	if acquireErr != nil {
		// CachingStore preserves ConditionalWriter structurally while probing its
		// backing at call time. A narrow Native backing refuses before writing;
		// retry through the independently resolved transactional predicate seam.
		if beads.IsConditionalWriteUnsupported(acquireErr) && hasPredicateWriter {
			lease.usePredicate = true
			return lease.acquireWithPredicateLocked(current, acquireOpts)
		}
		if beads.IsPreconditionFailed(acquireErr) || beads.IsConditionalWriteUnsupported(acquireErr) {
			return nil, classifyConditionalMutationWriteError(req.SessionID, acquireErr)
		}
	}
	committed, getErr := s.validatedBead(req.SessionID)
	if getErr != nil {
		if acquireErr != nil {
			getErr = errors.Join(classifyConditionalMutationWriteError(req.SessionID, acquireErr), getErr)
		}
		cleanupErr := lease.cleanupFailedAcquisitionIntentLocked()
		return nil, errors.Join(getErr, cleanupErr)
	}
	if committed.Revision == current.Revision ||
		!conditionalMutationRevisionPoststateEqual(current, committed, acquireOpts, false) {
		lostErr := conditionalMutationPostwriteLost(req.SessionID, current.Revision, committed.Revision)
		if acquireErr != nil {
			lostErr = errors.Join(classifyConditionalMutationWriteError(req.SessionID, acquireErr), lostErr)
		}
		cleanupErr := lease.cleanupFailedAcquisitionIntentLocked()
		return nil, errors.Join(lostErr, cleanupErr)
	}
	lease.current = committed
	lease.revision = committed.Revision
	if err := validateConditionalMutationPredicates(committed, lease.predicates); err != nil {
		releaseErr := lease.releaseLocked()
		return nil, errors.Join(err, releaseErr)
	}
	return lease, nil
}

func (l *ConditionalMutationLease) acquireWithPredicateLocked(expected beads.Bead, opts beads.UpdateOpts) (*ConditionalMutationLease, error) {
	post, err := l.predicateWriter.UpdateIfPredicate(l.sessionID, func(current beads.Bead) error {
		if !conditionalMutationSnapshotsEqual(expected, current) {
			return conditionalMutationSnapshotLost(l.sessionID)
		}
		if current.Revision != l.capturedRev {
			return conditionalMutationRevisionLost(l.sessionID, l.capturedRev, current.Revision)
		}
		if value := strings.TrimSpace(current.Metadata[ConditionalMutationLeaseMetadataKey]); value != "" {
			return fmt.Errorf("%w: session %q", ErrConditionalMutationHeld, l.sessionID)
		}
		return validateConditionalMutationPredicates(current, l.predicates)
	}, opts)
	if err != nil {
		cleanupErr := l.cleanupFailedAcquisitionIntentLocked()
		return nil, errors.Join(classifyConditionalMutationWriteError(l.sessionID, err), cleanupErr)
	}
	if post.Metadata[ConditionalMutationLeaseMetadataKey] != l.encodedIntent ||
		!conditionalMutationUpdateApplied(post, opts) {
		return nil, fmt.Errorf("%w: session %q predicate acquisition returned an invalid poststate", ErrConditionalMutationLost, l.sessionID)
	}
	if err := validateConditionalMutationPredicates(post, l.predicates); err != nil {
		l.current = post
		l.revision = post.Revision
		releaseErr := l.releaseLocked()
		return nil, errors.Join(err, releaseErr)
	}
	l.current = post
	l.revision = post.Revision
	return l, nil
}

func (l *ConditionalMutationLease) cleanupFailedAcquisitionIntentLocked() error {
	if l == nil || l.metadataWriter == nil {
		return nil
	}
	// The user callback has not started, so clearing only this unguessable
	// intent is always safe even if unrelated row fields drifted after an
	// ambiguous store commit. Never clear a foreign replacement.
	l.active = false
	swapped, err := l.metadataWriter.CompareAndSetMetadataKey(
		l.sessionID,
		ConditionalMutationLeaseMetadataKey,
		l.encodedIntent,
		"",
	)
	if err != nil {
		if current, getErr := l.front.validatedBead(l.sessionID); getErr == nil &&
			current.Metadata[ConditionalMutationLeaseMetadataKey] != l.encodedIntent {
			return nil
		}
		return fmt.Errorf("cleaning failed conditional mutation acquisition for %q: %w", l.sessionID, err)
	}
	if swapped {
		return nil
	}
	current, getErr := l.front.validatedBead(l.sessionID)
	if getErr != nil {
		return fmt.Errorf("checking failed conditional mutation acquisition for %q: %w", l.sessionID, getErr)
	}
	if current.Metadata[ConditionalMutationLeaseMetadataKey] == l.encodedIntent {
		return fmt.Errorf("%w: session %q still holds the failed acquisition intent", ErrConditionalMutationLost, l.sessionID)
	}
	return nil
}

// SessionID returns the immutable session ID bound to the lease.
func (l *ConditionalMutationLease) SessionID() string {
	if l == nil {
		return ""
	}
	return l.sessionID
}

// CapturedRevision returns the caller's original decision revision.
func (l *ConditionalMutationLease) CapturedRevision() int64 {
	if l == nil {
		return 0
	}
	return l.capturedRev
}

// Revision returns the current opaque revision after the most recent
// successful lease mutation.
func (l *ConditionalMutationLease) Revision() int64 {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.revision
}

// Nonce returns the random operation nonce persisted by acquisition.
func (l *ConditionalMutationLease) Nonce() string {
	if l == nil {
		return ""
	}
	return l.nonce
}

// Action returns the caller-supplied, role-neutral operation identity.
func (l *ConditionalMutationLease) Action() string {
	if l == nil {
		return ""
	}
	return l.action
}

// PredicateIdentities returns a defensive copy of the caller-supplied
// constraint identities persisted in the operation intent.
func (l *ConditionalMutationLease) PredicateIdentities() []string {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	identities := make([]string, len(l.predicates))
	for i := range l.predicates {
		identities[i] = l.predicates[i].Identity
	}
	return identities
}

// Current returns defensive persisted projections at the lease's current
// revision. The infrastructure lease key is omitted from Metadata.
func (l *ConditionalMutationLease) Current() (Info, PersistedResponse) {
	if l == nil {
		return Info{}, PersistedResponse{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return conditionalMutationProjections(l.current)
}

// SetMetadata conditionally writes one metadata key while the exact lease and
// caller predicates remain current.
func (l *ConditionalMutationLease) SetMetadata(key, value string) error {
	if strings.TrimSpace(key) == "" || key == ConditionalMutationLeaseMetadataKey {
		return fmt.Errorf("%w: reserved or empty metadata key %q", ErrConditionalMutationInvalid, key)
	}
	return l.Patch(beads.UpdateOpts{Metadata: map[string]string{key: value}})
}

// Patch atomically applies one patch after revalidating the exact persisted
// lease nonce, store fence, and every caller predicate.
func (l *ConditionalMutationLease) Patch(opts beads.UpdateOpts) error {
	if l == nil {
		return fmt.Errorf("%w: nil lease", ErrConditionalMutationLost)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.patchLocked(opts, false)
}

// Close closes the session row after revalidating the lease and predicates.
// The scope subsequently clears the still-owned nonce.
func (l *ConditionalMutationLease) Close() error {
	if l == nil {
		return fmt.Errorf("%w: nil lease", ErrConditionalMutationLost)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	current, err := l.validateLocked()
	if err != nil {
		return err
	}
	if l.usePredicate {
		post, err := l.predicateWriter.CloseIfPredicate(l.sessionID, l.exactPredicate(current), beads.UpdateOpts{})
		if err != nil {
			return classifyConditionalMutationWriteError(l.sessionID, err)
		}
		if post.Status != "closed" || post.Metadata[ConditionalMutationLeaseMetadataKey] != l.encodedIntent {
			return fmt.Errorf("%w: session %q conditional close returned an invalid poststate", ErrConditionalMutationLost, l.sessionID)
		}
		l.current = post
		l.revision = post.Revision
		return nil
	}
	before := current
	if err := l.writer.CloseIfMatch(l.sessionID, before.Revision); err != nil {
		if beads.IsPreconditionFailed(err) || beads.IsConditionalWriteUnsupported(err) {
			return classifyConditionalMutationWriteError(l.sessionID, err)
		}
		return l.reconcileRevisionWriteErrorLocked(before, beads.UpdateOpts{}, true, err)
	}
	return l.refreshAfterWriteLocked(before, beads.UpdateOpts{}, true)
}

// Commit atomically applies the final patch and clears the exact owned lease.
// A stale fence or foreign nonce leaves every caller field untouched and
// returns ErrConditionalMutationLost.
func (l *ConditionalMutationLease) Commit(opts beads.UpdateOpts) error {
	if l == nil {
		return fmt.Errorf("%w: nil lease", ErrConditionalMutationLost)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.patchLocked(opts, true)
}

// CommitClose atomically applies finalPatch, closes the session row, and
// clears this lease's exact owned intent in one predicate-conditional
// transaction. It returns with an inactive handle whose Current and Revision
// identify the authoritative closed poststate. Stores without the atomic
// patch-and-close capability fail closed.
func (l *ConditionalMutationLease) CommitClose(finalPatch beads.UpdateOpts) error {
	if l == nil {
		return fmt.Errorf("%w: nil lease", ErrConditionalMutationLost)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.predicateWriter == nil {
		return fmt.Errorf("%w: store does not expose atomic predicate close", ErrConditionalMutationUnsupported)
	}
	current, err := l.validateLocked()
	if err != nil {
		return err
	}
	finalPatch, err = cloneConditionalMutationUpdate(finalPatch)
	if err != nil {
		return err
	}
	if finalPatch.Metadata == nil {
		finalPatch.Metadata = make(map[string]string, 1)
	}
	finalPatch.Metadata[ConditionalMutationLeaseMetadataKey] = ""
	post, err := l.predicateWriter.CloseIfPredicate(l.sessionID, l.exactPredicate(current), finalPatch)
	if err != nil {
		return classifyConditionalMutationWriteError(l.sessionID, err)
	}
	if post.Status != "closed" || post.Metadata[ConditionalMutationLeaseMetadataKey] != "" ||
		!conditionalMutationUpdateApplied(post, finalPatch) {
		return fmt.Errorf("%w: session %q atomic final close returned an invalid poststate", ErrConditionalMutationLost, l.sessionID)
	}
	l.current = post
	l.revision = post.Revision
	l.active = false
	return nil
}

// Preserve intentionally leaves this lease's exact persisted intent held and
// disables deferred cleanup for the process-local handle. Callers use it once
// an external effect may have started but neither exact final commit nor safe
// containment can be proven. It is safe across row revision drift so long as
// the exact nonce and intent remain current; a foreign replacement is never
// cleared. Preserve is idempotent and recovery is deliberately out of scope.
func (l *ConditionalMutationLease) Preserve() error {
	if l == nil {
		return fmt.Errorf("%w: nil lease", ErrConditionalMutationLost)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.preserved {
		return l.preserveErr
	}
	if !l.active {
		return fmt.Errorf("%w: session %q lease is inactive", ErrConditionalMutationLost, l.sessionID)
	}

	// Disable Release before any fallible attestation. On uncertainty, clearing
	// a possibly safety-critical hold is less safe than retaining it.
	l.active = false
	l.preserved = true
	if err := l.validateStore(l.front); err != nil {
		l.preserveErr = err
		return err
	}
	current, err := l.front.validatedBead(l.sessionID)
	if err != nil {
		l.preserveErr = err
		return err
	}
	if current.Metadata[ConditionalMutationLeaseMetadataKey] != l.encodedIntent {
		err = fmt.Errorf("%w: session %q lease nonce or intent changed", ErrConditionalMutationLost, l.sessionID)
		l.preserveErr = err
		return err
	}
	l.current = current
	l.revision = current.Revision
	return nil
}

func (l *ConditionalMutationLease) patchLocked(opts beads.UpdateOpts, commit bool) error {
	current, err := l.validateLocked()
	if err != nil {
		return err
	}
	opts, err = cloneConditionalMutationUpdate(opts)
	if err != nil {
		return err
	}
	if commit {
		if opts.Metadata == nil {
			opts.Metadata = make(map[string]string, 1)
		}
		opts.Metadata[ConditionalMutationLeaseMetadataKey] = ""
	}
	if l.usePredicate {
		post, err := l.predicateWriter.UpdateIfPredicate(l.sessionID, l.exactPredicate(current), opts)
		if err != nil {
			return classifyConditionalMutationWriteError(l.sessionID, err)
		}
		wantLease := l.encodedIntent
		if commit {
			wantLease = ""
		}
		if post.Metadata[ConditionalMutationLeaseMetadataKey] != wantLease ||
			!conditionalMutationUpdateApplied(post, opts) {
			return fmt.Errorf("%w: session %q conditional patch returned an invalid poststate", ErrConditionalMutationLost, l.sessionID)
		}
		l.current = post
		l.revision = post.Revision
		if commit {
			l.active = false
		}
		return nil
	}
	before := current
	if err := l.writer.UpdateIfMatch(l.sessionID, before.Revision, opts); err != nil {
		return l.reconcileUpdateErrorLocked(before, opts, commit, err)
	}
	if err := l.refreshAfterWriteLocked(before, opts, false); err != nil {
		return err
	}
	if commit {
		l.active = false
	}
	return nil
}

func (l *ConditionalMutationLease) exactPredicate(expected beads.Bead) beads.BeadPredicate {
	return func(current beads.Bead) error {
		if !conditionalMutationSnapshotsEqual(expected, current) {
			return conditionalMutationSnapshotLost(l.sessionID)
		}
		if current.Metadata[ConditionalMutationLeaseMetadataKey] != l.encodedIntent {
			return fmt.Errorf("%w: session %q lease nonce or intent changed", ErrConditionalMutationLost, l.sessionID)
		}
		return validateConditionalMutationPredicates(current, l.predicates)
	}
}

func (l *ConditionalMutationLease) reconcileUpdateErrorLocked(before beads.Bead, opts beads.UpdateOpts, commit bool, writeErr error) error {
	if beads.IsPreconditionFailed(writeErr) || beads.IsConditionalWriteUnsupported(writeErr) {
		return classifyConditionalMutationWriteError(l.sessionID, writeErr)
	}
	if err := l.reconcileRevisionWriteErrorLocked(before, opts, false, writeErr); err != nil {
		return err
	}
	if commit {
		l.active = false
	}
	return nil
}

func (l *ConditionalMutationLease) reconcileRevisionWriteErrorLocked(before beads.Bead, opts beads.UpdateOpts, closeRow bool, writeErr error) error {
	current, getErr := l.front.validatedBead(l.sessionID)
	if getErr != nil {
		return errors.Join(classifyConditionalMutationWriteError(l.sessionID, writeErr), getErr)
	}
	if current.Revision == before.Revision ||
		!conditionalMutationRevisionPoststateEqual(before, current, opts, closeRow) {
		return errors.Join(
			classifyConditionalMutationWriteError(l.sessionID, writeErr),
			conditionalMutationPostwriteLost(l.sessionID, before.Revision, current.Revision),
		)
	}
	l.current = current
	l.revision = current.Revision
	return nil
}

// Release value-CAS clears only this lease's exact encoded nonce and intent.
// It deliberately does not re-run predicates: cleanup remains safe after
// side-effect errors or unrelated row drift, while a foreign replacement is
// never cleared.
func (l *ConditionalMutationLease) Release() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.releaseLocked()
}

func (l *ConditionalMutationLease) releaseLocked() error {
	if !l.active {
		return nil
	}
	if err := l.validateStore(l.front); err != nil {
		l.active = false
		return err
	}
	l.active = false
	swapped, err := l.metadataWriter.CompareAndSetMetadataKey(
		l.sessionID,
		ConditionalMutationLeaseMetadataKey,
		l.encodedIntent,
		"",
	)
	if err != nil {
		if current, getErr := l.front.validatedBead(l.sessionID); getErr == nil && current.Metadata[ConditionalMutationLeaseMetadataKey] == "" {
			l.current = current
			l.revision = current.Revision
			return nil
		}
		return fmt.Errorf("releasing conditional mutation lease for %q: %w", l.sessionID, err)
	}
	if !swapped {
		return fmt.Errorf("%w: session %q lease nonce was replaced", ErrConditionalMutationLost, l.sessionID)
	}
	if current, getErr := l.front.validatedBead(l.sessionID); getErr == nil {
		l.current = current
		l.revision = current.Revision
	}
	return nil
}

func (l *ConditionalMutationLease) validateLocked() (beads.Bead, error) {
	if !l.active {
		return beads.Bead{}, fmt.Errorf("%w: session %q lease is inactive", ErrConditionalMutationLost, l.sessionID)
	}
	if err := l.validateStore(l.front); err != nil {
		return beads.Bead{}, err
	}
	current, err := l.front.validatedBead(l.sessionID)
	if err != nil {
		return beads.Bead{}, err
	}
	if l.usePredicate {
		if !conditionalMutationSnapshotsEqual(l.current, current) {
			return beads.Bead{}, conditionalMutationSnapshotLost(l.sessionID)
		}
	} else if current.Revision != l.revision {
		return beads.Bead{}, conditionalMutationRevisionLost(l.sessionID, l.revision, current.Revision)
	}
	if current.Metadata[ConditionalMutationLeaseMetadataKey] != l.encodedIntent {
		return beads.Bead{}, fmt.Errorf("%w: session %q lease nonce or intent changed", ErrConditionalMutationLost, l.sessionID)
	}
	if err := validateConditionalMutationPredicates(current, l.predicates); err != nil {
		return beads.Bead{}, err
	}
	return current, nil
}

func (l *ConditionalMutationLease) validateStore(front *Store) error {
	if l == nil || front == nil || front.store.Store == nil ||
		!beads.SameStoreIdentity(l.storeIdentity, front.store.Store) {
		return fmt.Errorf("%w: session %q", ErrConditionalMutationStoreMismatch, l.sessionID)
	}
	return nil
}

func (l *ConditionalMutationLease) refreshAfterWriteLocked(before beads.Bead, opts beads.UpdateOpts, closeRow bool) error {
	current, err := l.front.validatedBead(l.sessionID)
	if err != nil {
		return err
	}
	if current.Revision == before.Revision {
		return fmt.Errorf("%w: session %q conditional write did not advance revision", ErrConditionalMutationLost, l.sessionID)
	}
	if !conditionalMutationRevisionPoststateEqual(before, current, opts, closeRow) {
		return conditionalMutationPostwriteLost(l.sessionID, before.Revision, current.Revision)
	}
	l.current = current
	l.revision = current.Revision
	return nil
}

// RequireNoConditionalMutationLease rejects a persisted snapshot owned by an
// active mutation operation. A sanctioned strict writer calls this helper on
// the same snapshot whose Revision it supplies to UpdateIfMatch.
func RequireNoConditionalMutationLease(persisted PersistedResponse) error {
	if strings.TrimSpace(persisted.Metadata[ConditionalMutationLeaseMetadataKey]) != "" {
		return ErrConditionalMutationHeld
	}
	return nil
}

func validateConditionalMutationPredicates(b beads.Bead, predicates []ConditionalMutationPredicate) error {
	info, persisted := conditionalMutationProjections(b)
	for _, predicate := range predicates {
		if err := predicate.Validate(info, persisted); err != nil {
			return fmt.Errorf("conditional mutation predicate %q: %w", predicate.Identity, err)
		}
	}
	return nil
}

func conditionalMutationProjections(b beads.Bead) (Info, PersistedResponse) {
	copyBead := b
	copyBead.Labels = append([]string(nil), b.Labels...)
	copyBead.Metadata = make(map[string]string, len(b.Metadata))
	for key, value := range b.Metadata {
		if key != ConditionalMutationLeaseMetadataKey {
			copyBead.Metadata[key] = value
		}
	}
	return infoFromPersistedBead(copyBead), PersistedResponseFromBead(copyBead)
}

func cloneConditionalMutationUpdate(opts beads.UpdateOpts) (beads.UpdateOpts, error) {
	if _, reserved := opts.Metadata[ConditionalMutationLeaseMetadataKey]; reserved {
		return beads.UpdateOpts{}, fmt.Errorf("%w: metadata key %q is lease-owned", ErrConditionalMutationInvalid, ConditionalMutationLeaseMetadataKey)
	}
	opts.Labels = append([]string(nil), opts.Labels...)
	opts.RemoveLabels = append([]string(nil), opts.RemoveLabels...)
	if opts.Metadata != nil {
		metadata := make(map[string]string, len(opts.Metadata)+1)
		for key, value := range opts.Metadata {
			metadata[key] = value
		}
		opts.Metadata = metadata
	}
	return opts, nil
}

func conditionalMutationUpdateApplied(current beads.Bead, opts beads.UpdateOpts) bool {
	if opts.Title != nil && current.Title != *opts.Title {
		return false
	}
	if opts.Status != nil && current.Status != *opts.Status {
		return false
	}
	if opts.Type != nil && current.Type != *opts.Type {
		return false
	}
	if opts.Priority != nil && (current.Priority == nil || *current.Priority != *opts.Priority) {
		return false
	}
	if opts.Description != nil && current.Description != *opts.Description {
		return false
	}
	if opts.ParentID != nil && current.ParentID != *opts.ParentID {
		return false
	}
	if opts.Assignee != nil && current.Assignee != *opts.Assignee {
		return false
	}
	for _, label := range opts.Labels {
		if !conditionalMutationHasLabel(current.Labels, label) {
			return false
		}
	}
	for _, label := range opts.RemoveLabels {
		if conditionalMutationHasLabel(current.Labels, label) {
			return false
		}
	}
	for key, value := range opts.Metadata {
		if current.Metadata[key] != value {
			return false
		}
	}
	return true
}

func conditionalMutationHasLabel(labels []string, expected string) bool {
	for _, label := range labels {
		if label == expected {
			return true
		}
	}
	return false
}

// conditionalMutationRevisionPoststateEqual proves that candidate is exactly
// the row produced by applying opts (and, for CloseIfMatch, the closed status)
// to before. Unlike a touched-field check, it rejects drift in every untouched
// hydrated field, the complete label set, the complete metadata map, and the
// ownership ClaimFence.
//
// Revision is an opaque equality token, so callers separately require that it
// changed and normalize only its new value here. UpdatedAt is likewise authored
// inside the backend write and cannot be predicted from UpdateOpts; candidate's
// timestamp is therefore the expected timestamp of the candidate write. Every
// other hydrated value is derived exclusively from before plus the requested
// mutation.
func conditionalMutationRevisionPoststateEqual(before, candidate beads.Bead, opts beads.UpdateOpts, closeRow bool) bool {
	expected := before
	expected.Labels = append([]string(nil), before.Labels...)
	if before.Metadata != nil {
		expected.Metadata = make(beads.StringMap, len(before.Metadata)+len(opts.Metadata))
		for key, value := range before.Metadata {
			expected.Metadata[key] = value
		}
	}
	if before.Priority != nil {
		priority := *before.Priority
		expected.Priority = &priority
	}

	if opts.Title != nil {
		expected.Title = *opts.Title
	}
	if opts.Status != nil {
		expected.Status = *opts.Status
	}
	if opts.Type != nil {
		expected.Type = *opts.Type
	}
	if opts.Priority != nil {
		priority := *opts.Priority
		expected.Priority = &priority
	}
	if opts.Description != nil {
		expected.Description = *opts.Description
	}
	if opts.ParentID != nil {
		expected.ParentID = *opts.ParentID
	}
	if opts.Assignee != nil {
		expected.Assignee = *opts.Assignee
	}
	if len(opts.Metadata) > 0 {
		if expected.Metadata == nil {
			expected.Metadata = make(beads.StringMap, len(opts.Metadata))
		}
		for key, value := range opts.Metadata {
			expected.Metadata[key] = value
		}
	}
	if len(opts.Labels) > 0 {
		expected.Labels = append(expected.Labels, opts.Labels...)
	}
	if len(opts.RemoveLabels) > 0 {
		remove := make(map[string]struct{}, len(opts.RemoveLabels))
		for _, label := range opts.RemoveLabels {
			remove[label] = struct{}{}
		}
		filtered := expected.Labels[:0]
		for _, label := range expected.Labels {
			if _, removed := remove[label]; !removed {
				filtered = append(filtered, label)
			}
		}
		expected.Labels = filtered
	}
	if conditionalMutationOwnershipTransition(before, opts) {
		expected.ClaimFence++
	}
	if closeRow {
		expected.Status = "closed"
	}

	expected.Revision = candidate.Revision
	expected.UpdatedAt = candidate.UpdatedAt
	return conditionalMutationSnapshotsEqual(expected, candidate)
}

func conditionalMutationOwnershipTransition(before beads.Bead, opts beads.UpdateOpts) bool {
	if opts.Assignee != nil && *opts.Assignee != before.Assignee {
		return true
	}
	return opts.Status != nil && *opts.Status != "" && before.Status == "closed" && *opts.Status != "closed"
}

func conditionalMutationSnapshotsEqual(expected, current beads.Bead) bool {
	return beads.PredicateSnapshotEqual(expected, current)
}

func conditionalMutationSnapshotLost(id string) error {
	return fmt.Errorf("%w: session %q persisted snapshot changed", ErrConditionalMutationLost, id)
}

func conditionalMutationRevisionLost(id string, expected, current int64) error {
	return fmt.Errorf("%w: %w", ErrConditionalMutationLost, &beads.PreconditionFailedError{
		ID: id, Expected: expected, Current: current,
	})
}

func conditionalMutationPostwriteLost(id string, expectedRevision, currentRevision int64) error {
	return errors.Join(
		conditionalMutationSnapshotLost(id),
		conditionalMutationRevisionLost(id, expectedRevision, currentRevision),
	)
}

func classifyConditionalMutationWriteError(id string, err error) error {
	if errors.Is(err, ErrConditionalMutationLost) ||
		errors.Is(err, ErrConditionalMutationHeld) ||
		errors.Is(err, ErrConditionalMutationStoreMismatch) ||
		errors.Is(err, ErrConditionalMutationInvalid) {
		return err
	}
	if beads.IsPreconditionFailed(err) {
		return fmt.Errorf("%w: %w", ErrConditionalMutationLost, err)
	}
	if beads.IsConditionalWriteUnsupported(err) {
		return fmt.Errorf("%w: %w", ErrConditionalMutationUnsupported, err)
	}
	return fmt.Errorf("conditional mutation write for %q: %w", id, err)
}

func newConditionalMutationNonce() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}
