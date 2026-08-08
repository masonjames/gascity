package agentutil

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/gastownhall/gascity/internal/agent"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
)

// ErrPersistedSessionIdentityConflict reports contradictory or ambiguous
// configured identity evidence on one persisted session row.
var ErrPersistedSessionIdentityConflict = errors.New("persisted session identity conflict")

// PersistedSessionIdentitySource names one independently evaluated durable
// session identity signal.
type PersistedSessionIdentitySource string

const (
	// PersistedIdentityTemplate identifies the session template signal.
	PersistedIdentityTemplate PersistedSessionIdentitySource = "template"
	// PersistedIdentityCommonName identifies the legacy common-name signal.
	PersistedIdentityCommonName PersistedSessionIdentitySource = "common_name"
	// PersistedIdentityAgentName identifies the persisted agent-name signal.
	PersistedIdentityAgentName PersistedSessionIdentitySource = "agent_name"
	// PersistedIdentityAgentLabel identifies an agent label signal.
	PersistedIdentityAgentLabel PersistedSessionIdentitySource = "agent_label"
	// PersistedIdentityConfiguredName identifies a configured named-session signal.
	PersistedIdentityConfiguredName PersistedSessionIdentitySource = "configured_named_identity"
	// PersistedIdentityAlias identifies the persisted alias signal.
	PersistedIdentityAlias PersistedSessionIdentitySource = "alias"
	// PersistedIdentityCanonical identifies the canonical identity record.
	PersistedIdentityCanonical PersistedSessionIdentitySource = "canonical_identity"
	// PersistedIdentitySessionName identifies the persisted runtime-name signal.
	PersistedIdentitySessionName PersistedSessionIdentitySource = "session_name"
)

// PersistedSessionIdentitySignal records how one durable value mapped to a
// configured base agent. ConcreteIdentity is populated for a specific pool,
// namepool, or configured adhoc runtime identity and empty for base-template
// or named-session evidence.
type PersistedSessionIdentitySignal struct {
	Source           PersistedSessionIdentitySource
	Value            string
	AgentIdentity    string
	ConcreteIdentity string
}

// PersistedSessionAgentResolution is the conflict-checked configured policy
// identity for one session row. Agent is a copy of the configured base agent;
// pool members never replace it with a synthetic member config.
type PersistedSessionAgentResolution struct {
	Agent    config.Agent
	Resolved bool
	Strict   bool
	Signals  []PersistedSessionIdentitySignal
}

type persistedAgentMatch struct {
	agent            config.Agent
	baseIdentity     string
	concreteIdentity string
	poolSlot         int
}

type persistedAgentCatalog struct {
	cfg    *config.City
	agents []config.Agent
}

// ResolvePersistedSessionAgent independently evaluates every durable
// configured-identity signal on info. Distinct base agents and incompatible
// concrete pool members fail closed. Unresolved ordinary legacy aliases are
// ignored; a present canonical record is authoritative enough that malformed
// or unknown values are conflicts.
//
// Work-bead routing metadata is intentionally absent from this API. In
// particular, gc.template on a work item is not session policy evidence.
func ResolvePersistedSessionAgent(cfg *config.City, info session.Info) (PersistedSessionAgentResolution, error) {
	if cfg == nil {
		return PersistedSessionAgentResolution{}, nil
	}
	catalog := newPersistedAgentCatalog(cfg)
	var signals []PersistedSessionIdentitySignal

	ordinary := []struct {
		source        PersistedSessionIdentitySource
		value         string
		authoritative bool
	}{
		{source: PersistedIdentityTemplate, value: info.Template, authoritative: true},
		{source: PersistedIdentityCommonName, value: info.CommonName},
		{source: PersistedIdentityAgentName, value: info.AgentName, authoritative: true},
		{source: PersistedIdentityAlias, value: info.Alias},
	}
	for _, candidate := range ordinary {
		signal, found, err := catalog.resolveSignal(candidate.source, candidate.value, true)
		if err != nil {
			return PersistedSessionAgentResolution{}, err
		}
		if !found && candidate.authoritative && strings.TrimSpace(candidate.value) != "" {
			return PersistedSessionAgentResolution{}, unknownPersistedSessionIdentity(candidate.source, candidate.value)
		}
		if found {
			signals = append(signals, signal)
		}
	}
	configuredNamedSignal, configuredNamedFound, err := catalog.resolveConfiguredNamedSignal(info.ConfiguredNamedIdentity)
	if err != nil {
		return PersistedSessionAgentResolution{}, err
	}
	if configuredNamedFound {
		signals = append(signals, configuredNamedSignal)
	}
	for _, label := range info.Labels {
		if !strings.HasPrefix(label, "agent:") {
			continue
		}
		signal, found, err := catalog.resolveSignal(PersistedIdentityAgentLabel, strings.TrimPrefix(label, "agent:"), true)
		if err != nil {
			return PersistedSessionAgentResolution{}, err
		}
		if !found && strings.TrimSpace(strings.TrimPrefix(label, "agent:")) != "" {
			return PersistedSessionAgentResolution{}, unknownPersistedSessionIdentity(PersistedIdentityAgentLabel, strings.TrimPrefix(label, "agent:"))
		}
		if found {
			signals = append(signals, signal)
		}
	}

	canonicalSignal, canonicalFound, err := catalog.resolveCanonical(info)
	if err != nil {
		return PersistedSessionAgentResolution{}, err
	}
	if canonicalFound {
		signals = append(signals, canonicalSignal)
	}

	// Only the raw persisted mirror is identity evidence. Info.SessionName is
	// fallback-filled from the bead ID when session_name metadata is absent and
	// may also carry a live runtime overlay; neither is durable configuration.
	rawSessionName := strings.TrimSpace(info.SessionNameMetadata)
	if rawSessionName != "" {
		if signal, found := catalog.resolveUniqueSessionName(rawSessionName); found {
			signals = append(signals, signal)
		}
	}

	return combinePersistedSessionSignals(catalog, signals)
}

func unknownPersistedSessionIdentity(source PersistedSessionIdentitySource, value string) error {
	return fmt.Errorf(
		"%w: %s %q is not a configured identity",
		ErrPersistedSessionIdentityConflict, source, strings.TrimSpace(value),
	)
}

func newPersistedAgentCatalog(cfg *config.City) persistedAgentCatalog {
	catalog := persistedAgentCatalog{cfg: cfg}
	for i := range cfg.Agents {
		configured := cfg.Agents[i]
		if configured.Scope == "rig" && strings.TrimSpace(configured.Dir) == "" {
			for _, rig := range cfg.Rigs {
				catalog.agents = append(catalog.agents, DeepCopyAgent(&configured, configured.Name, rig.Name))
			}
			continue
		}
		catalog.agents = append(catalog.agents, configured)
	}
	return catalog
}

func (c persistedAgentCatalog) resolveSignal(source PersistedSessionIdentitySource, raw string, includeNamed bool) (PersistedSessionIdentitySignal, bool, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return PersistedSessionIdentitySignal{}, false, nil
	}
	matches := c.resolveAgentValue(value)
	if includeNamed {
		namedMatches, err := c.resolveNamedValue(source, value, false)
		if err != nil {
			return PersistedSessionIdentitySignal{}, false, err
		}
		matches = append(matches, namedMatches...)
	}
	match, found, err := uniquePersistedAgentMatch(source, value, matches)
	if err != nil || !found {
		return PersistedSessionIdentitySignal{}, false, err
	}
	return PersistedSessionIdentitySignal{
		Source:           source,
		Value:            value,
		AgentIdentity:    match.baseIdentity,
		ConcreteIdentity: match.concreteIdentity,
	}, true, nil
}

func (c persistedAgentCatalog) resolveConfiguredNamedSignal(raw string) (PersistedSessionIdentitySignal, bool, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return PersistedSessionIdentitySignal{}, false, nil
	}
	matches, err := c.resolveNamedValue(PersistedIdentityConfiguredName, value, true)
	if err != nil {
		return PersistedSessionIdentitySignal{}, false, err
	}
	match, found, err := uniquePersistedAgentMatch(PersistedIdentityConfiguredName, value, matches)
	if err != nil || !found {
		return PersistedSessionIdentitySignal{}, false, err
	}
	return PersistedSessionIdentitySignal{
		Source:        PersistedIdentityConfiguredName,
		Value:         value,
		AgentIdentity: match.baseIdentity,
	}, true, nil
}

func (c persistedAgentCatalog) resolveAgentValue(value string) []persistedAgentMatch {
	var matches []persistedAgentMatch
	for i := range c.agents {
		configured := c.agents[i]
		baseIdentity := configured.QualifiedName()
		if persistedValueMatchesBase(configured, value) {
			matches = append(matches, persistedAgentMatch{agent: configured, baseIdentity: baseIdentity})
		}
		if concrete, ok := persistedAdhocIdentityMatch(configured, value); ok {
			matches = append(matches, persistedAgentMatch{
				agent:            configured,
				baseIdentity:     baseIdentity,
				concreteIdentity: concrete,
			})
		}
		if concrete, slot, ok := persistedPoolMemberMatch(configured, value); ok {
			matches = append(matches, persistedAgentMatch{
				agent:            configured,
				baseIdentity:     baseIdentity,
				concreteIdentity: concrete,
				poolSlot:         slot,
			})
		}
	}
	return matches
}

func persistedAdhocIdentityMatch(configured config.Agent, value string) (string, bool) {
	for _, prefix := range []string{
		configured.QualifiedName(),
		configured.BindingQualifiedName(),
		configured.Name,
		agent.SanitizeQualifiedNameForSession(configured.QualifiedName()),
		agent.SanitizeQualifiedNameForSession(configured.BindingQualifiedName()),
		agent.SanitizeQualifiedNameForSession(configured.Name),
	} {
		marker := strings.TrimSpace(prefix) + "-adhoc-"
		if marker == "-adhoc-" || !strings.HasPrefix(value, marker) {
			continue
		}
		suffix := strings.TrimPrefix(value, marker)
		if !validPersistedAdhocSuffix(suffix) {
			continue
		}
		return configured.QualifiedName() + "-adhoc-" + suffix, true
	}
	return "", false
}

func validPersistedAdhocSuffix(suffix string) bool {
	if suffix == "" {
		return false
	}
	for _, r := range suffix {
		if (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return false
		}
	}
	return true
}

func persistedValueMatchesBase(configured config.Agent, value string) bool {
	if config.AgentMatchesIdentity(&configured, value) || value == configured.BindingQualifiedName() || value == configured.Name {
		return true
	}
	if configured.BindingName != "" {
		return false
	}
	dir, local := config.ParseQualifiedName(value)
	if dir != configured.Dir {
		return false
	}
	_, legacyName, ok := strings.Cut(local, ".")
	return ok && strings.TrimSpace(legacyName) == configured.Name
}

func persistedPoolMemberMatch(configured config.Agent, value string) (string, int, bool) {
	if !configured.SupportsExpandedSessionIdentities() {
		return "", 0, false
	}
	for slot, name := range configured.NamepoolNames {
		memberSlot := slot + 1
		concrete := configured.QualifiedInstanceName(name)
		if value == concrete || value == name {
			return concrete, memberSlot, true
		}
		if configured.BindingName == "" && configured.Dir != "" && value == configured.Dir+"/"+name {
			return concrete, memberSlot, true
		}
	}
	if concrete, slot, ok := persistedDecoratedPoolMemberMatch(configured, value); ok {
		return concrete, slot, true
	}
	for _, prefix := range []string{configured.QualifiedName(), configured.BindingQualifiedName(), configured.Name} {
		if prefix == "" || !strings.HasPrefix(value, prefix+"-") {
			continue
		}
		rawSlot := strings.TrimPrefix(value, prefix+"-")
		slot, err := strconv.Atoi(rawSlot)
		if err != nil || slot < 1 || !persistedPoolSlotAllowed(configured, slot) {
			continue
		}
		memberName := PoolInstanceName(configured.Name, slot, configured)
		return configured.QualifiedInstanceName(memberName), slot, true
	}
	return "", 0, false
}

func persistedDecoratedPoolMemberMatch(configured config.Agent, value string) (string, int, bool) {
	for _, prefix := range []string{configured.QualifiedName(), configured.BindingQualifiedName(), configured.Name} {
		marker := strings.TrimSpace(prefix) + "__"
		if marker == "__" || !strings.HasPrefix(value, marker) {
			continue
		}
		decorated := strings.TrimPrefix(value, marker)
		separator := strings.LastIndex(decorated, "-")
		if separator < 1 || separator == len(decorated)-1 {
			continue
		}
		memberName := decorated[:separator]
		slot, err := strconv.Atoi(decorated[separator+1:])
		if err != nil || !persistedPoolSlotAllowed(configured, slot) || !validPersistedPoolMemberName(memberName) {
			continue
		}
		if len(configured.NamepoolNames) > 0 && (slot > len(configured.NamepoolNames) || configured.NamepoolNames[slot-1] != memberName) {
			continue
		}
		return value, slot, true
	}
	return "", 0, false
}

func validPersistedPoolMemberName(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && r != '.' && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

func persistedPoolSlotAllowed(configured config.Agent, slot int) bool {
	if slot < 1 {
		return false
	}
	if configured.HasUnlimitedSessionCapacity() {
		return true
	}
	maxSessions := configured.EffectiveMaxActiveSessions()
	return maxSessions != nil && slot <= *maxSessions
}

func (c persistedAgentCatalog) resolveNamedValue(source PersistedSessionIdentitySource, value string, required bool) ([]persistedAgentMatch, error) {
	var namedMatches []config.NamedSession
	for i := range c.cfg.NamedSessions {
		named := c.cfg.NamedSessions[i]
		if !persistedValueMatchesNamed(named, value) {
			continue
		}
		namedMatches = append(namedMatches, named)
	}
	if len(namedMatches) == 0 {
		if !required {
			return nil, nil
		}
		return nil, fmt.Errorf(
			"%w: %s %q is not a configured named session",
			ErrPersistedSessionIdentityConflict, source, value,
		)
	}
	if len(namedMatches) > 1 {
		return nil, fmt.Errorf(
			"%w: %s %q ambiguously names %d configured sessions",
			ErrPersistedSessionIdentityConflict, source, value, len(namedMatches),
		)
	}

	named := namedMatches[0]
	backingIdentity := named.TemplateQualifiedName()
	match, found, err := uniquePersistedAgentMatch(source, value, c.resolveAgentValue(backingIdentity))
	if err != nil {
		return nil, fmt.Errorf(
			"%w: configured named session %q has ambiguous backing agent %q",
			err, named.QualifiedName(), backingIdentity,
		)
	}
	if !found {
		return nil, fmt.Errorf(
			"%w: configured named session %q has no backing agent %q",
			ErrPersistedSessionIdentityConflict, named.QualifiedName(), backingIdentity,
		)
	}
	match.concreteIdentity = ""
	match.poolSlot = 0
	return []persistedAgentMatch{match}, nil
}

func persistedValueMatchesNamed(named config.NamedSession, value string) bool {
	if value == named.QualifiedName() || value == named.IdentityName() {
		return true
	}
	if named.Name != "" && value == named.Name {
		return true
	}
	return named.Name == "" && value == named.Template
}

func (c persistedAgentCatalog) resolveCanonical(info session.Info) (PersistedSessionIdentitySignal, bool, error) {
	name := strings.TrimSpace(info.CanonicalInstanceNameMetadata)
	rawSlot := strings.TrimSpace(info.CanonicalPoolSlotMetadata)
	if name == "" {
		if rawSlot != "" {
			return PersistedSessionIdentitySignal{}, false, fmt.Errorf(
				"%w: canonical pool slot %q has no canonical identity",
				ErrPersistedSessionIdentityConflict, rawSlot,
			)
		}
		return PersistedSessionIdentitySignal{}, false, nil
	}
	slot := 0
	if rawSlot != "" {
		parsed, err := strconv.Atoi(rawSlot)
		if err != nil || parsed < 1 {
			return PersistedSessionIdentitySignal{}, false, fmt.Errorf(
				"%w: canonical identity %q has invalid pool slot %q",
				ErrPersistedSessionIdentityConflict, name, rawSlot,
			)
		}
		slot = parsed
	}
	match, found, err := uniquePersistedAgentMatch(PersistedIdentityCanonical, name, c.resolveAgentValue(name))
	if err != nil {
		return PersistedSessionIdentitySignal{}, false, err
	}
	if !found {
		return PersistedSessionIdentitySignal{}, false, fmt.Errorf(
			"%w: canonical identity %q is not configured",
			ErrPersistedSessionIdentityConflict, name,
		)
	}
	switch {
	case match.concreteIdentity == "":
		if slot != 0 {
			return PersistedSessionIdentitySignal{}, false, fmt.Errorf(
				"%w: base canonical identity %q cannot carry pool slot %d",
				ErrPersistedSessionIdentityConflict, name, slot,
			)
		}
	case match.poolSlot == 0:
		if slot != 0 {
			return PersistedSessionIdentitySignal{}, false, fmt.Errorf(
				"%w: canonical adhoc identity %q cannot carry pool slot %d",
				ErrPersistedSessionIdentityConflict, name, slot,
			)
		}
	case slot == 0 || match.poolSlot != slot:
		return PersistedSessionIdentitySignal{}, false, fmt.Errorf(
			"%w: canonical member %q has slot %d, want %d",
			ErrPersistedSessionIdentityConflict, name, slot, match.poolSlot,
		)
	}
	return PersistedSessionIdentitySignal{
		Source:           PersistedIdentityCanonical,
		Value:            name,
		AgentIdentity:    match.baseIdentity,
		ConcreteIdentity: match.concreteIdentity,
	}, true, nil
}

func (c persistedAgentCatalog) resolveUniqueSessionName(sessionName string) (PersistedSessionIdentitySignal, bool) {
	var matches []persistedAgentMatch
	cityName := c.cfg.EffectiveCityName()
	sessionTemplate := c.cfg.Workspace.SessionTemplate
	for i := range c.agents {
		configured := c.agents[i]
		base := configured.QualifiedName()
		if agent.SessionNameFor(cityName, base, sessionTemplate) == sessionName {
			matches = append(matches, persistedAgentMatch{agent: configured, baseIdentity: base})
		}
		if !configured.SupportsExpandedSessionIdentities() || configured.HasUnlimitedSessionCapacity() {
			continue
		}
		maxSessions := configured.EffectiveMaxActiveSessions()
		if maxSessions == nil || *maxSessions < 1 {
			continue
		}
		for slot := 1; slot <= *maxSessions; slot++ {
			member := configured.QualifiedInstanceName(PoolInstanceName(configured.Name, slot, configured))
			if agent.SessionNameFor(cityName, member, sessionTemplate) == sessionName {
				matches = append(matches, persistedAgentMatch{
					agent: configured, baseIdentity: base, concreteIdentity: member, poolSlot: slot,
				})
			}
		}
	}
	for i := range c.cfg.NamedSessions {
		named := c.cfg.NamedSessions[i]
		if config.NamedSessionRuntimeName(cityName, c.cfg.Workspace, named.QualifiedName()) != sessionName {
			continue
		}
		backing := c.resolveAgentValue(named.TemplateQualifiedName())
		if match, found, err := uniquePersistedAgentMatch(PersistedIdentitySessionName, sessionName, backing); err == nil && found {
			match.concreteIdentity = ""
			match.poolSlot = 0
			matches = append(matches, match)
		}
	}
	match, found, err := uniquePersistedAgentMatch(PersistedIdentitySessionName, sessionName, matches)
	if err != nil || !found {
		return PersistedSessionIdentitySignal{}, false
	}
	return PersistedSessionIdentitySignal{
		Source:           PersistedIdentitySessionName,
		Value:            sessionName,
		AgentIdentity:    match.baseIdentity,
		ConcreteIdentity: match.concreteIdentity,
	}, true
}

func uniquePersistedAgentMatch(source PersistedSessionIdentitySource, value string, matches []persistedAgentMatch) (persistedAgentMatch, bool, error) {
	var selected persistedAgentMatch
	found := false
	for _, match := range matches {
		if !found {
			selected = match
			found = true
			continue
		}
		if selected.baseIdentity != match.baseIdentity {
			return persistedAgentMatch{}, false, fmt.Errorf(
				"%w: %s %q resolves to both %q and %q",
				ErrPersistedSessionIdentityConflict, source, value, selected.baseIdentity, match.baseIdentity,
			)
		}
		if selected.concreteIdentity == "" {
			selected.concreteIdentity = match.concreteIdentity
			selected.poolSlot = match.poolSlot
			continue
		}
		if match.concreteIdentity != "" && selected.concreteIdentity != match.concreteIdentity {
			return persistedAgentMatch{}, false, fmt.Errorf(
				"%w: %s %q resolves to incompatible members %q and %q",
				ErrPersistedSessionIdentityConflict, source, value, selected.concreteIdentity, match.concreteIdentity,
			)
		}
	}
	return selected, found, nil
}

func combinePersistedSessionSignals(catalog persistedAgentCatalog, signals []PersistedSessionIdentitySignal) (PersistedSessionAgentResolution, error) {
	if len(signals) == 0 {
		return PersistedSessionAgentResolution{}, nil
	}
	baseIdentity := signals[0].AgentIdentity
	concreteIdentity := signals[0].ConcreteIdentity
	for _, signal := range signals[1:] {
		if signal.AgentIdentity != baseIdentity {
			return PersistedSessionAgentResolution{}, fmt.Errorf(
				"%w: durable signals resolve to both %q and %q",
				ErrPersistedSessionIdentityConflict, baseIdentity, signal.AgentIdentity,
			)
		}
		if concreteIdentity == "" {
			concreteIdentity = signal.ConcreteIdentity
			continue
		}
		if signal.ConcreteIdentity != "" && signal.ConcreteIdentity != concreteIdentity {
			return PersistedSessionAgentResolution{}, fmt.Errorf(
				"%w: durable signals resolve to incompatible members %q and %q",
				ErrPersistedSessionIdentityConflict, concreteIdentity, signal.ConcreteIdentity,
			)
		}
	}
	for i := range catalog.agents {
		if catalog.agents[i].QualifiedName() != baseIdentity {
			continue
		}
		configured := catalog.agents[i]
		return PersistedSessionAgentResolution{
			Agent: configured.Clone(), Resolved: true, Strict: configured.ForbidsProjectHooks(), Signals: signals,
		}, nil
	}
	return PersistedSessionAgentResolution{}, fmt.Errorf(
		"%w: resolved agent %q disappeared from configuration",
		ErrPersistedSessionIdentityConflict, baseIdentity,
	)
}
