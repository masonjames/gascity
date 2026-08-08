package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/beads/beadstest"
	"github.com/gastownhall/gascity/internal/runtime"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
)

type failNextGetStore struct {
	beads.Store

	mu      sync.Mutex
	failGet bool
}

func (s *failNextGetStore) Get(id string) (beads.Bead, error) {
	s.mu.Lock()
	fail := s.failGet
	s.failGet = false
	s.mu.Unlock()
	if fail {
		return beads.Bead{}, fmt.Errorf("injected authoritative read failure")
	}
	return s.Store.Get(id)
}

func TestFactoryCloseDetailedWithWitnessFailsBeforeProviderOrSessionMutation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *failNextGetStore)
	}{
		{
			name: "classification_read_failure",
			mutate: func(_ *testing.T, store *failNextGetStore) {
				store.mu.Lock()
				store.failGet = true
				store.mu.Unlock()
			},
		},
		{
			name: "trigger_repoint_after_authorization",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backing := beads.NewMemStore()
			failing := &failNextGetStore{Store: backing}
			recorder := beadstest.NewRecordingStore(failing)
			provider := runtime.NewFake()
			manager := sessionpkg.NewManagerWithOptions(recorder, provider)
			created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
				BeadOnly: true,
				Template: "strict-worker",
				Title:    "Strict Worker",
				Command:  "codex",
				WorkDir:  t.TempDir(),
				Provider: "codex",
				ExtraMeta: map[string]string{
					beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
					beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
				},
			})
			if err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			authorizerCalls := 0
			factory, err := NewFactory(FactoryConfig{
				Store:              recorder,
				CanonicalCityStore: recorder,
				Provider:           provider,
				AuthorizeLaunch: func(_ context.Context, req LaunchAuthorizationRequest) (LaunchAuthorization, error) {
					authorizerCalls++
					authorization, err := RequireExactSessionTriggerAuthority(req, "city:fixture-city")
					if err != nil {
						return LaunchAuthorization{}, err
					}
					if tc.name == "trigger_repoint_after_authorization" {
						if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: map[string]string{
							beadmeta.TriggerBeadIDMetadataKey: "work-repointed",
						}}); err != nil {
							return LaunchAuthorization{}, err
						}
					}
					return authorization, nil
				},
			})
			if err != nil {
				t.Fatalf("NewFactory: %v", err)
			}
			if tc.mutate != nil {
				tc.mutate(t, failing)
			}
			recorder.Reset()
			_, _, _, err = factory.CloseDetailedWithWitness(
				context.Background(),
				created.ID,
				sessionpkg.LiveBoundaryWitness{TriggerBeadID: "work-exact", TriggerBeadStoreRef: "city:fixture-city"},
			)
			if err == nil {
				t.Fatal("CloseDetailedWithWitness error = nil, want refusal")
			}
			if tc.name == "trigger_repoint_after_authorization" && !errors.Is(err, sessionpkg.ErrLiveBoundaryWitnessMismatch) {
				t.Fatalf("CloseDetailedWithWitness error = %v, want witness mismatch", err)
			}
			if calls := provider.SnapshotCalls(); len(calls) != 0 {
				t.Fatalf("provider calls after refusal = %+v, want none", calls)
			}
			if calls := recorder.Calls(); len(calls) != 0 {
				t.Fatalf("session mutations after refusal = %+v, want none", calls)
			}
			current, getErr := backing.Get(created.ID)
			if getErr != nil {
				t.Fatalf("Get current session: %v", getErr)
			}
			if current.Status != "open" {
				t.Fatalf("session status = %q, want open", current.Status)
			}
			if authorizerCalls > 1 {
				t.Fatalf("authorizer calls = %d, want at most one", authorizerCalls)
			}
		})
	}
}

func TestFactoryWitnessRejectsTriggerDriftBeforeRunLiveOrRelaunchPreparation(t *testing.T) {
	for _, action := range []string{"run_live", "relaunch"} {
		t.Run(action, func(t *testing.T) {
			backing := beads.NewMemStore()
			recorder := beadstest.NewRecordingStore(backing)
			provider := runtime.NewFake()
			manager := sessionpkg.NewManagerWithOptions(recorder, provider)
			created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
				BeadOnly: true,
				Template: "strict-worker",
				Title:    "Strict Worker",
				Command:  "codex",
				WorkDir:  t.TempDir(),
				Provider: "codex",
				ExtraMeta: map[string]string{
					beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
					beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
				},
			})
			if err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			recorder.Reset()
			callbackCalls := 0
			factory, err := NewFactory(FactoryConfig{
				Store:              recorder,
				CanonicalCityStore: recorder,
				Provider:           provider,
				AuthorizeLaunch: func(_ context.Context, req LaunchAuthorizationRequest) (LaunchAuthorization, error) {
					callbackCalls++
					authorization, err := RequireExactSessionTriggerAuthority(req, "city:fixture-city")
					if err != nil {
						return LaunchAuthorization{}, err
					}
					authorization.RuntimeEnforcement = &LaunchRuntimeEnforcement{
						ProjectHooksForbidden: true,
						ProviderName:          "codex",
						WorkDir:               created.WorkDir,
					}
					if callbackCalls == 2 {
						if err := recorder.Update(created.ID, beads.UpdateOpts{Metadata: map[string]string{
							beadmeta.TriggerBeadIDMetadataKey: "work-repointed",
						}}); err != nil {
							return LaunchAuthorization{}, err
						}
						recorder.Reset()
					}
					return authorization, nil
				},
			})
			if err != nil {
				t.Fatalf("NewFactory: %v", err)
			}

			prepared := false
			expected := sessionpkg.LiveBoundaryWitness{TriggerBeadID: "work-exact", TriggerBeadStoreRef: "city:fixture-city"}
			switch action {
			case "run_live":
				err = factory.RunLive(context.Background(), created.ID, runtime.Config{Command: "codex", WorkDir: created.WorkDir}, expected)
			case "relaunch":
				err = factory.RelaunchPrepared(context.Background(), created.ID, expected, func() (runtime.Config, error) {
					prepared = true
					return runtime.Config{Command: "codex", WorkDir: created.WorkDir}, nil
				})
			}
			if !errors.Is(err, ErrLaunchUnauthorized) || !errors.Is(err, sessionpkg.ErrLiveBoundaryWitnessMismatch) {
				t.Fatalf("error = %v, want authorization witness mismatch", err)
			}
			if prepared {
				t.Fatal("relaunch preparation ran before witness rejection")
			}
			if calls := provider.SnapshotCalls(); len(calls) != 0 {
				t.Fatalf("provider calls after drift = %+v, want none", calls)
			}
			if calls := recorder.Calls(); len(calls) != 0 {
				t.Fatalf("session mutations after drift = %+v, want none", calls)
			}
		})
	}
}

func TestFactoryReconcilerLiveActionsRejectSuspensionAtManagerReload(t *testing.T) {
	for _, action := range []string{"run_live", "relaunch"} {
		t.Run(action, func(t *testing.T) {
			backing := beads.NewMemStore()
			recorder := beadstest.NewRecordingStore(backing)
			provider := runtime.NewFake()
			manager := sessionpkg.NewManagerWithOptions(recorder, provider)
			created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
				BeadOnly: true,
				Template: "strict-worker",
				Title:    "Strict Worker",
				Command:  "codex",
				WorkDir:  t.TempDir(),
				Provider: "codex",
				ExtraMeta: map[string]string{
					beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
					beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
				},
			})
			if err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			authorizerCalls := 0
			factory, err := NewFactory(FactoryConfig{
				Store:              recorder,
				CanonicalCityStore: recorder,
				Provider:           provider,
				AuthorizeLaunch: func(_ context.Context, req LaunchAuthorizationRequest) (LaunchAuthorization, error) {
					authorizerCalls++
					authorization, err := RequireExactSessionTriggerAuthority(req, "city:fixture-city")
					if err != nil {
						return LaunchAuthorization{}, err
					}
					authorization.RuntimeEnforcement = &LaunchRuntimeEnforcement{
						ProjectHooksForbidden: true,
						ProviderName:          "codex",
						WorkDir:               created.WorkDir,
					}
					if authorizerCalls == 2 {
						if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: map[string]string{
							"state": string(sessionpkg.StateSuspended),
						}}); err != nil {
							return LaunchAuthorization{}, err
						}
					}
					return authorization, nil
				},
			})
			if err != nil {
				t.Fatalf("NewFactory: %v", err)
			}
			prepared := false
			recorder.Reset()
			expected := sessionpkg.LiveBoundaryWitness{TriggerBeadID: "work-exact", TriggerBeadStoreRef: "city:fixture-city"}
			switch action {
			case "run_live":
				err = factory.RunLive(context.Background(), created.ID, runtime.Config{Command: "codex", WorkDir: created.WorkDir}, expected)
			case "relaunch":
				err = factory.RelaunchPrepared(context.Background(), created.ID, expected, func() (runtime.Config, error) {
					prepared = true
					return runtime.Config{Command: "codex", WorkDir: created.WorkDir}, nil
				})
			}
			if !errors.Is(err, ErrLaunchUnauthorized) || !errors.Is(err, sessionpkg.ErrLiveBoundaryWitnessMismatch) {
				t.Fatalf("error = %v, want suspended reconciler wake refusal", err)
			}
			if prepared {
				t.Fatal("relaunch preparation ran after suspension")
			}
			if calls := provider.SnapshotCalls(); len(calls) != 0 {
				t.Fatalf("provider calls after suspension = %+v, want none", calls)
			}
			if calls := recorder.Calls(); len(calls) != 0 {
				t.Fatalf("session mutations after suspension = %+v, want none", calls)
			}
		})
	}
}

func TestFactoryReconcilerLiveActionsRejectCapturedWitnessDriftBeforePreparationOrProvider(t *testing.T) {
	for _, action := range []string{"run_live", "relaunch"} {
		for _, drift := range []struct {
			name string
			meta map[string]string
		}{
			{name: "clear", meta: map[string]string{beadmeta.TriggerBeadIDMetadataKey: "", beadmeta.TriggerBeadStoreRefMetadataKey: ""}},
			{name: "repoint", meta: map[string]string{beadmeta.TriggerBeadIDMetadataKey: "work-other"}},
			{name: "suspend", meta: map[string]string{"state": string(sessionpkg.StateSuspended)}},
		} {
			t.Run(action+"/"+drift.name, func(t *testing.T) {
				backing := beads.NewMemStore()
				recorder := beadstest.NewRecordingStore(backing)
				provider := runtime.NewFake()
				manager := sessionpkg.NewManagerWithOptions(recorder, provider)
				created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
					BeadOnly: true,
					Template: "strict-worker",
					Title:    "Strict Worker",
					Command:  "codex",
					WorkDir:  t.TempDir(),
					Provider: "codex",
					ExtraMeta: map[string]string{
						beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
						beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
					},
				})
				if err != nil {
					t.Fatalf("CreateSession: %v", err)
				}
				factory, err := NewFactory(FactoryConfig{
					Store:              recorder,
					CanonicalCityStore: recorder,
					Provider:           provider,
					AuthorizeLaunch: func(_ context.Context, req LaunchAuthorizationRequest) (LaunchAuthorization, error) {
						authorization, err := RequireExactSessionTriggerAuthority(req, "city:fixture-city")
						if err != nil {
							return LaunchAuthorization{}, err
						}
						authorization.RuntimeEnforcement = &LaunchRuntimeEnforcement{
							ProjectHooksForbidden: true,
							ProviderName:          "codex",
							WorkDir:               created.WorkDir,
						}
						return authorization, nil
					},
				})
				if err != nil {
					t.Fatalf("NewFactory: %v", err)
				}
				expected := sessionpkg.LiveBoundaryWitness{TriggerBeadID: "work-exact", TriggerBeadStoreRef: "city:fixture-city"}
				if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: drift.meta}); err != nil {
					t.Fatalf("inject drift before Factory.SessionByID: %v", err)
				}
				afterDrift, err := backing.Get(created.ID)
				if err != nil {
					t.Fatalf("Get after drift: %v", err)
				}
				recorder.Reset()
				prepared := false
				switch action {
				case "run_live":
					err = factory.RunLive(context.Background(), created.ID, runtime.Config{Command: "codex", WorkDir: created.WorkDir}, expected)
				case "relaunch":
					err = factory.RelaunchPrepared(context.Background(), created.ID, expected, func() (runtime.Config, error) {
						prepared = true
						return runtime.Config{Command: "codex", WorkDir: created.WorkDir}, nil
					})
				}
				if !errors.Is(err, ErrLaunchUnauthorized) {
					t.Fatalf("error = %v, want ErrLaunchUnauthorized", err)
				}
				if prepared {
					t.Fatal("relaunch preparation ran after captured witness drift")
				}
				if calls := provider.SnapshotCalls(); len(calls) != 0 {
					t.Fatalf("provider calls after captured witness drift = %+v, want none", calls)
				}
				if calls := recorder.Calls(); len(calls) != 0 {
					t.Fatalf("session mutations after captured witness drift = %+v, want none", calls)
				}
				current, err := backing.Get(created.ID)
				if err != nil {
					t.Fatalf("Get final session: %v", err)
				}
				if current.Revision != afterDrift.Revision {
					t.Fatalf("session revision after refusal = %d, want %d", current.Revision, afterDrift.Revision)
				}
			})
		}
	}
}

func TestFactoryReconcilerLiveActionsPreserveZeroWitnessForInherit(t *testing.T) {
	store := beads.NewMemStore()
	provider := runtime.NewFake()
	manager := sessionpkg.NewManagerWithOptions(store, provider)
	created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
		BeadOnly: true,
		Template: "ordinary-worker",
		Title:    "Ordinary Worker",
		Command:  "codex",
		WorkDir:  t.TempDir(),
		Provider: "codex",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	factory, err := NewFactory(FactoryConfig{Store: store, Provider: provider})
	if err != nil {
		t.Fatalf("NewFactory: %v", err)
	}
	if err := factory.RunLive(context.Background(), created.ID, runtime.Config{Command: "codex", WorkDir: created.WorkDir}, sessionpkg.LiveBoundaryWitness{}); err != nil {
		t.Fatalf("RunLive with inherit zero witness: %v", err)
	}
	if err := provider.Start(context.Background(), created.SessionNameMetadata, runtime.Config{Command: "codex"}); err != nil {
		t.Fatalf("seed inherit runtime: %v", err)
	}
	prepared := false
	if err := factory.RelaunchPrepared(context.Background(), created.ID, sessionpkg.LiveBoundaryWitness{}, func() (runtime.Config, error) {
		prepared = true
		return runtime.Config{Command: "codex", WorkDir: created.WorkDir}, nil
	}); err != nil {
		t.Fatalf("RelaunchPrepared with inherit zero witness: %v", err)
	}
	if !prepared {
		t.Fatal("inherit relaunch preparation did not run")
	}
	if provider.CountCalls("RunLive", created.SessionNameMetadata) != 1 || provider.CountCalls("Relaunch", created.SessionNameMetadata) != 1 {
		t.Fatalf("inherit provider calls = %+v, want one RunLive and one Relaunch", provider.SnapshotCalls())
	}
}

func TestFactoryReconcilerLiveActionsPreserveCapturedWitnessForRoutedInherit(t *testing.T) {
	for _, action := range []string{"run_live", "relaunch"} {
		t.Run(action, func(t *testing.T) {
			store := beads.NewMemStore()
			provider := runtime.NewFake()
			manager := sessionpkg.NewManagerWithOptions(store, provider)
			created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
				BeadOnly: true,
				Template: "ordinary-worker",
				Title:    "Ordinary Worker",
				Command:  "codex",
				WorkDir:  t.TempDir(),
				Provider: "codex",
				ExtraMeta: map[string]string{
					beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
					beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
				},
			})
			if err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			factory, err := NewFactory(FactoryConfig{
				Store:              store,
				CanonicalCityStore: store,
				Provider:           provider,
				AuthorizeLaunch: func(_ context.Context, req LaunchAuthorizationRequest) (LaunchAuthorization, error) {
					return RequireExactSessionTriggerAuthority(req, "city:fixture-city")
				},
			})
			if err != nil {
				t.Fatalf("NewFactory: %v", err)
			}
			expected := sessionpkg.LiveBoundaryWitness{TriggerBeadID: "work-exact", TriggerBeadStoreRef: "city:fixture-city"}
			switch action {
			case "run_live":
				err = factory.RunLive(context.Background(), created.ID, runtime.Config{Command: "codex", WorkDir: created.WorkDir}, expected)
			case "relaunch":
				if err := provider.Start(context.Background(), created.SessionNameMetadata, runtime.Config{Command: "codex"}); err != nil {
					t.Fatalf("seed inherit runtime: %v", err)
				}
				err = factory.RelaunchPrepared(context.Background(), created.ID, expected, func() (runtime.Config, error) {
					return runtime.Config{Command: "codex", WorkDir: created.WorkDir}, nil
				})
			}
			if err != nil {
				t.Fatalf("routed inherit %s: %v", action, err)
			}
			method := "RunLive"
			if action == "relaunch" {
				method = "Relaunch"
			}
			if got := provider.CountCalls(method, created.SessionNameMetadata); got != 1 {
				t.Fatalf("%s provider calls = %d, want 1; calls=%+v", method, got, provider.SnapshotCalls())
			}
		})
	}
}

func TestFactoryReconcilerLiveActionsRejectCapturedWitnessDriftForRoutedInherit(t *testing.T) {
	for _, action := range []string{"run_live", "relaunch"} {
		t.Run(action, func(t *testing.T) {
			backing := beads.NewMemStore()
			recorder := beadstest.NewRecordingStore(backing)
			provider := runtime.NewFake()
			manager := sessionpkg.NewManagerWithOptions(recorder, provider)
			created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
				BeadOnly: true,
				Template: "ordinary-worker",
				Title:    "Ordinary Worker",
				Command:  "codex",
				WorkDir:  t.TempDir(),
				Provider: "codex",
				ExtraMeta: map[string]string{
					beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
					beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
				},
			})
			if err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			factory, err := NewFactory(FactoryConfig{
				Store:              recorder,
				CanonicalCityStore: recorder,
				Provider:           provider,
				AuthorizeLaunch: func(_ context.Context, req LaunchAuthorizationRequest) (LaunchAuthorization, error) {
					return RequireExactSessionTriggerAuthority(req, "city:fixture-city")
				},
			})
			if err != nil {
				t.Fatalf("NewFactory: %v", err)
			}
			expected := sessionpkg.LiveBoundaryWitness{TriggerBeadID: "work-exact", TriggerBeadStoreRef: "city:fixture-city"}
			if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: map[string]string{
				beadmeta.TriggerBeadIDMetadataKey: "work-repointed",
			}}); err != nil {
				t.Fatalf("repoint routed inherit trigger: %v", err)
			}
			afterDrift, err := backing.Get(created.ID)
			if err != nil {
				t.Fatalf("Get after drift: %v", err)
			}
			recorder.Reset()
			prepared := false
			switch action {
			case "run_live":
				err = factory.RunLive(context.Background(), created.ID, runtime.Config{Command: "codex", WorkDir: created.WorkDir}, expected)
			case "relaunch":
				err = factory.RelaunchPrepared(context.Background(), created.ID, expected, func() (runtime.Config, error) {
					prepared = true
					return runtime.Config{Command: "codex", WorkDir: created.WorkDir}, nil
				})
			}
			if !errors.Is(err, ErrLaunchUnauthorized) || !errors.Is(err, sessionpkg.ErrLiveBoundaryWitnessMismatch) {
				t.Fatalf("error = %v, want captured routed-inherit witness mismatch", err)
			}
			if prepared {
				t.Fatal("relaunch preparation ran after routed inherit witness drift")
			}
			if calls := provider.SnapshotCalls(); len(calls) != 0 {
				t.Fatalf("provider calls after routed inherit drift = %+v, want none", calls)
			}
			if calls := recorder.Calls(); len(calls) != 0 {
				t.Fatalf("session mutations after routed inherit drift = %+v, want none", calls)
			}
			current, err := backing.Get(created.ID)
			if err != nil {
				t.Fatalf("Get final session: %v", err)
			}
			if current.Revision != afterDrift.Revision {
				t.Fatalf("session revision after routed inherit refusal = %d, want %d", current.Revision, afterDrift.Revision)
			}
		})
	}
}

func TestPreparedStartWitnessRejectsFinalTriggerOrSuspensionDriftBeforeProviderObservation(t *testing.T) {
	for _, drift := range []struct {
		name string
		meta map[string]string
	}{
		{name: "clear", meta: map[string]string{beadmeta.TriggerBeadIDMetadataKey: "", beadmeta.TriggerBeadStoreRefMetadataKey: ""}},
		{name: "repoint", meta: map[string]string{beadmeta.TriggerBeadIDMetadataKey: "work-other"}},
		{name: "suspend", meta: map[string]string{"state": string(sessionpkg.StateSuspended)}},
	} {
		t.Run(drift.name, func(t *testing.T) {
			backing := beads.NewMemStore()
			recorder := beadstest.NewRecordingStore(backing)
			provider := runtime.NewFake()
			manager := sessionpkg.NewManagerWithOptions(recorder, provider)
			created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
				BeadOnly: true,
				Template: "strict-worker",
				Title:    "Strict Worker",
				Command:  "codex",
				WorkDir:  t.TempDir(),
				Provider: "codex",
				ExtraMeta: map[string]string{
					beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
					beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
				},
			})
			if err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			authorizerCalls := 0
			factory, err := NewFactory(FactoryConfig{
				Store:              recorder,
				CanonicalCityStore: recorder,
				Provider:           provider,
				AuthorizeLaunch: func(_ context.Context, req LaunchAuthorizationRequest) (LaunchAuthorization, error) {
					authorizerCalls++
					authorization, err := RequireExactSessionTriggerAuthority(req, "city:fixture-city")
					if err != nil {
						return LaunchAuthorization{}, err
					}
					authorization.RuntimeEnforcement = &LaunchRuntimeEnforcement{
						ProjectHooksForbidden: true,
						ProviderName:          "codex",
						WorkDir:               created.WorkDir,
					}
					if authorizerCalls == 2 {
						if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: drift.meta}); err != nil {
							return LaunchAuthorization{}, err
						}
					}
					return authorization, nil
				},
			})
			if err != nil {
				t.Fatalf("NewFactory: %v", err)
			}
			handle, err := factory.SessionByID(created.ID)
			if err != nil {
				t.Fatalf("SessionByID: %v", err)
			}
			preparedHandle, ok := handle.(PreparedLifecycleHandle)
			if !ok {
				t.Fatal("session handle does not expose PreparedLifecycleHandle")
			}
			recorder.Reset()
			result, err := preparedHandle.StartPreparedResolved(
				context.Background(),
				"codex",
				runtime.Config{Command: "codex", WorkDir: created.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true},
				sessionpkg.LiveBoundaryWitness{TriggerBeadID: "work-exact", TriggerBeadStoreRef: "city:fixture-city"},
			)
			if !errors.Is(err, ErrLaunchUnauthorized) || !errors.Is(err, sessionpkg.ErrLiveBoundaryWitnessMismatch) {
				t.Fatalf("StartPreparedResolved error = %v, want authorization witness mismatch", err)
			}
			if result.Attempted || result.Recycled {
				t.Fatalf("prepared result = %+v, want no provider attempt", result)
			}
			if calls := provider.SnapshotCalls(); len(calls) != 0 {
				t.Fatalf("provider calls after final drift = %+v, want none", calls)
			}
			if calls := recorder.Calls(); len(calls) != 0 {
				t.Fatalf("session mutations after final drift = %+v, want none", calls)
			}
		})
	}
}

func TestPreparedStartZeroWitnessRejectsOwnerAttachedAfterCandidateCapture(t *testing.T) {
	backing := beads.NewMemStore()
	recorder := beadstest.NewRecordingStore(backing)
	provider := runtime.NewFake()
	manager := sessionpkg.NewManagerWithOptions(recorder, provider)
	created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
		BeadOnly: true,
		Template: "ordinary-worker",
		Title:    "Ordinary Worker",
		Command:  "true",
		WorkDir:  t.TempDir(),
		Provider: "fake",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	factory, err := NewFactory(FactoryConfig{Store: recorder, Provider: provider})
	if err != nil {
		t.Fatalf("NewFactory: %v", err)
	}
	handle, err := factory.SessionByID(created.ID)
	if err != nil {
		t.Fatalf("SessionByID: %v", err)
	}
	preparedHandle, ok := handle.(PreparedLifecycleHandle)
	if !ok {
		t.Fatal("session handle does not expose PreparedLifecycleHandle")
	}
	if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: map[string]string{
		beadmeta.TriggerBeadIDMetadataKey:       "work-new",
		beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
	}}); err != nil {
		t.Fatalf("bind trigger after handle construction: %v", err)
	}
	recorder.Reset()
	result, err := preparedHandle.StartPreparedResolved(
		context.Background(),
		"true",
		runtime.Config{Command: "true", WorkDir: created.WorkDir, ProviderName: "fake"},
		sessionpkg.LiveBoundaryWitness{},
	)
	if !errors.Is(err, sessionpkg.ErrLiveBoundaryWitnessMismatch) {
		t.Fatalf("StartPreparedResolved error = %v, want zero-witness mismatch", err)
	}
	if result.Attempted || result.Recycled {
		t.Fatalf("prepared result = %+v, want no provider attempt", result)
	}
	if calls := provider.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("provider calls = %+v, want none", calls)
	}
	if calls := recorder.Calls(); len(calls) != 0 {
		t.Fatalf("session mutations = %+v, want none", calls)
	}
}
