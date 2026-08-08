package beads

import "testing"

// typedStore is the shape every per-class wrapper has: a struct exposing an
// embedded Store via a field named Store. The test below asserts each wrapper
// conforms to it so the embedding contract is enforced at compile time.
type typedStore interface {
	Store
}

func TestClassStoresEmbedStore(t *testing.T) {
	base := NewMemStore()

	cases := []struct {
		name string
		// get returns the typed wrapper value (as a Store) and its embedded
		// .Store field, so the test can confirm round-trip identity.
		get func() (Store, Store)
	}{
		{"WorkStore", func() (Store, Store) { s := WorkStore{Store: base}; return s, s.Store }},
		{"GraphStore", func() (Store, Store) { s := GraphStore{Store: base}; return s, s.Store }},
		{"SessionStore", func() (Store, Store) { s := SessionStore{Store: base}; return s, s.Store }},
		{"MailStore", func() (Store, Store) { s := MailStore{Store: base}; return s, s.Store }},
		{"OrdersStore", func() (Store, Store) { s := OrdersStore{Store: base}; return s, s.Store }},
		{"NudgesStore", func() (Store, Store) { s := NudgesStore{Store: base}; return s, s.Store }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			typed, embedded := tc.get()

			// The typed wrapper satisfies beads.Store via method promotion.
			var _ typedStore = typed

			// The embedded .Store is the exact value passed in (pointer
			// equality), proving the wrapper adds no layer at runtime.
			if embedded != Store(base) {
				t.Fatalf("%s embedded .Store = %p, want %p", tc.name, embedded, base)
			}

			// Method promotion routes through the embedded store: a write via
			// the wrapper is observable on the underlying store.
			created, err := typed.Create(Bead{Title: "round-trip"})
			if err != nil {
				t.Fatalf("%s.Create: %v", tc.name, err)
			}
			got, err := base.Get(created.ID)
			if err != nil {
				t.Fatalf("base.Get(%q): %v", created.ID, err)
			}
			if got.ID != created.ID {
				t.Fatalf("%s wrote to a different store: got %q want %q", tc.name, got.ID, created.ID)
			}
		})
	}
}

func TestClassStoresExposeGuardedAssignmentCapability(t *testing.T) {
	base := NewMemStore()
	cases := []struct {
		name  string
		store Store
	}{
		{"WorkStore", WorkStore{Store: base}},
		{"GraphStore", GraphStore{Store: base}},
		{"SessionStore", SessionStore{Store: base}},
		{"MailStore", MailStore{Store: base}},
		{"OrdersStore", OrdersStore{Store: base}},
		{"NudgesStore", NudgesStore{Store: base}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claimer, ok := GuardedAssignmentClaimerFor(tc.store)
			if !ok || claimer != base {
				t.Fatalf("GuardedAssignmentClaimerFor(%s) = (%T, %v), want embedded MemStore", tc.name, claimer, ok)
			}
		})
	}
}

func TestClassStoresExposeCreateAssignmentCapability(t *testing.T) {
	base := NewMemStore()
	cases := []struct {
		name  string
		store Store
	}{
		{"WorkStore", WorkStore{Store: base}},
		{"GraphStore", GraphStore{Store: base}},
		{"SessionStore", SessionStore{Store: base}},
		{"MailStore", MailStore{Store: base}},
		{"OrdersStore", OrdersStore{Store: base}},
		{"NudgesStore", NudgesStore{Store: base}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claimer, ok := CreateAssignmentClaimerFor(tc.store)
			if !ok || claimer != base {
				t.Fatalf("CreateAssignmentClaimerFor(%s) = (%T, %v), want embedded MemStore", tc.name, claimer, ok)
			}
		})
	}
}

func TestClassStoresExposeAssignmentReleaseCapability(t *testing.T) {
	base := NewMemStore()
	cases := []struct {
		name  string
		store Store
	}{
		{"WorkStore", WorkStore{Store: base}},
		{"GraphStore", GraphStore{Store: base}},
		{"SessionStore", SessionStore{Store: base}},
		{"MailStore", MailStore{Store: base}},
		{"OrdersStore", OrdersStore{Store: base}},
		{"NudgesStore", NudgesStore{Store: base}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			releaser, ok := AssignmentReleaserFor(tc.store)
			if !ok || releaser != base {
				t.Fatalf("AssignmentReleaserFor(%s) = (%T, %v), want embedded MemStore", tc.name, releaser, ok)
			}
		})
	}

	unsupportedBase := struct{ Store }{Store: base}
	unsupportedCases := []struct {
		name  string
		store Store
	}{
		{"WorkStore", WorkStore{Store: unsupportedBase}},
		{"GraphStore", GraphStore{Store: unsupportedBase}},
		{"SessionStore", SessionStore{Store: unsupportedBase}},
		{"MailStore", MailStore{Store: unsupportedBase}},
		{"OrdersStore", OrdersStore{Store: unsupportedBase}},
		{"NudgesStore", NudgesStore{Store: unsupportedBase}},
	}
	for _, tc := range unsupportedCases {
		t.Run(tc.name+"Unsupported", func(t *testing.T) {
			if releaser, ok := AssignmentReleaserFor(tc.store); ok || releaser != nil {
				t.Fatalf("AssignmentReleaserFor(%s(store-only)) = (%T, %v), want nil,false", tc.name, releaser, ok)
			}
		})
	}
}
