package bifrost

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

// Provider-level session scenarios and live configuration changes from the routing test plan: what
// a session does when every provider fails, when its binding expires or is refreshed, when the bound
// provider leaves the chain or Bifrost, when the caller cancels or the provider times out, when a
// new session's first turns race, and when max_retries or the affinity switch change between turns.
// They run on the upstream, account and router of routingscenarios_test.go.

// clockKVStore is a session store whose entries expire on a clock the test moves, so a TTL can be
// crossed without waiting for it.
type clockKVStore struct {
	mu   sync.Mutex
	now  time.Time
	data map[string]clockKVEntry
}

// clockKVEntry is one stored value and the moment it expires.
type clockKVEntry struct {
	value   any
	expires time.Time
}

// newClockKVStore returns an empty store whose clock starts now.
func newClockKVStore() *clockKVStore {
	return &clockKVStore{now: time.Now(), data: map[string]clockKVEntry{}}
}

// advance moves the store's clock forward.
func (s *clockKVStore) advance(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = s.now.Add(d)
}

// live reports whether key holds an entry that has not expired. The caller holds mu.
func (s *clockKVStore) live(key string) (clockKVEntry, bool) {
	entry, ok := s.data[key]
	return entry, ok && s.now.Before(entry.expires)
}

// Get implements schemas.KVStore.
func (s *clockKVStore) Get(key string) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.live(key); ok {
		return entry.value, nil
	}
	return nil, errors.New("key not found")
}

// SetWithTTL implements schemas.KVStore.
func (s *clockKVStore) SetWithTTL(key string, value any, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = clockKVEntry{value: value, expires: s.now.Add(ttl)}
	return nil
}

// SetNXWithTTL implements schemas.KVStore: an expired entry counts as absent.
func (s *clockKVStore) SetNXWithTTL(key string, value any, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.live(key); ok {
		return false, nil
	}
	s.data[key] = clockKVEntry{value: value, expires: s.now.Add(ttl)}
	return true, nil
}

// Delete implements schemas.KVStore.
func (s *clockKVStore) Delete(key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.live(key)
	delete(s.data, key)
	return ok, nil
}

// deleteScenarioProvider deletes a provider from Bifrost as the gateway does: from the account that
// configures it and from the running client.
func deleteScenarioProvider(t *testing.T, client *Bifrost, account *MockAccount, provider schemas.ModelProvider) {
	t.Helper()
	account.mu.Lock()
	delete(account.configs, provider)
	delete(account.keys, provider)
	account.mu.Unlock()
	if err := client.RemoveProvider(provider); err != nil {
		t.Fatalf("removing %s: %v", provider, err)
	}
}

// chainOf routes every request to the providers in order, the first as the primary and the rest as
// its fallbacks, all on model m.
func chainOf(providers ...schemas.ModelProvider) func() (schemas.Fallback, []schemas.Fallback) {
	routes := make([]schemas.Fallback, len(providers))
	for i, provider := range providers {
		routes[i] = schemas.Fallback{Provider: provider, Model: "m"}
	}
	return fixedRoute(routes[0], routes[1:]...)
}

// shuffledChainOf routes every request to the providers in a random order, as a virtual key with
// equal weights does: one picked as the primary, the others attached as its fallbacks.
func shuffledChainOf(providers ...schemas.ModelProvider) func() (schemas.Fallback, []schemas.Fallback) {
	return func() (schemas.Fallback, []schemas.Fallback) {
		order := append([]schemas.ModelProvider(nil), providers...)
		rand.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
		return chainOf(order...)()
	}
}

// threeProviders is the keys every provider-level scenario uses: two keys on A, one on B and on C.
func threeProviders() map[schemas.ModelProvider][]schemas.Key {
	return map[schemas.ModelProvider][]schemas.Key{
		provA: {scenarioKey("a1", 1), scenarioKey("a2", 1)},
		provB: {scenarioKey("b1", 1)},
		provC: {scenarioKey("c1", 1)},
	}
}

// A fresh session whose first turn fails on every provider writes nothing, so the next turn follows
// the rule's order with no session at work in it (plan row PS-06).
func TestScenarioEveryProviderFailsBeforeABinding(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	client := scenarioClient(t, scenarioAccount(u, threeProviders()), &scenarioRouter{route: chainOf(provA, provB)}, kv)
	for _, token := range []string{"sk-a1", "sk-a2", "sk-b1"} {
		u.answer(token, http.StatusInternalServerError)
	}
	if _, err := scenarioChat(client, sessionCtx("s"), "", "m"); err == nil {
		t.Fatal("a turn whose every provider failed was served")
	}
	if n := len(kv.data); n != 0 {
		t.Fatalf("a failed first turn wrote %d session entries", n)
	}

	for _, token := range []string{"sk-a1", "sk-a2", "sk-b1"} {
		u.heal(token)
	}
	ctx := sessionCtx("s")
	served, err := scenarioChat(client, ctx, "", "m")
	requireServed(t, served, err, provA)
	if logs := routingLogs(ctx); strings.Contains(logs, schemas.RoutingEngineSessionAffinity+":") {
		t.Fatalf("the session had nothing to say on the turn after a failed first turn:\n%s", logs)
	}
}

// A ten-second binding: (a) a session idle for fifteen seconds has lost it, so its next turn follows
// routing with no "Session stays" line; (b) a session turned every five seconds for forty never lets
// go of it, since each served turn refreshes the TTL (plan row PS-07; one session per case, on one
// clock). Routing is fixed rather than rolled, so following routing is told apart from the binding.
func TestScenarioSessionTTLExpiresAndRefreshes(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newClockKVStore()
	router := &scenarioRouter{route: chainOf(provA, provB, provC)}
	client := scenarioClient(t, scenarioAccount(u, threeProviders()), router, kv)
	turn := func(session string) (*schemas.BifrostContext, schemas.ModelProvider, *schemas.BifrostError) {
		ctx := sessionCtx(session)
		ctx.SetValue(schemas.BifrostContextKeySessionTTL, 10*time.Second)
		served, err := scenarioChat(client, ctx, "", "m")
		return ctx, served, err
	}
	for _, session := range []string{"idle", "busy"} {
		_, served, err := turn(session)
		requireServed(t, served, err, provA)
	}
	// From here routing puts B first, so only a live binding keeps a session on A.
	router.setRoute(chainOf(provB, provA, provC))

	for step := 1; step <= 8; step++ {
		kv.advance(5 * time.Second)
		_, served, err := turn("busy")
		requireServed(t, served, err, provA)
		if step == 3 {
			// (a) Fifteen seconds after its only turn, the idle session's binding has expired.
			ctx, served, err := turn("idle")
			requireServed(t, served, err, provB)
			if logs := routingLogs(ctx); strings.Contains(logs, "Session stays") {
				t.Fatalf("an expired binding still moved the turn:\n%s", logs)
			}
		}
	}
}

// A bound provider that routing no longer offers, because it was removed from the virtual key, is
// dropped on the next turn with a trail line saying so; whatever serves becomes the binding and the
// turn after it stays there (plan row PS-09).
func TestScenarioBoundProviderLeavesTheChain(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	router := &scenarioRouter{route: chainOf(provA, provB, provC)}
	client := scenarioClient(t, scenarioAccount(u, threeProviders()), router, kv)
	served, err := scenarioChat(client, sessionCtx("s"), "", "m")
	requireServed(t, served, err, provA)

	router.setRoute(shuffledChainOf(provB, provC))
	ctx := sessionCtx("s")
	first, err := scenarioChat(client, ctx, "", "m")
	if err != nil || first == provA {
		t.Fatalf("the turn after A left the chain was served by %q (err %v)", first, err)
	}
	if !strings.Contains(routingLogs(ctx), "which this request cannot use") {
		t.Fatalf("dropping the binding should be on the trail:\n%s", routingLogs(ctx))
	}
	requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", string(first)+"/m")
	for range 5 {
		served, err := scenarioChat(client, sessionCtx("s"), "", "m")
		requireServed(t, served, err, first)
	}
}

// A bound provider deleted from Bifrost causes no error: when routing stops offering it the binding
// is dropped and rebound, and when routing still offers it (a stale chain) the attempt on it fails,
// a fallback serves, and the session moves there (plan row PS-10).
func TestScenarioBoundProviderDeleted(t *testing.T) {
	t.Run("routing stops offering it", func(t *testing.T) {
		u := newScenarioUpstream(t)
		kv := newMockKVStore()
		account := scenarioAccount(u, threeProviders())
		router := &scenarioRouter{route: chainOf(provA, provB, provC)}
		client := scenarioClient(t, account, router, kv)
		served, err := scenarioChat(client, sessionCtx("s"), "", "m")
		requireServed(t, served, err, provA)

		deleteScenarioProvider(t, client, account, provA)
		router.setRoute(shuffledChainOf(provB, provC))
		for turn := range 2 {
			ctx := sessionCtx("s")
			served, err = scenarioChat(client, ctx, "", "m")
			if err != nil || served == provA {
				t.Fatalf("served by %q after A was deleted (err %v)", served, err)
			}
			if turn == 0 && !strings.Contains(routingLogs(ctx), "which this request cannot use") {
				t.Fatalf("dropping the binding to the deleted provider should be on the trail:\n%s", routingLogs(ctx))
			}
		}
		requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", string(served)+"/m")
	})
	t.Run("a stale chain still offers it", func(t *testing.T) {
		u := newScenarioUpstream(t)
		kv := newMockKVStore()
		account := scenarioAccount(u, threeProviders())
		router := &scenarioRouter{route: chainOf(provA, provB, provC)}
		client := scenarioClient(t, account, router, kv)
		served, err := scenarioChat(client, sessionCtx("s"), "", "m")
		requireServed(t, served, err, provA)

		deleteScenarioProvider(t, client, account, provA)
		router.setRoute(chainOf(provB, provA, provC))
		served, err = scenarioChat(client, sessionCtx("s"), "", "m")
		requireServed(t, served, err, provB)
		requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-b/m")
	})
}

// A caller that abandons a turn after a second says nothing about the provider, so the binding stays
// and the next turn is back on it; a provider that times out has failed the session, so the timeout
// is not retried, the fallback serves at once, and the binding moves to it (plan row PS-17; one
// subtest per case).
func TestScenarioCancelKeepsTheBindingATimeoutMovesIt(t *testing.T) {
	t.Run("(a) caller cancel", func(t *testing.T) {
		u := newScenarioUpstream(t)
		kv := newMockKVStore()
		client := scenarioClient(t, scenarioAccount(u, threeProviders()), &scenarioRouter{route: chainOf(provA, provB)}, kv)
		served, err := scenarioChat(client, sessionCtx("s"), "", "m")
		requireServed(t, served, err, provA)
		bound, _ := sessionState(kv, "s", SessionStateKindKey, provA, "m")

		u.reply("sk-"+bound, scenarioReply{stall: 3 * time.Second})
		parent, cancel := context.WithCancel(context.Background())
		ctx := schemas.NewBifrostContext(parent, schemas.NoDeadline)
		ctx.SetValue(schemas.BifrostContextKeySessionID, "s")
		time.AfterFunc(time.Second, cancel)
		_, err = scenarioChat(client, ctx, "", "m")
		requireStatus(t, err, 499, schemas.RequestCancelled)
		requireHits(t, u, map[string]int{"sk-b1": 0})
		requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-a/m")
		requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", bound)

		u.heal("sk-" + bound)
		u.clearHits()
		served, err = scenarioChat(client, sessionCtx("s"), "", "m")
		requireServed(t, served, err, provA)
		requireHits(t, u, map[string]int{"sk-" + bound: 1})
	})
	t.Run("(b) provider timeout", func(t *testing.T) {
		u := newScenarioUpstream(t)
		kv := newMockKVStore()
		account := scenarioAccount(u, threeProviders())
		account.configs[provA].NetworkConfig.DefaultRequestTimeoutInSeconds = 1
		client := scenarioClient(t, account, &scenarioRouter{route: chainOf(provA, provB)}, kv)
		served, err := scenarioChat(client, sessionCtx("s"), "", "m")
		requireServed(t, served, err, provA)
		bound, _ := sessionState(kv, "s", SessionStateKindKey, provA, "m")

		u.reply("sk-"+bound, scenarioReply{stall: 3 * time.Second})
		u.clearHits()
		served, err = scenarioChat(client, sessionCtx("s"), "", "m")
		requireServed(t, served, err, provB)
		requireHits(t, u, map[string]int{"sk-" + bound: 1, "sk-b1": 1})
		requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-b/m")
		requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", "")
	})
}

// Ten first turns of a new session in flight together may land on different providers; the first to
// be served binds the session, the others do not overwrite it, and every later turn follows that one
// binding. Each provider answers after a different delay, so the first to be served is the fastest
// provider the burst reached (plan row PS-18).
func TestScenarioParallelFirstTurnsConverge(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	client := scenarioClient(t, scenarioAccount(u, threeProviders()), &scenarioRouter{route: shuffledChainOf(provA, provB, provC)}, kv)
	stall := map[schemas.ModelProvider]time.Duration{provA: 20 * time.Millisecond, provB: 150 * time.Millisecond, provC: 280 * time.Millisecond}
	for token, provider := range map[string]schemas.ModelProvider{"sk-a1": provA, "sk-a2": provA, "sk-b1": provB, "sk-c1": provC} {
		u.reply(token, scenarioReply{stall: stall[provider]})
	}
	var wg sync.WaitGroup
	burst := make([]schemas.ModelProvider, 10)
	errs := make([]*schemas.BifrostError, 10)
	for i := range burst {
		wg.Go(func() { burst[i], errs[i] = scenarioChat(client, sessionCtx("burst"), "", "m") })
	}
	wg.Wait()
	first := provC
	for i, served := range burst {
		if errs[i] != nil {
			t.Fatalf("burst request %d failed: %s", i, errs[i].Error.Message)
		}
		if stall[served] < stall[first] {
			first = served
		}
	}
	requireSessionState(t, kv, "burst", SessionStateKindRoute, "", "m", string(first)+"/m")
	for range 10 {
		served, err := scenarioChat(client, sessionCtx("burst"), "", "m")
		requireServed(t, served, err, first)
	}
}

// A max_retries change reaches the next request: three attempts on a failing key before the change,
// one after (plan row DC-13).
func TestScenarioMaxRetriesChangedLive(t *testing.T) {
	u := newScenarioUpstream(t)
	account := scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{provA: {scenarioKey("a1", 1)}})
	client := scenarioClient(t, account, nil, nil)
	u.answer("sk-a1", http.StatusInternalServerError)

	_, err := scenarioChat(client, sessionCtx(""), provA, "m")
	requireStatus(t, err, 500, "")
	requireHits(t, u, map[string]int{"sk-a1": 3})

	setMaxRetries(account, provA, 0)
	if err := client.UpdateProvider(provA); err != nil {
		t.Fatalf("UpdateProvider: %v", err)
	}
	u.clearHits()
	_, err = scenarioChat(client, sessionCtx(""), provA, "m")
	requireStatus(t, err, 500, "")
	requireHits(t, u, map[string]int{"sk-a1": 1})
}

// A provider deleted and added back under the same name: the session returns to it if no turn ran in
// between, since its binding was never dropped; once a turn ran without it, the binding moved to what
// served, and adding it back does not move the session (plan row DC-15).
func TestScenarioDeletedProviderAddedBack(t *testing.T) {
	setup := func(t *testing.T) (*scenarioUpstream, *mockKVStore, *MockAccount, *scenarioRouter, *Bifrost) {
		u := newScenarioUpstream(t)
		kv := newMockKVStore()
		account := scenarioAccount(u, threeProviders())
		router := &scenarioRouter{route: chainOf(provA, provB)}
		client := scenarioClient(t, account, router, kv)
		served, err := scenarioChat(client, sessionCtx("s"), "", "m")
		requireServed(t, served, err, provA)
		deleteScenarioProvider(t, client, account, provA)
		return u, kv, account, router, client
	}
	t.Run("added back before any turn", func(t *testing.T) {
		u, _, account, router, client := setup(t)
		addScenarioProvider(account, u, provA, threeProviders()[provA])
		router.setRoute(chainOf(provB, provA))
		served, err := scenarioChat(client, sessionCtx("s"), "", "m")
		requireServed(t, served, err, provA)
	})
	t.Run("added back after a turn ran without it", func(t *testing.T) {
		u, kv, account, router, client := setup(t)
		router.setRoute(chainOf(provB))
		served, err := scenarioChat(client, sessionCtx("s"), "", "m")
		requireServed(t, served, err, provB)
		requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-b/m")

		addScenarioProvider(account, u, provA, threeProviders()[provA])
		router.setRoute(chainOf(provA, provB))
		served, err = scenarioChat(client, sessionCtx("s"), "", "m")
		requireServed(t, served, err, provB)
	})
}

// Turns with affinity switched off are routed as if they had no session and never touch its
// bindings, so the turns with it on keep following the binding (plan row DC-17).
func TestScenarioAffinityOffTurnsLeaveTheSession(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	router := &scenarioRouter{route: chainOf(provA, provB, provC)}
	client := scenarioClient(t, scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{
		provA: {scenarioKey("a1", 1), scenarioKey("a2", 1)},
		provB: {scenarioKey("b1", 1), scenarioKey("b2", 1)},
		provC: {scenarioKey("c1", 1)},
	}), router, kv)
	turn := func(session string, on bool) (schemas.ModelProvider, *schemas.BifrostError) {
		ctx := sessionCtx(session)
		if !on {
			ctx.SetValue(schemas.BifrostContextKeySessionAffinity, false)
		}
		return scenarioChat(client, ctx, "", "m")
	}
	served, err := turn("s", true)
	requireServed(t, served, err, provA)
	aKey, _ := sessionState(kv, "s", SessionStateKindKey, provA, "m")
	// From here routing puts B first, so only the binding keeps a turn on A.
	router.setRoute(chainOf(provB, provA, provC))

	for i := range 10 {
		on := i%2 == 0
		served, err := turn("s", on)
		want := provB
		if on {
			want = provA
		}
		requireServed(t, served, err, want)
		requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-a/m")
		requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", aKey)
		requireSessionState(t, kv, "s", SessionStateKindKey, provB, "m", "")
	}

	// A session whose every turn has affinity off is never bound at all.
	for range 3 {
		served, err := turn("off", false)
		requireServed(t, served, err, provB)
	}
	requireSessionState(t, kv, "off", SessionStateKindRoute, "", "m", "")
	requireSessionState(t, kv, "off", SessionStateKindKey, provB, "m", "")
}
