package bifrost

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

// Routing scenarios from the routing test plan, run in-process: real core (key selection, retries,
// fallbacks, session affinity, the caller-pin step) against one OpenAI-compatible upstream that
// answers by bearer token. A test plugin stands in for whatever picked the route (a routing rule, a
// virtual key's weighted pick), including the key a rule's target pins. Each test names the plan
// rows it covers.

const (
	provA schemas.ModelProvider = "prov-a"
	provB schemas.ModelProvider = "prov-b"
	provC schemas.ModelProvider = "prov-c"
)

// scenarioUpstream is the one upstream every provider of a scenario points at. It answers by the
// bearer token a request carries, so each key can be made to fail on its own, and it counts the
// requests by token and by token and model.
type scenarioUpstream struct {
	*httptest.Server
	mu        sync.Mutex
	status    map[string]int           // by "token" or "token|model"; 0 answers 200
	remaining map[string]int           // by the status's match: how many more requests answer it before 200
	replies   map[string]scenarioReply // by "token" or "token|model"; overrides status
	hits      map[string]int           // by "token" and by "token|model"
}

// scenarioReply is an answer spelled out in full, for the failure classes the default bodies do not
// produce: a status with its own body and headers, after an optional stall.
type scenarioReply struct {
	status int
	body   string
	header map[string]string
	stall  time.Duration
}

// newScenarioUpstream starts an upstream that serves every request until told otherwise.
func newScenarioUpstream(t *testing.T) *scenarioUpstream {
	t.Helper()
	u := &scenarioUpstream{status: map[string]int{}, remaining: map[string]int{}, replies: map[string]scenarioReply{}, hits: map[string]int{}}
	u.Server = httptest.NewServer(http.HandlerFunc(u.serve))
	t.Cleanup(u.Close)
	return u
}

// serve answers one chat completion: the status set for the token and model, else for the token,
// else 200, with an error body shaped like OpenAI's for the failure classes core reads.
func (u *scenarioUpstream) serve(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	var body struct {
		Model string `json:"model"`
	}
	raw, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(raw, &body)
	u.mu.Lock()
	u.hits[token]++
	u.hits[token+"|"+body.Model]++
	match := token + "|" + body.Model
	status, ok := u.status[match]
	if !ok {
		match = token
		status = u.status[token]
	}
	if left, limited := u.remaining[match]; limited && status != 0 {
		if left <= 1 {
			delete(u.status, match)
			delete(u.remaining, match)
		} else {
			u.remaining[match] = left - 1
		}
	}
	reply, replied := u.replies[token+"|"+body.Model]
	if !replied {
		reply, replied = u.replies[token]
	}
	u.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if replied {
		if reply.stall > 0 {
			select {
			case <-time.After(reply.stall):
			case <-r.Context().Done():
				return
			}
		}
		for name, value := range reply.header {
			w.Header().Set(name, value)
		}
		if reply.status == 0 || reply.status == http.StatusOK {
			fmt.Fprintf(w, `{"id":"c1","object":"chat.completion","created":1,"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, body.Model)
			return
		}
		w.WriteHeader(reply.status)
		_, _ = w.Write([]byte(reply.body))
		return
	}
	switch status {
	case 0, http.StatusOK:
		fmt.Fprintf(w, `{"id":"c1","object":"chat.completion","created":1,"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, body.Model)
	case http.StatusUnauthorized:
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":{"message":"Incorrect API key provided","type":"invalid_request_error","code":"invalid_api_key"}}`))
	case http.StatusTooManyRequests:
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":{"message":"Rate limit reached for requests","type":"requests","code":"rate_limit_exceeded"}}`))
	default:
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":{"message":"The server had an error while processing your request","type":"server_error"}}`))
	}
}

// answer sets the status the upstream answers for a token, or for "token|model".
func (u *scenarioUpstream) answer(match string, status int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.status[match] = status
	delete(u.remaining, match)
}

// answerFirst sets the status the upstream answers for a token, or for "token|model", for the next n
// requests only; after them it answers 200 again.
func (u *scenarioUpstream) answerFirst(match string, status, n int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.status[match] = status
	u.remaining[match] = n
}

// reply sets the full answer for a token, or for "token|model"; heal removes it.
func (u *scenarioUpstream) reply(match string, r scenarioReply) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.replies[match] = r
}

// heal makes a token, or "token|model", answer 200 again, whatever it was set to answer.
func (u *scenarioUpstream) heal(match string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	delete(u.replies, match)
	delete(u.status, match)
	delete(u.remaining, match)
}

// count returns how many requests reached the upstream with a token, or with "token|model".
func (u *scenarioUpstream) count(match string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.hits[match]
}

// clearHits forgets the counts, so a turn's hits can be read on their own.
func (u *scenarioUpstream) clearHits() {
	u.mu.Lock()
	defer u.mu.Unlock()
	clear(u.hits)
}

// scenarioKey is a key whose id and name are name and whose value, the upstream's token, is "sk-"+name.
func scenarioKey(name string, weight float64) schemas.Key {
	return schemas.Key{ID: name, Name: name, Value: *schemas.NewSecretVar("sk-" + name), Models: schemas.WhiteList{"*"}, Weight: weight}
}

// scenarioAccount registers each provider as an OpenAI-compatible custom provider at the upstream,
// with two retries and a near-zero backoff, and the given keys.
func scenarioAccount(u *scenarioUpstream, keys map[schemas.ModelProvider][]schemas.Key) *MockAccount {
	account := NewMockAccount()
	for provider, providerKeys := range keys {
		addScenarioProvider(account, u, provider, providerKeys)
	}
	return account
}

// addScenarioProvider registers one provider the way scenarioAccount does, so a test can also add a
// provider back after deleting it, as an operator would.
func addScenarioProvider(account *MockAccount, u *scenarioUpstream, provider schemas.ModelProvider, keys []schemas.Key) {
	account.AddProviderWithBaseURL(provider, 4, 256, u.URL)
	account.mu.Lock()
	network := &account.configs[provider].NetworkConfig
	network.MaxRetries = 2
	network.RetryBackoffInitial = time.Millisecond
	network.RetryBackoffMax = 2 * time.Millisecond
	account.mu.Unlock()
	account.SetCustomProviderConfig(provider, &schemas.CustomProviderConfig{BaseProviderType: schemas.OpenAI})
	account.SetKeysForProvider(provider, keys)
}

// scenarioRouter stands in for a routing rule or a virtual key's weighted pick: a request that names
// no provider gets the chain route returns, and the primary's KeyID is set as the rule's key pin. It
// also records the attempt trail each provider attempt leaves, since a fallback starts a trail of its
// own and the request's context keeps only the last one.
type scenarioRouter struct {
	mu       sync.Mutex
	route    func() (schemas.Fallback, []schemas.Fallback)
	attempts []scenarioAttempt
}

// scenarioAttempt is what one provider attempt of a request left behind: its attempt trail, one
// record per upstream call, and the number_of_retries it reported.
type scenarioAttempt struct {
	trail   []schemas.KeyAttemptRecord
	retries int
}

// awaitAttempts waits up to two seconds for n attempts to be recorded, since an attempt the caller
// stopped waiting for (a cancel, a deadline) reaches the post-hooks after the request returns, then
// returns them like takeAttempts.
func (r *scenarioRouter) awaitAttempts(t *testing.T, n int) []scenarioAttempt {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		r.mu.Lock()
		got := len(r.attempts)
		r.mu.Unlock()
		if got >= n {
			break
		}
	}
	attempts := r.takeAttempts()
	if len(attempts) < n {
		t.Fatalf("recorded %d provider attempts, want %d", len(attempts), n)
	}
	return attempts
}

// takeAttempts returns the attempts recorded since the last call and forgets them.
func (r *scenarioRouter) takeAttempts() []scenarioAttempt {
	r.mu.Lock()
	defer r.mu.Unlock()
	attempts := r.attempts
	r.attempts = nil
	return attempts
}

// fixedRoute always routes to primary, with the given fallbacks.
func fixedRoute(primary schemas.Fallback, fallbacks ...schemas.Fallback) func() (schemas.Fallback, []schemas.Fallback) {
	return func() (schemas.Fallback, []schemas.Fallback) { return primary, fallbacks }
}

// setRoute replaces the router's route, as an operator editing the rule would.
func (r *scenarioRouter) setRoute(route func() (schemas.Fallback, []schemas.Fallback)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.route = route
}

// GetName implements schemas.LLMPlugin.
func (r *scenarioRouter) GetName() string { return "scenario-router" }

// Cleanup implements schemas.LLMPlugin.
func (r *scenarioRouter) Cleanup() error { return nil }

// PreRequestHook routes a request that names no provider, as the routing plugin does.
func (r *scenarioRouter) PreRequestHook(ctx *schemas.BifrostContext, req *schemas.BifrostRequest) error {
	if provider, _, _ := req.GetRequestFields(); provider != "" {
		return nil
	}
	r.mu.Lock()
	route := r.route
	r.mu.Unlock()
	primary, fallbacks := route()
	req.SetProvider(primary.Provider)
	req.SetModel(primary.Model)
	req.SetFallbacks(fallbacks)
	if primary.KeyID != "" {
		ctx.SetValue(schemas.BifrostContextKeyRoutingPinnedAPIKeyID, primary.KeyID)
	}
	return nil
}

// PreLLMHook implements schemas.LLMPlugin.
func (r *scenarioRouter) PreLLMHook(_ *schemas.BifrostContext, req *schemas.BifrostRequest) (*schemas.BifrostRequest, *schemas.LLMPluginShortCircuit, error) {
	return req, nil, nil
}

// PostLLMHook records the attempt trail and retry count the provider attempt left on the context.
func (r *scenarioRouter) PostLLMHook(ctx *schemas.BifrostContext, resp *schemas.BifrostResponse, bifrostErr *schemas.BifrostError) (*schemas.BifrostResponse, *schemas.BifrostError, error) {
	trail, _ := ctx.Value(schemas.BifrostContextKeyAttemptTrail).([]schemas.KeyAttemptRecord)
	retries, _ := ctx.Value(schemas.BifrostContextKeyNumberOfRetries).(int)
	r.mu.Lock()
	r.attempts = append(r.attempts, scenarioAttempt{trail: slices.Clone(trail), retries: retries})
	r.mu.Unlock()
	return resp, bifrostErr, nil
}

// scenarioClient starts core over the account, with the router when given and session state in kv
// when given.
func scenarioClient(t *testing.T, account *MockAccount, router *scenarioRouter, kv schemas.KVStore) *Bifrost {
	t.Helper()
	config := schemas.BifrostConfig{Account: account, Logger: NewDefaultLogger(schemas.LogLevelError), KVStore: kv}
	if router != nil {
		config.LLMPlugins = []schemas.LLMPlugin{router}
	}
	client, err := Init(context.Background(), config)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(client.Shutdown)
	return client
}

// scenarioChat sends one non-streaming chat request and returns the provider that served it.
func scenarioChat(client *Bifrost, ctx *schemas.BifrostContext, provider schemas.ModelProvider, model string, fallbacks ...schemas.Fallback) (schemas.ModelProvider, *schemas.BifrostError) {
	resp, err := client.ChatCompletionRequest(ctx, scenarioRequest(provider, model, fallbacks))
	if err != nil {
		return "", err
	}
	return resp.ExtraFields.RoutingInfo.Provider, nil
}

// scenarioChatInfo sends one non-streaming chat request and returns the routing info of the attempt
// that answered it, the one that served or, when every attempt failed, the one whose error came back.
func scenarioChatInfo(client *Bifrost, ctx *schemas.BifrostContext, provider schemas.ModelProvider, model string, fallbacks ...schemas.Fallback) (schemas.RoutingInfo, *schemas.BifrostError) {
	resp, err := client.ChatCompletionRequest(ctx, scenarioRequest(provider, model, fallbacks))
	if err != nil {
		return err.ExtraFields.RoutingInfo, err
	}
	return resp.ExtraFields.RoutingInfo, nil
}

// scenarioRequest is the one-message chat request every scenario sends.
func scenarioRequest(provider schemas.ModelProvider, model string, fallbacks []schemas.Fallback) *schemas.BifrostChatRequest {
	return &schemas.BifrostChatRequest{
		Provider:  provider,
		Model:     model,
		Fallbacks: fallbacks,
		Input:     []schemas.ChatMessage{{Role: schemas.ChatMessageRoleUser, Content: &schemas.ChatMessageContent{ContentStr: schemas.Ptr("hi")}}},
	}
}

// requireHits fails the test unless each token or "token|model" reached the upstream as often as given.
func requireHits(t *testing.T, u *scenarioUpstream, want map[string]int) {
	t.Helper()
	for match, n := range want {
		if got := u.count(match); got != n {
			t.Errorf("%s reached the upstream %d times, want %d", match, got, n)
		}
	}
}

// requireServed fails the test when the request failed or was served by another provider.
func requireServed(t *testing.T, got schemas.ModelProvider, err *schemas.BifrostError, want schemas.ModelProvider) {
	t.Helper()
	if err != nil {
		t.Fatalf("request failed: %s", err.Error.Message)
	}
	if got != want {
		t.Fatalf("served by %s, want %s", got, want)
	}
}

// pinnedRule is the rule every explicit-key scenario uses: target A pinned to a1, fallback B pinned to b1.
func pinnedRule() func() (schemas.Fallback, []schemas.Fallback) {
	return fixedRoute(schemas.Fallback{Provider: provA, Model: "m", KeyID: "a1"}, schemas.Fallback{Provider: provB, Model: "m", KeyID: "b1"})
}

// pinnedRuleAccount gives A three keys and B two, all at the same weight, so only a pin explains a
// request landing on one of them every time.
func pinnedRuleAccount(u *scenarioUpstream) *MockAccount {
	return scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{
		provA: {scenarioKey("a1", 1), scenarioKey("a2", 1), scenarioKey("a3", 1)},
		provB: {scenarioKey("b1", 1), scenarioKey("b2", 1)},
	})
}

// A rule target pinned to a key never uses another key of its provider, whatever the pinned key
// answers; the request moves to the fallback, uses only the fallback's pinned key, and the fresh
// session binds to the fallback and its key. A transient error and a rate limit are retried on the
// pin, a rejected key is not (plan rows EK-02, EK-03, EK-04; one test, one subtest per row).
func TestScenarioPinnedTargetFailsOverToItsPinnedFallback(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		wantA1 int
	}{
		{name: "EK-02 transient 500", status: http.StatusInternalServerError, wantA1: 3},
		{name: "EK-03 rate limit 429", status: http.StatusTooManyRequests, wantA1: 3},
		{name: "EK-04 rejected key 401", status: http.StatusUnauthorized, wantA1: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := newScenarioUpstream(t)
			kv := newMockKVStore()
			client := scenarioClient(t, pinnedRuleAccount(u), &scenarioRouter{route: pinnedRule()}, kv)
			u.answer("sk-a1", tc.status)

			info, err := scenarioChatInfo(client, sessionCtx("s"), "", "m")
			requireServed(t, info.Provider, err, provB)
			if !info.IsFallback || info.PrimaryProvider == nil || *info.PrimaryProvider != provA || info.Key != "b1" {
				t.Fatalf("B should serve on b1 as the fallback of primary A, routing info %+v", info)
			}
			requireHits(t, u, map[string]int{"sk-a1": tc.wantA1, "sk-a2": 0, "sk-a3": 0, "sk-b1": 1, "sk-b2": 0})
			requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-b/m")
			requireSessionState(t, kv, "s", SessionStateKindKey, provB, "m", "b1")
		})
	}
}

// With a session, the request the pinned fallback rescued binds the session to B and b1, so the next
// ten turns start there, with B as the primary and A never tried; when b1 fails, the demoted A entry
// runs as a fallback on a1 alone, the session moves back, and the turn after it has A as the primary
// (plan rows EK-05, EK-06; EK-02's rescue sets them up).
func TestScenarioSessionFollowsThePinnedFallbackAndBack(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	client := scenarioClient(t, pinnedRuleAccount(u), &scenarioRouter{route: pinnedRule()}, kv)
	turn := func() (schemas.RoutingInfo, *schemas.BifrostError) {
		u.clearHits()
		return scenarioChatInfo(client, sessionCtx("s"), "", "m")
	}

	u.answer("sk-a1", http.StatusInternalServerError)
	info, err := turn()
	requireServed(t, info.Provider, err, provB)
	requireHits(t, u, map[string]int{"sk-a1": 3, "sk-b1": 1})

	// EK-05: a1 still fails; ten turns start on B with b1 as the primary, and A is not tried.
	u.clearHits()
	for i := range 10 {
		info, err = scenarioChatInfo(client, sessionCtx("s"), "", "m")
		requireServed(t, info.Provider, err, provB)
		if info.IsFallback || info.Key != "b1" {
			t.Fatalf("turn %d should be served by B on b1 as the primary, routing info %+v", i+2, info)
		}
	}
	requireHits(t, u, map[string]int{"sk-a1": 0, "sk-b1": 10, "sk-b2": 0})

	// EK-06: a1 heals and b1 fails. The demoted A entry runs as a fallback on a1 alone, and the session
	// moves its route and A's key binding to A and a1.
	u.answer("sk-a1", http.StatusOK)
	u.answer("sk-b1", http.StatusInternalServerError)
	info, err = turn()
	requireServed(t, info.Provider, err, provA)
	if !info.IsFallback || info.Key != "a1" {
		t.Fatalf("A should serve on a1 as a fallback, routing info %+v", info)
	}
	requireHits(t, u, map[string]int{"sk-b1": 3, "sk-a1": 1, "sk-a2": 0, "sk-a3": 0})
	requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-a/m")
	requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", "a1")

	// The next turn's chain is the rule's own, [A@a1, B@b1], so A is the primary.
	info, err = turn()
	requireServed(t, info.Provider, err, provA)
	if info.IsFallback || info.Key != "a1" {
		t.Fatalf("A should serve on a1 as the primary, routing info %+v", info)
	}
	requireHits(t, u, map[string]int{"sk-a1": 1, "sk-a2": 0, "sk-a3": 0, "sk-b1": 0})
}

// A session bound to B and b1 after a rescue, with both pinned keys failing: every attempt stays on
// its own pin, the turn returns the error of B, the provider the session put first, and the binding
// it followed is dropped; with both healed the next turn follows the rule to A and a1 (plan row EK-07).
func TestScenarioBothPinnedKeysFailUnderABoundSession(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	client := scenarioClient(t, pinnedRuleAccount(u), &scenarioRouter{route: pinnedRule()}, kv)
	u.answer("sk-a1", http.StatusInternalServerError)
	info, err := scenarioChatInfo(client, sessionCtx("s"), "", "m")
	requireServed(t, info.Provider, err, provB)
	requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-b/m")

	u.answer("sk-b1", http.StatusInternalServerError)
	u.clearHits()
	info, err = scenarioChatInfo(client, sessionCtx("s"), "", "m")
	if err == nil {
		t.Fatal("a turn whose every attempt failed was served")
	}
	if info.Provider != provB {
		t.Fatalf("the failed turn returned the error of %q, want B's, the head of the chain the session reordered", info.Provider)
	}
	requireHits(t, u, map[string]int{"sk-b1": 3, "sk-a1": 3, "sk-a2": 0, "sk-a3": 0, "sk-b2": 0})
	requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "")

	u.heal("sk-a1")
	u.heal("sk-b1")
	u.clearHits()
	info, err = scenarioChatInfo(client, sessionCtx("s"), "", "m")
	requireServed(t, info.Provider, err, provA)
	if info.Key != "a1" {
		t.Fatalf("the turn after should follow the rule to a1, routing info %+v", info)
	}
	requireHits(t, u, map[string]int{"sk-a1": 1, "sk-b1": 0})
}

// keyPinClient is a client over A{a1,a2,a3} at equal weights with a router whose target is A, pinned
// to pin when it is not empty, and a session store.
func keyPinClient(t *testing.T, pin string) (*scenarioUpstream, *mockKVStore, *scenarioRouter, *Bifrost) {
	t.Helper()
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	router := &scenarioRouter{route: fixedRoute(schemas.Fallback{Provider: provA, Model: "m", KeyID: pin})}
	client := scenarioClient(t, scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{
		provA: {scenarioKey("a1", 1), scenarioKey("a2", 1), scenarioKey("a3", 1)},
	}), router, kv)
	return u, kv, router, client
}

// keyPinTurns sends n turns with a fresh context from ctx each time, after forgetting the upstream's
// counts, and fails unless A serves every one.
func keyPinTurns(t *testing.T, u *scenarioUpstream, client *Bifrost, n int, ctx func() *schemas.BifrostContext) {
	t.Helper()
	u.clearHits()
	for range n {
		served, err := scenarioChat(client, ctx(), "", "m")
		requireServed(t, served, err, provA)
	}
}

// A rule's key pin outranks the session's key binding, and a pinned serve does not replace that
// binding: three pinned turns on a1 bind it, three turns after the pin moves to a2 use a2 with the
// binding still a1, and the turn after the pin is removed is back on a1 (plan row EK-12).
func TestScenarioKeyPinEditedUnderABoundSession(t *testing.T) {
	u, kv, router, client := keyPinClient(t, "a1")
	session := func() *schemas.BifrostContext { return sessionCtx("s") }

	keyPinTurns(t, u, client, 3, session)
	requireHits(t, u, map[string]int{"sk-a1": 3, "sk-a2": 0, "sk-a3": 0})
	requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", "a1")

	router.setRoute(fixedRoute(schemas.Fallback{Provider: provA, Model: "m", KeyID: "a2"}))
	keyPinTurns(t, u, client, 3, session)
	requireHits(t, u, map[string]int{"sk-a1": 0, "sk-a2": 3, "sk-a3": 0})
	requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", "a1")

	router.setRoute(fixedRoute(schemas.Fallback{Provider: provA, Model: "m"}))
	keyPinTurns(t, u, client, 1, session)
	requireHits(t, u, map[string]int{"sk-a1": 1, "sk-a2": 0, "sk-a3": 0})
}

// A pin removed from the target while a session is bound: the session keeps the key its pinned turns
// bound for all ten turns after the edit, and a2 and a3 get traffic only from new sessions (plan row
// EK-11; EK-01's pinned turns set it up).
func TestScenarioKeyPinRemovedUnderABoundSession(t *testing.T) {
	u, kv, router, client := keyPinClient(t, "a1")
	keyPinTurns(t, u, client, 3, func() *schemas.BifrostContext { return sessionCtx("s") })
	requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", "a1")

	router.setRoute(fixedRoute(schemas.Fallback{Provider: provA, Model: "m"}))
	keyPinTurns(t, u, client, 10, func() *schemas.BifrostContext { return sessionCtx("s") })
	requireHits(t, u, map[string]int{"sk-a1": 10, "sk-a2": 0, "sk-a3": 0})

	// Sixty new sessions with neither a2 nor a3 among them happens once in about ten billion runs.
	n := 0
	keyPinTurns(t, u, client, 60, func() *schemas.BifrostContext { n++; return sessionCtx(fmt.Sprintf("new-%d", n)) })
	if u.count("sk-a2") == 0 || u.count("sk-a3") == 0 {
		t.Fatalf("new sessions should rotate across a1 to a3: a1=%d a2=%d a3=%d", u.count("sk-a1"), u.count("sk-a2"), u.count("sk-a3"))
	}
}

// A weighted pick with the other provider as a fallback: the first turn of each session follows the
// weights, and every later turn stays where the first was served. Without fallbacks the pick is rolled
// on every turn, so one session sees both providers (plan rows GV-19, RR-30, RR-29; one subtest each).
func TestScenarioSessionsKeepTheirWeightedPick(t *testing.T) {
	a, b := schemas.Fallback{Provider: provA, Model: "m"}, schemas.Fallback{Provider: provB, Model: "m"}
	// roll picks A with probability shareA, else B.
	roll := func(shareA float64) schemas.Fallback {
		if rand.Float64() < shareA {
			return a
		}
		return b
	}
	// virtualKeyPick is a virtual key's weighted pick: the other provider rides as the fallback.
	virtualKeyPick := func(shareA float64) func() (schemas.Fallback, []schemas.Fallback) {
		return func() (schemas.Fallback, []schemas.Fallback) {
			if primary := roll(shareA); primary == a {
				return a, []schemas.Fallback{b}
			}
			return b, []schemas.Fallback{a}
		}
	}
	// rulePick is a rule's weighted targets with the rule's own fallback list.
	rulePick := func(shareA float64, fallbacks ...schemas.Fallback) func() (schemas.Fallback, []schemas.Fallback) {
		return func() (schemas.Fallback, []schemas.Fallback) { return roll(shareA), fallbacks }
	}
	keys := map[schemas.ModelProvider][]schemas.Key{provA: {scenarioKey("a1", 1)}, provB: {scenarioKey("b1", 1)}}

	t.Run("GV-19 with fallbacks the session keeps its first pick", func(t *testing.T) {
		u := newScenarioUpstream(t)
		client := scenarioClient(t, scenarioAccount(u, keys), &scenarioRouter{route: virtualKeyPick(0.7)}, newMockKVStore())
		const sessions = 500
		first := map[schemas.ModelProvider]int{}
		for i := range sessions {
			id := fmt.Sprintf("s-%d", i)
			var bound schemas.ModelProvider
			for turn := range 3 {
				served, err := scenarioChat(client, sessionCtx(id), "", "m")
				if err != nil {
					t.Fatalf("session %s turn %d failed: %s", id, turn, err.Error.Message)
				}
				if turn == 0 {
					bound = served
					first[served]++
				} else if served != bound {
					t.Fatalf("session %s moved from %s to %s on turn %d", id, bound, served, turn)
				}
			}
		}
		// Chi-square at p = 1e-6 on one degree of freedom.
		stat := 0.0
		for provider, share := range map[schemas.ModelProvider]float64{provA: 0.7, provB: 0.3} {
			expected := sessions * share
			diff := float64(first[provider]) - expected
			stat += diff * diff / expected
		}
		if stat >= 23.928 {
			t.Fatalf("first turns %v do not fit 70/30 (chi-square %.1f)", first, stat)
		}
	})

	t.Run("RR-30 targets with fallbacks keep the session on its first provider", func(t *testing.T) {
		u := newScenarioUpstream(t)
		client := scenarioClient(t, scenarioAccount(u, keys), &scenarioRouter{route: rulePick(0.5, a, b)}, newMockKVStore())
		var bound schemas.ModelProvider
		for turn := range 20 {
			ctx := sessionCtx("s")
			served, err := scenarioChat(client, ctx, "", "m")
			if err != nil {
				t.Fatalf("turn %d failed: %s", turn+1, err.Error.Message)
			}
			if turn == 0 {
				bound = served
				continue
			}
			if served != bound {
				t.Fatalf("turn %d was served by %s, but turn 1 bound %s", turn+1, served, bound)
			}
			if !strings.Contains(routingLogs(ctx), "Session stays on "+string(bound)+"/m") {
				t.Fatalf("turn %d should report the session keeping its binding first:\n%s", turn+1, routingLogs(ctx))
			}
		}
	})

	t.Run("RR-29 targets without fallbacks are rolled every turn", func(t *testing.T) {
		u := newScenarioUpstream(t)
		client := scenarioClient(t, scenarioAccount(u, keys), &scenarioRouter{route: rulePick(0.5)}, newMockKVStore())
		seen := map[schemas.ModelProvider]bool{}
		dropped := false
		// Twenty turns on one provider happens about twice in a million runs.
		for range 20 {
			ctx := sessionCtx("one")
			served, err := scenarioChat(client, ctx, "", "m")
			if err != nil {
				t.Fatalf("turn failed: %s", err.Error.Message)
			}
			seen[served] = true
			dropped = dropped || strings.Contains(routingLogs(ctx), "which this request cannot use")
		}
		if len(seen) != 2 {
			t.Fatalf("a one-entry chain cannot be held by a session, yet 20 turns only reached %v", seen)
		}
		if !dropped {
			t.Fatal("a turn whose roll differed from the binding should report dropping it")
		}
	})
}

// With a fixed configuration and a session, identical requests take identical routes: the same
// provider and the same key every time, and from turn 2 on the same routing trail (plan row IX-21).
func TestScenarioIdenticalRequestsTakeIdenticalRoutes(t *testing.T) {
	u := newScenarioUpstream(t)
	client := scenarioClient(t, scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{
		provA: {scenarioKey("a1", 1), scenarioKey("a2", 1), scenarioKey("a3", 1)},
		provB: {scenarioKey("b1", 1)},
	}), &scenarioRouter{route: fixedRoute(schemas.Fallback{Provider: provA, Model: "m"}, schemas.Fallback{Provider: provB, Model: "m"})}, newMockKVStore())
	var trail string
	for turn := range 100 {
		ctx := sessionCtx("s")
		served, err := scenarioChat(client, ctx, "", "m")
		requireServed(t, served, err, provA)
		switch logs := routingLogs(ctx); {
		case turn == 1:
			trail = logs
		case turn > 1 && logs != trail:
			t.Fatalf("turn %d's routing trail differs from turn 2's:\n%s\n--- turn 2 ---\n%s", turn+1, logs, trail)
		}
	}
	used := 0
	for _, token := range []string{"sk-a1", "sk-a2", "sk-a3"} {
		switch u.count(token) {
		case 0:
		case 100:
			used++
		default:
			t.Fatalf("%s served %d of 100 identical requests; one key should serve them all", token, u.count(token))
		}
	}
	if used != 1 {
		t.Fatalf("%d keys served the session, want exactly one", used)
	}
}

// A caller's key pin applies to the attempt on the provider the caller named and to nothing after
// it: a fallback to another model of the same provider picks a key of its own, and a pin naming
// another provider's key fails key selection on the named provider with no upstream call, and is not
// carried to that provider's fallback (plan rows KH-06, KH-07, KH-08; one subtest each, KH-07 in two parts). A bare model's pin is tried
// on the provider routing put first, which is never changed for it (KH-08).
func TestScenarioCallerPinStaysOffFallbacks(t *testing.T) {
	// The Key-ID header base: A{a1,a2,a3} and B{b1,b2}, every key at the same weight.
	keys := map[schemas.ModelProvider][]schemas.Key{
		provA: {scenarioKey("a1", 1), scenarioKey("a2", 1), scenarioKey("a3", 1)},
		provB: {scenarioKey("b1", 1), scenarioKey("b2", 1)},
	}
	t.Run("KH-06 a model fallback on the same provider picks a key of its own", func(t *testing.T) {
		u := newScenarioUpstream(t)
		client := scenarioClient(t, scenarioAccount(u, keys), nil, nil)
		u.answer("sk-a2|m", http.StatusInternalServerError)
		for range 10 {
			served, err := scenarioChat(client, pinCtx("", "a2"), provA, "m", schemas.Fallback{Provider: provA, Model: "m2"})
			requireServed(t, served, err, provA)
		}
		requireHits(t, u, map[string]int{"sk-a2|m": 30, "sk-a1|m": 0, "sk-a3|m": 0})
		if got := u.count("sk-a1|m2") + u.count("sk-a2|m2") + u.count("sk-a3|m2"); got != 10 {
			t.Fatalf("the m2 fallback ran %d times, want once per request", got)
		}
		// A carried pin would send all ten to a2; a free pick does so once in about 59,000 runs.
		if u.count("sk-a2|m2") == 10 {
			t.Fatal("every m2 fallback ran on a2, the caller's pin; the fallback should pick freely among a1 to a3")
		}
	})
	t.Run("KH-07 a pin on another provider's key, with no fallbacks, is refused on the named provider", func(t *testing.T) {
		u := newScenarioUpstream(t)
		client := scenarioClient(t, scenarioAccount(u, keys), nil, nil)
		_, err := scenarioChat(client, pinCtx("", "b1"), provA, "m")
		requireStatus(t, err, http.StatusBadRequest, "")
		if !strings.Contains(err.Error.Message, "no supported key found with id") {
			t.Fatalf("the refusal should say no key with the pinned id was found: %s", err.Error.Message)
		}
		requireHits(t, u, map[string]int{"sk-a1": 0, "sk-a2": 0, "sk-a3": 0, "sk-b1": 0, "sk-b2": 0})
	})
	t.Run("KH-07 a pin on another provider's key is not carried to that provider's fallback", func(t *testing.T) {
		u := newScenarioUpstream(t)
		client := scenarioClient(t, scenarioAccount(u, keys), nil, nil)
		for range 20 {
			served, err := scenarioChat(client, pinCtx("", "b1"), provA, "m", schemas.Fallback{Provider: provB, Model: "m"})
			requireServed(t, served, err, provB)
		}
		requireHits(t, u, map[string]int{"sk-a1": 0, "sk-a2": 0, "sk-a3": 0})
		// A carried pin would send all twenty to b1; a free pick does so about once in a million runs.
		if u.count("sk-b2") == 0 {
			t.Fatal("every B fallback ran on b1, the caller's pin; the fallback should pick freely")
		}
	})
	// A pin is tried on the first attempt only, on whatever provider routing put first: Bifrost never
	// moves the pinned key's provider forward. Routing chose A for a bare model with B as its fallback;
	// the caller's pin on b1 fails key selection on A with no upstream call, and B runs as a fallback,
	// without the pin.
	t.Run("KH-08 routing's choice of provider stands against a pin on another provider's key", func(t *testing.T) {
		u := newScenarioUpstream(t)
		router := &scenarioRouter{}
		router.setRoute(fixedRoute(schemas.Fallback{Provider: provA, Model: "m"}, schemas.Fallback{Provider: provB, Model: "m"}))
		client := scenarioClient(t, scenarioAccount(u, keys), router, nil)
		for range 20 {
			info, err := scenarioChatInfo(client, pinCtx("", "b1"), "", "m")
			if err != nil {
				t.Fatalf("request failed: %s", err.Error.Message)
			}
			if info.Provider != provB || !info.IsFallback {
				t.Fatalf("served by %s (fallback %v), want B as the fallback", info.Provider, info.IsFallback)
			}
		}
		requireHits(t, u, map[string]int{"sk-a1": 0, "sk-a2": 0, "sk-a3": 0})
		// A carried pin would send all twenty to b1; a free pick does so about once in a million runs.
		if u.count("sk-b2") == 0 {
			t.Fatal("every B fallback ran on b1, the caller's pin; the fallback should pick freely")
		}
	})
}

// directKeyAccount registers A{a1,a2} at uA and B{b1} at uB, the direct-key base, so what each
// provider received can be read on its own upstream.
func directKeyAccount(uA, uB *scenarioUpstream) *MockAccount {
	account := NewMockAccount()
	addScenarioProvider(account, uA, provA, []schemas.Key{scenarioKey("a1", 1), scenarioKey("a2", 1)})
	addScenarioProvider(account, uB, provB, []schemas.Key{scenarioKey("b1", 1)})
	return account
}

// A caller's direct key is used on the first provider of the chain, retries included, and on nothing
// after it: a fallback to another provider, or to another model of the same provider, runs on that
// provider's configured keys. With a bare model the first provider is whatever routing picks, so the
// caller's key can reach a provider it is not for (plan rows DK-01, DK-05, DK-06, DK-07; one subtest
// each).
func TestScenarioDirectKeyGoesOnlyToTheFirstProvider(t *testing.T) {
	directCtx := func() *schemas.BifrostContext {
		ctx := sessionCtx("")
		ctx.SetValue(schemas.BifrostContextKeyDirectKey, schemas.Key{ID: "header-provided", Name: "header-provided", Value: *schemas.NewSecretVar("sk-caller"), Weight: 1})
		return ctx
	}
	keys := map[schemas.ModelProvider][]schemas.Key{
		provA: {scenarioKey("a1", 1), scenarioKey("a2", 1)},
		provB: {scenarioKey("b1", 1)},
	}

	t.Run("DK-01 served on the caller's key", func(t *testing.T) {
		u := newScenarioUpstream(t)
		client := scenarioClient(t, scenarioAccount(u, keys), nil, nil)
		for range 5 {
			info, err := scenarioChatInfo(client, directCtx(), provA, "m")
			requireServed(t, info.Provider, err, provA)
			if info.Key != "header-provided" {
				t.Fatalf("routing info should name the caller's key, got %q", info.Key)
			}
		}
		requireHits(t, u, map[string]int{"sk-caller": 5, "sk-a1": 0, "sk-a2": 0})
	})
	t.Run("DK-05 a provider fallback runs on its own keys", func(t *testing.T) {
		uA, uB := newScenarioUpstream(t), newScenarioUpstream(t)
		client := scenarioClient(t, directKeyAccount(uA, uB), nil, nil)
		uA.answer("sk-caller", http.StatusInternalServerError)
		served, err := scenarioChat(client, directCtx(), provA, "m", schemas.Fallback{Provider: provB, Model: "m"})
		requireServed(t, served, err, provB)
		requireHits(t, uA, map[string]int{"sk-caller": 3, "sk-a1": 0, "sk-a2": 0})
		requireHits(t, uB, map[string]int{"sk-caller": 0, "sk-b1": 1})
	})
	t.Run("DK-06 a model fallback on the same provider runs on the pool", func(t *testing.T) {
		u := newScenarioUpstream(t)
		client := scenarioClient(t, scenarioAccount(u, keys), nil, nil)
		u.answer("sk-caller", http.StatusUnauthorized)
		served, err := scenarioChat(client, directCtx(), provA, "m", schemas.Fallback{Provider: provA, Model: "m2"})
		requireServed(t, served, err, provA)
		requireHits(t, u, map[string]int{"sk-caller|m": 1, "sk-caller|m2": 0})
		if u.count("sk-a1|m2")+u.count("sk-a2|m2") != 1 {
			t.Fatalf("the model fallback should run once on a pool key, a1=%d a2=%d", u.count("sk-a1|m2"), u.count("sk-a2|m2"))
		}
	})
	t.Run("DK-07 with a bare model the caller's key goes to whichever provider routing picks", func(t *testing.T) {
		uA, uB := newScenarioUpstream(t), newScenarioUpstream(t)
		// The caller's key works on A only; B refuses it.
		uB.answer("sk-caller", http.StatusUnauthorized)
		// vk1 A=1, B=1: either provider is picked, the other rides as its fallback.
		router := &scenarioRouter{route: func() (schemas.Fallback, []schemas.Fallback) {
			a, b := schemas.Fallback{Provider: provA, Model: "m"}, schemas.Fallback{Provider: provB, Model: "m"}
			if rand.IntN(2) == 0 {
				a, b = b, a
			}
			return a, []schemas.Fallback{b}
		}}
		client := scenarioClient(t, directKeyAccount(uA, uB), router, nil)
		const n = 40
		for range n {
			if _, err := scenarioChat(client, directCtx(), "", "m"); err != nil {
				t.Fatalf("request failed: %s", err.Error.Message)
			}
		}
		toB := uB.count("sk-caller")
		if got := uA.count("sk-caller") + toB; got != n {
			t.Fatalf("the caller's key reached a first provider %d times, want %d", got, n)
		}
		if toB == 0 {
			t.Fatal("routing never put B first in 40 requests; the caller's key should have reached it")
		}
		if got := uA.count("sk-a1") + uA.count("sk-a2"); got != toB {
			t.Fatalf("each request B refused should be served by A on a pool key: %d, want %d", got, toB)
		}
		requireHits(t, uB, map[string]int{"sk-b1": 0})
	})
}

// A session's key is used for every retry, so a rate limit does not rotate to a sibling; only the
// fallback provider moves the session, and A's key binding is dropped. The same request without a
// session rotates to a sibling on A (plan row IX-19).
func TestScenarioBoundKeyOnARateLimitDoesNotRotate(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	// a2 and a3 have weight 0: a free pick always lands on a1, a rotation on one of them.
	client := scenarioClient(t, scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{
		provA: {scenarioKey("a1", 1), scenarioKey("a2", 0), scenarioKey("a3", 0)},
		provB: {scenarioKey("b1", 1)},
	}), &scenarioRouter{route: fixedRoute(schemas.Fallback{Provider: provA, Model: "m"}, schemas.Fallback{Provider: provB, Model: "m"})}, kv)

	served, err := scenarioChat(client, sessionCtx("s"), "", "m")
	requireServed(t, served, err, provA)

	u.answer("sk-a1", http.StatusTooManyRequests)
	u.clearHits()
	served, err = scenarioChat(client, sessionCtx("s"), "", "m")
	requireServed(t, served, err, provB)
	requireHits(t, u, map[string]int{"sk-a1": 3, "sk-a2": 0, "sk-a3": 0, "sk-b1": 1})
	if entry := kv.data[SessionStateKey(sessionCtx("s"), SessionStateKindRoute, "", "m")]; entry.value != "prov-b/m" {
		t.Fatalf("the session should move to the fallback that served, got %+v", entry)
	}
	if _, bound := kv.data[SessionStateKey(sessionCtx("s"), SessionStateKindKey, string(provA), "m")]; bound {
		t.Fatal("the key binding the turn followed into a failure survived")
	}

	// Without a session the same rate limit rotates to a sibling on A.
	u.clearHits()
	served, err = scenarioChat(client, sessionCtx(""), "", "m")
	requireServed(t, served, err, provA)
	if u.count("sk-a2")+u.count("sk-a3") != 1 || u.count("sk-b1") != 0 {
		t.Fatalf("a request without a session should rotate on A: a2=%d a3=%d b1=%d", u.count("sk-a2"), u.count("sk-a3"), u.count("sk-b1"))
	}
}

// A key bound to the session that starts answering 429, with no fallbacks: the retry policy runs on
// the bound key, never its siblings, the turn returns the 429 and drops the key binding, and the next
// turn picks a key normally and binds the one that serves (plan row KS-04).
func TestScenarioBoundKeyRateLimitedWithNoFallbacks(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	client := scenarioClient(t, scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{
		provA: {scenarioKey("a1", 1), scenarioKey("a2", 1), scenarioKey("a3", 1)},
	}), nil, kv)
	bindSession(t, kv, "s", SessionStateKindKey, provA, "m", "a1")
	u.answer("sk-a1", http.StatusTooManyRequests)

	_, err := scenarioChat(client, sessionCtx("s"), provA, "m")
	requireStatus(t, err, http.StatusTooManyRequests, "")
	requireHits(t, u, map[string]int{"sk-a1": 3, "sk-a2": 0, "sk-a3": 0})
	requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", "")

	// a1 still answers 429; the normal pick rotates off it when it lands there, and binds what serves.
	u.clearHits()
	served, err := scenarioChat(client, sessionCtx("s"), provA, "m")
	requireServed(t, served, err, provA)
	bound, _ := sessionState(kv, "s", SessionStateKindKey, provA, "m")
	if bound == "" || bound == "a1" || u.count("sk-"+bound) != 1 {
		t.Fatalf("the session should bind the key that served, holds %q (a1=%d a2=%d a3=%d)", bound, u.count("sk-a1"), u.count("sk-a2"), u.count("sk-a3"))
	}
}

// Before a session has a key, a transient error is retried on the same key and never rotates: a key
// that recovers within the retries serves on the third attempt and binds, and one that does not fails
// the turn with nothing bound, though healthy siblings sit beside it (plan row KS-03; one subtest per
// case). a2 and a3 weigh 0 so that a1 is picked first, as the row sets up.
func TestScenarioTransientErrorStaysOnItsKey(t *testing.T) {
	keys := map[schemas.ModelProvider][]schemas.Key{provA: {scenarioKey("a1", 1), scenarioKey("a2", 0), scenarioKey("a3", 0)}}
	t.Run("(a) a1 recovers on the third attempt", func(t *testing.T) {
		u := newScenarioUpstream(t)
		kv := newMockKVStore()
		client := scenarioClient(t, scenarioAccount(u, keys), nil, kv)
		u.answerFirst("sk-a1", http.StatusInternalServerError, 2)
		served, err := scenarioChat(client, sessionCtx("s"), provA, "m")
		requireServed(t, served, err, provA)
		requireHits(t, u, map[string]int{"sk-a1": 3, "sk-a2": 0, "sk-a3": 0})
		requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", "a1")
	})
	t.Run("(b) a1 keeps failing", func(t *testing.T) {
		u := newScenarioUpstream(t)
		kv := newMockKVStore()
		client := scenarioClient(t, scenarioAccount(u, keys), nil, kv)
		u.answer("sk-a1", http.StatusInternalServerError)
		_, err := scenarioChat(client, sessionCtx("s"), provA, "m")
		requireStatus(t, err, http.StatusInternalServerError, "")
		requireHits(t, u, map[string]int{"sk-a1": 3, "sk-a2": 0, "sk-a3": 0})
		requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", "")
	})
}

// When several fallbacks fail before one serves, the session binds the one that served, at fallback
// index 2, and the next turn's chain starts there (plan row PS-04).
func TestScenarioSessionBindsTheFallbackThatServed(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	client := scenarioClient(t, scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{
		provA: {scenarioKey("a1", 1)}, provB: {scenarioKey("b1", 1)}, provC: {scenarioKey("c1", 1)},
	}), &scenarioRouter{route: fixedRoute(schemas.Fallback{Provider: provA, Model: "m"}, schemas.Fallback{Provider: provB, Model: "m"}, schemas.Fallback{Provider: provC, Model: "m"})}, kv)
	u.answer("sk-a1", http.StatusInternalServerError)
	u.answer("sk-b1", http.StatusInternalServerError)

	ctx := sessionCtx("s")
	served, err := scenarioChat(client, ctx, "", "m")
	requireServed(t, served, err, provC)
	if index, _ := ctx.Value(schemas.BifrostContextKeyFallbackIndex).(int); index != 2 {
		t.Fatalf("C should serve as fallback 2, served at index %d", index)
	}
	requireHits(t, u, map[string]int{"sk-a1": 3, "sk-b1": 3, "sk-c1": 1})
	requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-c/m")

	u.clearHits()
	served, err = scenarioChat(client, sessionCtx("s"), "", "m")
	requireServed(t, served, err, provC)
	requireHits(t, u, map[string]int{"sk-a1": 0, "sk-b1": 0, "sk-c1": 1})
}

// Editing the rule while a session is bound: the session keeps its provider while the edited rule
// still offers it, and rebinds when the rule no longer does (plan row DC-06).
func TestScenarioEditedRuleAndABoundSession(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	router := &scenarioRouter{route: fixedRoute(schemas.Fallback{Provider: provA, Model: "m"}, schemas.Fallback{Provider: provB, Model: "m"})}
	client := scenarioClient(t, scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{
		provA: {scenarioKey("a1", 1)}, provB: {scenarioKey("b1", 1)}, provC: {scenarioKey("c1", 1)},
	}), router, kv)
	served, err := scenarioChat(client, sessionCtx("s"), "", "m")
	requireServed(t, served, err, provA)

	// (a) The rule now targets B, with A as its fallback: for two turns the session stays on A, and the
	// binding is kept.
	router.setRoute(fixedRoute(schemas.Fallback{Provider: provB, Model: "m"}, schemas.Fallback{Provider: provA, Model: "m"}))
	for range 2 {
		served, err = scenarioChat(client, sessionCtx("s"), "", "m")
		requireServed(t, served, err, provA)
		requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-a/m")
	}

	// (b) The rule targets C with B as its fallback, so it no longer offers A: the binding is dropped,
	// C serves and rebinds it, and the second turn stays on C.
	router.setRoute(fixedRoute(schemas.Fallback{Provider: provC, Model: "m"}, schemas.Fallback{Provider: provB, Model: "m"}))
	for range 2 {
		served, err = scenarioChat(client, sessionCtx("s"), "", "m")
		requireServed(t, served, err, provC)
		requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-c/m")
	}
}
