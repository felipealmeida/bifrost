package bifrost

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

// Key-level routing scenarios from the routing test plan: pinned keys (a rule's pin, the caller's
// x-bf-api-key-id, a direct key), key stickiness within a provider, and what each failure class does
// to the retry and fallback loops. They run on the upstream, account and router of
// routingscenarios_test.go. Each test names the plan rows it covers.

// failureReplies are OpenAI-shaped answers for the failure classes the upstream's default bodies do
// not produce, keyed by the plan's name for each.
var failureReplies = map[string]scenarioReply{
	"402 quota":        {status: http.StatusPaymentRequired, body: `{"error":{"message":"You exceeded your current quota, please check your plan and billing details.","type":"insufficient_quota","code":"insufficient_quota"}}`},
	"429 quota":        {status: http.StatusTooManyRequests, body: `{"error":{"message":"You exceeded your current quota, please check your plan and billing details.","type":"insufficient_quota","code":"insufficient_quota"}}`},
	"404 model":        {status: http.StatusNotFound, body: "{\"error\":{\"message\":\"The model `m` does not exist or you do not have access to it.\",\"type\":\"invalid_request_error\",\"code\":\"model_not_found\"}}"},
	"410 retired":      {status: http.StatusGone, body: `{"error":{"message":"The model has been deprecated and is no longer available.","type":"invalid_request_error","code":"model_deprecated"}}`},
	"403 region":       {status: http.StatusForbidden, body: `{"error":{"message":"Country, region, or territory not supported","type":"request_forbidden","code":"unsupported_country_region_territory"}}`},
	"400 caller fault": {status: http.StatusBadRequest, body: `{"error":{"message":"Invalid value for 'temperature': must be at most 2.","type":"invalid_request_error","param":"temperature","code":"invalid_value"}}`},
}

// setMaxRetries sets a provider's max_retries in the account, as an operator editing its network
// config would; a running client picks the change up on UpdateProvider.
func setMaxRetries(account *MockAccount, provider schemas.ModelProvider, n int) {
	account.mu.Lock()
	defer account.mu.Unlock()
	account.configs[provider].NetworkConfig.MaxRetries = n
}

// sessionState reads one of a session's bindings: its route for model (provider "") or its key on
// provider for model.
func sessionState(kv schemas.KVStore, session, kind string, provider schemas.ModelProvider, model string) (string, bool) {
	return SessionStateString(kv, SessionStateKey(sessionCtx(session), kind, string(provider), model))
}

// bindSession writes one of a session's bindings directly, as earlier turns would have left it.
func bindSession(t *testing.T, kv schemas.KVStore, session, kind string, provider schemas.ModelProvider, model, value string) {
	t.Helper()
	if err := kv.SetWithTTL(SessionStateKey(sessionCtx(session), kind, string(provider), model), value, time.Hour); err != nil {
		t.Fatalf("binding the session: %v", err)
	}
}

// requireSessionState fails the test unless the binding holds want, or is absent when want is "".
func requireSessionState(t *testing.T, kv schemas.KVStore, session, kind string, provider schemas.ModelProvider, model, want string) {
	t.Helper()
	got, found := sessionState(kv, session, kind, provider, model)
	if want == "" && found {
		t.Fatalf("the session's %s binding for %s/%s should be gone, holds %q", kind, provider, model, got)
	}
	if want != "" && got != want {
		t.Fatalf("the session's %s binding for %s/%s holds %q, want %q", kind, provider, model, got, want)
	}
}

// pinCtx is a context for session (none when empty) whose caller pins keyID with x-bf-api-key-id.
func pinCtx(session, keyID string) *schemas.BifrostContext {
	ctx := sessionCtx(session)
	ctx.SetValue(schemas.BifrostContextKeyAPIKeyID, keyID)
	return ctx
}

// directKeyCtx is a context for session (none when empty) that carries the caller's own key, whose
// value the upstream sees as the token "sk-caller".
func directKeyCtx(session string) *schemas.BifrostContext {
	ctx := sessionCtx(session)
	ctx.SetValue(schemas.BifrostContextKeyDirectKey, schemas.Key{ID: "header-provided", Name: "header-provided", Value: *schemas.NewSecretVar("sk-caller"), Weight: 1})
	return ctx
}

// routingLogs joins the routing engine log messages a request left on ctx, for substring checks.
func routingLogs(ctx *schemas.BifrostContext) string {
	var lines []string
	for _, entry := range ctx.GetRoutingEngineLogs() {
		lines = append(lines, entry.Engine+": "+entry.Message)
	}
	return strings.Join(lines, "\n")
}

// trailKeys lists the key each upstream call of an attempt trail went to.
func trailKeys(trail []schemas.KeyAttemptRecord) []string {
	keys := make([]string, len(trail))
	for i, record := range trail {
		keys[i] = record.KeyID
	}
	return keys
}

// requireClass fails the test unless every record of the trail carries class.
func requireClass(t *testing.T, trail []schemas.KeyAttemptRecord, class schemas.FailureClass) {
	t.Helper()
	for i, record := range trail {
		if record.FailureClass != class {
			t.Fatalf("attempt %d on %s was classed %q, want %q (trail %+v)", i, record.KeyID, record.FailureClass, class, trail)
		}
	}
}

// requireStatus fails the test unless the request failed with status, and, when errType is given,
// with that error type.
func requireStatus(t *testing.T, err *schemas.BifrostError, status int, errType string) {
	t.Helper()
	if err == nil {
		t.Fatalf("the request was served, want a %d", status)
	}
	if err.StatusCode == nil || *err.StatusCode != status {
		t.Fatalf("the request failed with status %v (%s), want %d", err.StatusCode, err.GetErrorString(), status)
	}
	if errType != "" && (err.Error == nil || err.Error.Type == nil || *err.Error.Type != errType) {
		t.Fatalf("the request failed with type %v, want %q", err.Error.Type, errType)
	}
}

// A rule target pinned to one key sends every request to that key, with healthy siblings beside it
// (plan row RR-07).
func TestScenarioRulePinnedKeyIsTheOnlyKeyUsed(t *testing.T) {
	u := newScenarioUpstream(t)
	client := scenarioClient(t, pinnedRuleAccount(u), &scenarioRouter{route: fixedRoute(schemas.Fallback{Provider: provA, Model: "m", KeyID: "a2"})}, nil)
	for range 30 {
		served, err := scenarioChat(client, sessionCtx(""), "", "m")
		requireServed(t, served, err, provA)
	}
	requireHits(t, u, map[string]int{"sk-a1": 0, "sk-a2": 30, "sk-a3": 0})
}

// A pinned key, whether a rule's target pinned it or the caller sent x-bf-api-key-id, has nothing
// behind it: a transient error and a rate limit are retried on the pin and returned, and a rejected
// key is tried once. Within the retry budget that rejection becomes 502 upstream_credentials_exhausted;
// with no retries it is the provider's own 401 (plan rows RR-08, RR-09, RR-10 for the rule's pin and
// KH-02, KH-05, KH-04 for the caller's; one subtest per row and case, named after the row).
func TestScenarioPinnedKeyFailuresStayOnThePin(t *testing.T) {
	type pinSource struct {
		name  string
		route func() (schemas.Fallback, []schemas.Fallback)
		ctx   func() *schemas.BifrostContext
		named schemas.ModelProvider
		rows  []string // the plan row of each case below, in order
	}
	sources := []pinSource{
		{name: "rule pin", route: fixedRoute(schemas.Fallback{Provider: provA, Model: "m", KeyID: "a2"}), ctx: func() *schemas.BifrostContext { return sessionCtx("") }, rows: []string{"RR-08", "RR-09", "RR-10", "RR-10"}},
		{name: "caller pin", ctx: func() *schemas.BifrostContext { return pinCtx("", "a2") }, named: provA, rows: []string{"KH-02", "KH-05", "KH-04", "KH-04"}},
	}
	for _, source := range sources {
		for i, tc := range []struct {
			name       string
			reply      scenarioReply
			maxRetries int
			hits       int
			status     int
			errType    string
			class      schemas.FailureClass
		}{
			{name: "transient 500", reply: scenarioReply{status: http.StatusInternalServerError, body: `{"error":{"message":"The server had an error","type":"server_error"}}`}, maxRetries: 2, hits: 3, status: 500, class: schemas.FailureClassTransient},
			{name: "rate limit 429", reply: scenarioReply{status: http.StatusTooManyRequests, body: `{"error":{"message":"Rate limit reached for requests","type":"requests","code":"rate_limit_exceeded"}}`, header: map[string]string{"Retry-After": "7"}}, maxRetries: 2, hits: 3, status: 429, class: schemas.FailureClassRateLimit},
			{name: "rejected 401 within the budget", reply: scenarioReply{status: http.StatusUnauthorized, body: `{"error":{"message":"Incorrect API key provided","type":"invalid_request_error","code":"invalid_api_key"}}`}, maxRetries: 2, hits: 1, status: 502, errType: "upstream_credentials_exhausted", class: schemas.FailureClassCredential},
			{name: "rejected 401 with no retries", reply: scenarioReply{status: http.StatusUnauthorized, body: `{"error":{"message":"Incorrect API key provided","type":"invalid_request_error","code":"invalid_api_key"}}`}, maxRetries: 0, hits: 1, status: 401, class: schemas.FailureClassCredential},
		} {
			t.Run(source.rows[i]+" "+source.name+"/"+tc.name, func(t *testing.T) {
				u := newScenarioUpstream(t)
				account := pinnedRuleAccount(u)
				setMaxRetries(account, provA, tc.maxRetries)
				router := &scenarioRouter{route: source.route}
				client := scenarioClient(t, account, router, nil)
				u.reply("sk-a2", tc.reply)

				_, err := scenarioChat(client, source.ctx(), source.named, "m")
				requireStatus(t, err, tc.status, tc.errType)
				requireHits(t, u, map[string]int{"sk-a1": 0, "sk-a2": tc.hits, "sk-a3": 0, "sk-b1": 0, "sk-b2": 0})
				attempts := router.takeAttempts()
				if len(attempts) != 1 {
					t.Fatalf("recorded %d provider attempts, want 1", len(attempts))
				}
				if keys := trailKeys(attempts[0].trail); len(keys) != tc.hits || strings.Count(strings.Join(keys, ","), "a2") != tc.hits {
					t.Fatalf("attempt trail %v, want a2 ×%d", keys, tc.hits)
				}
				requireClass(t, attempts[0].trail, tc.class)
				if tc.reply.header != nil {
					if err.ExtraFields.RetryAfter != 7000 {
						t.Fatalf("the 429 came back with retry hint %dms, want the provider's 7000ms", err.ExtraFields.RetryAfter)
					}
				}
			})
		}
	}
}

// A rule's key pin takes the primary attempt over from the caller's x-bf-api-key-id, and with a rule
// pin present the caller's pin does not hold the chain still: a session the rule's fallback served
// starts there, whatever key the caller pins (plan row EK-09).
func TestScenarioRulePinBeatsTheCallersKeyPin(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	client := scenarioClient(t, pinnedRuleAccount(u), &scenarioRouter{route: pinnedRule()}, kv)
	for range 5 {
		served, err := scenarioChat(client, pinCtx("s", "a2"), "", "m")
		requireServed(t, served, err, provA)
	}
	requireHits(t, u, map[string]int{"sk-a1": 5, "sk-a2": 0, "sk-a3": 0})

	// a1 fails once: the rule's fallback serves on its own pin, and the session moves to it.
	u.answer("sk-a1", http.StatusInternalServerError)
	u.clearHits()
	served, err := scenarioChat(client, pinCtx("s", "a2"), "", "m")
	requireServed(t, served, err, provB)
	requireHits(t, u, map[string]int{"sk-a1": 3, "sk-a2": 0, "sk-b1": 1})
	requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-b/m")

	// a1 is healthy again, and the caller still pins a2, a key of the rule's target. The session
	// still starts on B: only a caller pin with no rule pin beside it keeps the target first.
	u.heal("sk-a1")
	u.clearHits()
	served, err = scenarioChat(client, pinCtx("s", "a2"), "", "m")
	requireServed(t, served, err, provB)
	requireHits(t, u, map[string]int{"sk-a1": 0, "sk-a2": 0, "sk-b1": 1})
}

// A request on the caller's own key stays on that key whatever it answers: a transient error and a
// rate limit are retried on it and returned, and the pool is never touched. Sent with
// x-bf-api-key-id as well, the direct key still wins (plan rows DK-03, DK-04, DK-13).
func TestScenarioDirectKeyFailuresStayOnTheCallersKey(t *testing.T) {
	keys := map[schemas.ModelProvider][]schemas.Key{provA: {scenarioKey("a1", 1), scenarioKey("a2", 1)}}
	t.Run("transient 500", func(t *testing.T) {
		u := newScenarioUpstream(t)
		client := scenarioClient(t, scenarioAccount(u, keys), nil, nil)
		u.answer("sk-caller", http.StatusInternalServerError)
		_, err := scenarioChat(client, directKeyCtx(""), provA, "m")
		requireStatus(t, err, 500, "")
		requireHits(t, u, map[string]int{"sk-caller": 3, "sk-a1": 0, "sk-a2": 0})
	})
	t.Run("rate limit 429", func(t *testing.T) {
		u := newScenarioUpstream(t)
		client := scenarioClient(t, scenarioAccount(u, keys), nil, nil)
		u.answer("sk-caller", http.StatusTooManyRequests)
		_, err := scenarioChat(client, directKeyCtx(""), provA, "m")
		requireStatus(t, err, 429, "")
		requireHits(t, u, map[string]int{"sk-caller": 3, "sk-a1": 0, "sk-a2": 0})
	})
	t.Run("with x-bf-api-key-id", func(t *testing.T) {
		u := newScenarioUpstream(t)
		client := scenarioClient(t, scenarioAccount(u, keys), nil, nil)
		ctx := directKeyCtx("")
		ctx.SetValue(schemas.BifrostContextKeyAPIKeyID, "a2")
		served, err := scenarioChat(client, ctx, provA, "m")
		requireServed(t, served, err, provA)
		requireHits(t, u, map[string]int{"sk-caller": 1, "sk-a1": 0, "sk-a2": 0})
	})
}

// A direct-key turn neither reads nor writes the session's key binding on its provider: the turn
// after it is back on the bound key, though a free pick would now land elsewhere (plan row DK-10).
func TestScenarioDirectKeyLeavesTheSessionsKeyBinding(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	account := scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{provA: {scenarioKey("a1", 0), scenarioKey("a2", 1)}})
	client := scenarioClient(t, account, nil, kv)
	served, err := scenarioChat(client, sessionCtx("s"), provA, "m")
	requireServed(t, served, err, provA)
	requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", "a2")

	// From here a free pick lands on a1; only the binding keeps the session on a2.
	account.SetKeysForProvider(provA, []schemas.Key{scenarioKey("a1", 1), scenarioKey("a2", 0)})
	u.clearHits()
	ctx := directKeyCtx("s")
	served, err = scenarioChat(client, ctx, provA, "m")
	requireServed(t, served, err, provA)
	requireHits(t, u, map[string]int{"sk-caller": 1, "sk-a1": 0, "sk-a2": 0})
	if logs := routingLogs(ctx); strings.Contains(logs, "Session reused key") || strings.Contains(logs, "no longer eligible") {
		t.Fatalf("the direct-key turn consulted the session's key binding:\n%s", logs)
	}
	requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", "a2")

	served, err = scenarioChat(client, sessionCtx("s"), provA, "m")
	requireServed(t, served, err, provA)
	requireHits(t, u, map[string]int{"sk-a1": 0, "sk-a2": 1})
}

// A bare turn served on the caller's key by the primary binds the session's route but no key, so the
// next turn without a direct key starts on the same provider and picks a pool key normally, with no
// stale key binding to drop (plan row DK-08).
func TestScenarioDirectKeyServedTurnBindsTheRouteOnly(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	client := scenarioClient(t, scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{
		provA: {scenarioKey("a1", 1), scenarioKey("a2", 1)},
		provB: {scenarioKey("b1", 1), scenarioKey("b2", 1)},
	}), &scenarioRouter{route: func() (schemas.Fallback, []schemas.Fallback) {
		a, b := schemas.Fallback{Provider: provA, Model: "m"}, schemas.Fallback{Provider: provB, Model: "m"}
		if rand.IntN(2) == 0 {
			a, b = b, a
		}
		return a, []schemas.Fallback{b}
	}}, kv)
	for i := range 10 {
		session := fmt.Sprintf("s-%d", i)
		first, err := scenarioChat(client, directKeyCtx(session), "", "m")
		if err != nil {
			t.Fatalf("direct-key turn failed: %s", err.Error.Message)
		}
		requireSessionState(t, kv, session, SessionStateKindRoute, "", "m", string(first)+"/m")
		requireSessionState(t, kv, session, SessionStateKindKey, first, "m", "")

		ctx := sessionCtx(session)
		served, err := scenarioChat(client, ctx, "", "m")
		requireServed(t, served, err, first)
		if logs := routingLogs(ctx); strings.Contains(logs, "no longer eligible") {
			t.Fatalf("the turn after the direct-key turn found a stale key binding:\n%s", logs)
		}
		if bound, found := sessionState(kv, session, SessionStateKindKey, first, "m"); !found || bound == "header-provided" {
			t.Fatalf("the pool turn should bind a pool key, got %q", bound)
		}
	}
}

// Each failure class through the real retry and fallback loops, on a provider with two keys and a
// fallback: a transient error stays on its key, a rate limit rotates within the budget, a permanent
// per-key refusal walks the pool with one granted attempt per key, a caller fault and a provider
// timeout are not retried, and all of them move to the fallback. A caller's cancel and a request
// deadline end the request without a fallback (plan row LB-27).
func TestScenarioFailureClassLadder(t *testing.T) {
	type outcome struct {
		trail    int // upstream calls on A
		sameKey  bool
		bothKeys bool
		class    schemas.FailureClass
		fallback bool
	}
	cases := []struct {
		name     string
		reply    scenarioReply
		deadURL  bool
		timeout  bool
		cancel   bool
		deadline bool
		want     outcome
	}{
		{name: "500", reply: scenarioReply{status: 500, body: `{"error":{"message":"The server had an error","type":"server_error"}}`}, want: outcome{trail: 3, sameKey: true, class: schemas.FailureClassTransient, fallback: true}},
		{name: "502", reply: scenarioReply{status: 502, body: `{"error":{"message":"Bad gateway","type":"server_error"}}`}, want: outcome{trail: 3, sameKey: true, class: schemas.FailureClassTransient, fallback: true}},
		{name: "connection refused", deadURL: true, want: outcome{trail: 3, sameKey: true, class: schemas.FailureClassTransient, fallback: true}},
		{name: "429", reply: scenarioReply{status: 429, body: `{"error":{"message":"Rate limit reached for requests","type":"requests","code":"rate_limit_exceeded"}}`}, want: outcome{trail: 3, bothKeys: true, class: schemas.FailureClassRateLimit, fallback: true}},
		{name: "401", reply: scenarioReply{status: 401, body: `{"error":{"message":"Incorrect API key provided","type":"invalid_request_error","code":"invalid_api_key"}}`}, want: outcome{trail: 2, bothKeys: true, class: schemas.FailureClassCredential, fallback: true}},
		{name: "402", reply: failureReplies["402 quota"], want: outcome{trail: 2, bothKeys: true, class: schemas.FailureClassQuota, fallback: true}},
		{name: "404 model", reply: failureReplies["404 model"], want: outcome{trail: 2, bothKeys: true, class: schemas.FailureClassModelAccess, fallback: true}},
		{name: "410", reply: failureReplies["410 retired"], want: outcome{trail: 2, bothKeys: true, class: schemas.FailureClassModelGone, fallback: true}},
		{name: "403 region", reply: failureReplies["403 region"], want: outcome{trail: 2, bothKeys: true, class: schemas.FailureClassRegionBlocked, fallback: true}},
		{name: "400", reply: failureReplies["400 caller fault"], want: outcome{trail: 1, class: schemas.FailureClassCallerFault, fallback: true}},
		// The three below end the attempt on Bifrost's side, before the provider answers. The retry loop
		// stops on them, and labels the attempt: a provider timeout and a request deadline are timeouts,
		// a caller's cancel is cancelled (see TestScenarioTimedOutAttemptCarriesItsClass for the fields).
		{name: "provider timeout", reply: scenarioReply{stall: 3 * time.Second}, timeout: true, want: outcome{trail: 1, class: schemas.FailureClassTimeout, fallback: true}},
		{name: "caller cancel", reply: scenarioReply{stall: 3 * time.Second}, cancel: true, want: outcome{trail: 1, class: schemas.FailureClassCancelled}},
		{name: "request deadline", reply: scenarioReply{stall: 3 * time.Second}, deadline: true, want: outcome{trail: 1, class: schemas.FailureClassTimeout}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := newScenarioUpstream(t)
			account := scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{
				provA: {scenarioKey("a1", 1), scenarioKey("a2", 1)},
				provB: {scenarioKey("b1", 1)},
			})
			if tc.deadURL {
				account.configs[provA].NetworkConfig.BaseURL = "http://127.0.0.1:1"
			}
			if tc.timeout {
				account.configs[provA].NetworkConfig.DefaultRequestTimeoutInSeconds = 1
			}
			router := &scenarioRouter{}
			client := scenarioClient(t, account, router, nil)
			u.reply("sk-a1", tc.reply)
			u.reply("sk-a2", tc.reply)

			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			deadline := schemas.NoDeadline
			if tc.deadline {
				deadline = time.Now().Add(300 * time.Millisecond)
			}
			ctx := schemas.NewBifrostContext(parent, deadline)
			if tc.cancel {
				time.AfterFunc(300*time.Millisecond, cancel)
			}
			served, err := scenarioChat(client, ctx, provA, "m", schemas.Fallback{Provider: provB, Model: "m"})

			attempts := router.awaitAttempts(t, 1)
			trail := attempts[0].trail
			if len(trail) != tc.want.trail {
				t.Fatalf("A's attempt trail has %d calls %v, want %d", len(trail), trailKeys(trail), tc.want.trail)
			}
			if attempts[0].retries != tc.want.trail-1 {
				t.Fatalf("A reported number_of_retries %d for %d upstream calls", attempts[0].retries, len(trail))
			}
			keys := trailKeys(trail)
			if tc.want.sameKey && strings.Count(strings.Join(keys, ","), keys[0]) != len(keys) {
				t.Fatalf("a transient failure should stay on its key, trail %v", keys)
			}
			if tc.want.bothKeys && (!strings.Contains(strings.Join(keys, ","), "a1") || !strings.Contains(strings.Join(keys, ","), "a2")) {
				t.Fatalf("a per-key failure should move to the other key, trail %v", keys)
			}
			if tc.want.class != "" {
				requireClass(t, trail, tc.want.class)
			}
			if !tc.deadURL {
				if got := u.count("sk-a1") + u.count("sk-a2"); got != tc.want.trail {
					t.Fatalf("A's keys reached the upstream %d times, want %d", got, tc.want.trail)
				}
			}
			if tc.want.fallback {
				requireServed(t, served, err, provB)
				requireHits(t, u, map[string]int{"sk-b1": 1})
				return
			}
			if err == nil {
				t.Fatal("a cancelled or expired request was served")
			}
			requireHits(t, u, map[string]int{"sk-b1": 0})
			if tc.cancel {
				requireStatus(t, err, 499, schemas.RequestCancelled)
			}
			if tc.deadline {
				requireStatus(t, err, http.StatusGatewayTimeout, schemas.RequestTimedOut)
			}
		})
	}
}

// A session's key refused for good, by any permanent per-key class, is tried once; the attempt is
// granted back, a sibling serves, and the session rebinds to it (plan row KS-06).
func TestScenarioSessionKeyRefusedForGoodRebinds(t *testing.T) {
	for _, name := range []string{"404 model", "429 quota", "410 retired", "403 region"} {
		t.Run(name, func(t *testing.T) {
			u := newScenarioUpstream(t)
			kv := newMockKVStore()
			router := &scenarioRouter{}
			client := scenarioClient(t, pinnedRuleAccount(u), router, kv)
			bindSession(t, kv, "s", SessionStateKindKey, provA, "m", "a1")
			u.reply("sk-a1", failureReplies[name])

			served, err := scenarioChat(client, sessionCtx("s"), provA, "m")
			requireServed(t, served, err, provA)
			keys := trailKeys(router.takeAttempts()[0].trail)
			if len(keys) != 2 || keys[0] != "a1" || keys[1] == "a1" {
				t.Fatalf("attempt trail %v, want a1 once and then a sibling", keys)
			}
			requireHits(t, u, map[string]int{"sk-a1": 1, "sk-" + keys[1]: 1})
			requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", keys[1])
		})
	}
}

// Before a session has a key, a rate limit on the first pick rotates to another key within the same
// request, and the session binds to the key that served, never the one that refused (plan row KS-02).
func TestScenarioRateLimitBeforeABindingRotates(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	router := &scenarioRouter{}
	client := scenarioClient(t, pinnedRuleAccount(u), router, kv)
	u.answer("sk-a1", http.StatusTooManyRequests)
	rotated := false
	for i := 0; i < 200 && !rotated; i++ {
		session := fmt.Sprintf("s-%d", i)
		served, err := scenarioChat(client, sessionCtx(session), provA, "m")
		requireServed(t, served, err, provA)
		trail := router.takeAttempts()[0].trail
		bound, _ := sessionState(kv, session, SessionStateKindKey, provA, "m")
		if bound == "a1" || bound != trail[len(trail)-1].KeyID {
			t.Fatalf("the session bound %q; the key that served was %s", bound, trail[len(trail)-1].KeyID)
		}
		if trail[0].KeyID != "a1" {
			continue
		}
		rotated = true
		if len(trail) != 2 || trail[0].FailureClass != schemas.FailureClassRateLimit || !trail[0].TriggeredRotation || trail[1].KeyID == "a1" {
			t.Fatalf("a rate limit on the first pick should rotate once, trail %+v", trail)
		}
	}
	if !rotated {
		t.Fatal("no request's first pick landed on a1 in 200 tries")
	}
}

// A session's key that stops serving the model is dropped, and the sibling that serves becomes the
// binding (plan row KS-11).
func TestScenarioBoundKeyThatLeavesTheModelIsReplaced(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	account := pinnedRuleAccount(u)
	client := scenarioClient(t, account, nil, kv)
	bindSession(t, kv, "s", SessionStateKindKey, provA, "m", "a1")
	a1 := scenarioKey("a1", 1)
	a1.Models = schemas.WhiteList{"m2"}
	account.SetKeysForProvider(provA, []schemas.Key{a1, scenarioKey("a2", 1), scenarioKey("a3", 1)})

	ctx := sessionCtx("s")
	served, err := scenarioChat(client, ctx, provA, "m")
	requireServed(t, served, err, provA)
	if !strings.Contains(routingLogs(ctx), "is no longer eligible") {
		t.Fatalf("the stale key binding should be reported:\n%s", routingLogs(ctx))
	}
	bound, _ := sessionState(kv, "s", SessionStateKindKey, provA, "m")
	if bound != "a2" && bound != "a3" {
		t.Fatalf("the session should rebind to the sibling that served, holds %q", bound)
	}
	requireHits(t, u, map[string]int{"sk-a1": 0, "sk-" + bound: 1})
}

// Keys a4 and a5 added to a provider after a session bound a1 leave the session on a1 for ten turns,
// while new sessions pick among all of them (plan row KS-12).
func TestScenarioKeysAddedDuringASession(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	account := pinnedRuleAccount(u)
	client := scenarioClient(t, account, nil, kv)
	bindSession(t, kv, "s", SessionStateKindKey, provA, "m", "a1")
	bound := "a1"

	account.SetKeysForProvider(provA, []schemas.Key{scenarioKey("a1", 1), scenarioKey("a2", 1), scenarioKey("a3", 1), scenarioKey("a4", 1), scenarioKey("a5", 1)})
	u.clearHits()
	for range 10 {
		served, err := scenarioChat(client, sessionCtx("s"), provA, "m")
		requireServed(t, served, err, provA)
	}
	requireHits(t, u, map[string]int{"sk-" + bound: 10})
	requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", bound)

	// Thirty new sessions all missing the two new keys happens once in about five million runs.
	u.clearHits()
	for i := range 30 {
		served, err := scenarioChat(client, sessionCtx(fmt.Sprintf("new-%d", i)), provA, "m")
		requireServed(t, served, err, provA)
	}
	if u.count("sk-a4")+u.count("sk-a5") == 0 {
		t.Fatal("no new session used a key added during the first session")
	}
}

// A provider with one key has nothing to choose, so the session's key level is not consulted on its
// turns. Those turns still write a key binding to the one key, once (set if absent), and a later turn
// on the single key does not consult it, so it is never refreshed. Once a second key is added, the
// session stays on that first key, as KS-12 has a session stay on its key when keys are added (plan
// row KS-13).
func TestScenarioSingleKeyProviderSkipsTheKeyLevel(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	account := scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{provC: {scenarioKey("c1", 1)}})
	client := scenarioClient(t, account, nil, kv)
	for range 3 {
		ctx := sessionCtx("s")
		served, err := scenarioChat(client, ctx, provC, "m")
		requireServed(t, served, err, provC)
		if logs := routingLogs(ctx); strings.Contains(logs, "Session reused key") || strings.Contains(logs, "no longer eligible") {
			t.Fatalf("a single-key provider consulted the session's key binding:\n%s", logs)
		}
	}

	if bound, found := sessionState(kv, "s", SessionStateKindKey, provC, "m"); !found || bound != "c1" {
		t.Fatalf("the single-key turns should bind the session to c1, got %q (found %v)", bound, found)
	}

	account.SetKeysForProvider(provC, []schemas.Key{scenarioKey("c1", 1), scenarioKey("c2", 1)})
	u.clearHits()
	for range 5 {
		served, err := scenarioChat(client, sessionCtx("s"), provC, "m")
		requireServed(t, served, err, provC)
	}
	if bound, found := sessionState(kv, "s", SessionStateKindKey, provC, "m"); !found || bound != "c1" {
		t.Fatalf("after c2 is added the session should still hold c1, got %q (found %v)", bound, found)
	}
	// A free pick would leave c1 for c2 at least once in five turns 31 times in 32.
	requireHits(t, u, map[string]int{"sk-c1": 5, "sk-c2": 0})
}

// A fallback picks its key without the session, so the key binding the session already held on the
// fallback's provider is replaced by the key that served there, and the binding the request followed
// on the failed primary is dropped (plan row KS-15).
func TestScenarioFallbackReplacesTheSessionsKeyOnItsProvider(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	// b2 has weight 0, so a free pick on B lands on b1.
	client := scenarioClient(t, scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{
		provA: {scenarioKey("a1", 1), scenarioKey("a2", 1)},
		provB: {scenarioKey("b1", 1), scenarioKey("b2", 0)},
	}), &scenarioRouter{route: fixedRoute(schemas.Fallback{Provider: provA, Model: "m"}, schemas.Fallback{Provider: provB, Model: "m"})}, kv)
	bindSession(t, kv, "s", SessionStateKindRoute, "", "m", "prov-a/m")
	bindSession(t, kv, "s", SessionStateKindKey, provA, "m", "a1")
	bindSession(t, kv, "s", SessionStateKindKey, provB, "m", "b2")
	u.answer("sk-a1", http.StatusInternalServerError)

	served, err := scenarioChat(client, sessionCtx("s"), "", "m")
	requireServed(t, served, err, provB)
	requireHits(t, u, map[string]int{"sk-a1": 3, "sk-a2": 0, "sk-b1": 1, "sk-b2": 0})
	requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-b/m")
	requireSessionState(t, kv, "s", SessionStateKindKey, provB, "m", "b1")
	requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", "")
}

// Both levels of stickiness through a failover and back: the session follows the fallback that
// served, keeps the key it served on, and returns to the first provider on a fresh key when the
// fallback fails in turn (plan row KS-16).
func TestScenarioStickinessThroughAFailoverAndBack(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	weighted := func() (schemas.Fallback, []schemas.Fallback) {
		a, b := schemas.Fallback{Provider: provA, Model: "m"}, schemas.Fallback{Provider: provB, Model: "m"}
		if rand.Float64() >= 1/1.5 {
			a, b = b, a
		}
		return a, []schemas.Fallback{b}
	}
	client := scenarioClient(t, pinnedRuleAccount(u), &scenarioRouter{route: weighted}, kv)
	bindSession(t, kv, "s", SessionStateKindRoute, "", "m", "prov-a/m")
	bindSession(t, kv, "s", SessionStateKindKey, provA, "m", "a2")
	turn := func() (schemas.ModelProvider, *schemas.BifrostError) {
		u.clearHits()
		return scenarioChat(client, sessionCtx("s"), "", "m")
	}

	served, err := turn()
	requireServed(t, served, err, provA)
	requireHits(t, u, map[string]int{"sk-a2": 1})

	// A=500: every key of A fails; the session's a2 is retried and never rotated.
	for _, token := range []string{"sk-a1", "sk-a2", "sk-a3"} {
		u.answer(token, http.StatusInternalServerError)
	}
	served, err = turn()
	requireServed(t, served, err, provB)
	requireHits(t, u, map[string]int{"sk-a2": 3, "sk-a1": 0, "sk-a3": 0})
	requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-b/m")
	requireSessionState(t, kv, "s", SessionStateKindKey, provA, "m", "")
	bKey, _ := sessionState(kv, "s", SessionStateKindKey, provB, "m")
	if bKey == "" || u.count("sk-"+bKey) != 1 {
		t.Fatalf("B's key binding %q should be the key that served", bKey)
	}

	served, err = turn()
	requireServed(t, served, err, provB)
	requireHits(t, u, map[string]int{"sk-" + bKey: 1, "sk-a2": 0})

	// Heal A and set B=500: every key of B fails; the session's B key is retried, then A serves.
	for _, token := range []string{"sk-a1", "sk-a2", "sk-a3"} {
		u.heal(token)
	}
	for _, token := range []string{"sk-b1", "sk-b2"} {
		u.answer(token, http.StatusInternalServerError)
	}
	served, err = turn()
	requireServed(t, served, err, provA)
	otherB := "b1"
	if bKey == "b1" {
		otherB = "b2"
	}
	requireHits(t, u, map[string]int{"sk-" + bKey: 3, "sk-" + otherB: 0})
	requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-a/m")
	aKey, _ := sessionState(kv, "s", SessionStateKindKey, provA, "m")
	if aKey == "" || u.count("sk-"+aKey) != 1 {
		t.Fatalf("A's key binding %q should be the key that served", aKey)
	}
}

// A rule target pinned to a key whose fallback is unpinned: the fallback serves on a key it picks
// freely, and both bindings follow it, so later turns start on the fallback and reuse that key
// (plan row EK-08).
func TestScenarioUnpinnedRuleFallbackKeepsItsKey(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	client := scenarioClient(t, pinnedRuleAccount(u), &scenarioRouter{route: fixedRoute(schemas.Fallback{Provider: provA, Model: "m", KeyID: "a1"}, schemas.Fallback{Provider: provB, Model: "m"})}, kv)
	u.answer("sk-a1", http.StatusInternalServerError)

	served, err := scenarioChat(client, sessionCtx("s"), "", "m")
	requireServed(t, served, err, provB)
	requireHits(t, u, map[string]int{"sk-a1": 3, "sk-a2": 0, "sk-a3": 0})
	requireSessionState(t, kv, "s", SessionStateKindRoute, "", "m", "prov-b/m")
	bKey, _ := sessionState(kv, "s", SessionStateKindKey, provB, "m")
	if u.count("sk-"+bKey) != 1 {
		t.Fatalf("B's key binding %q should be the key that served", bKey)
	}

	for range 2 {
		u.clearHits()
		ctx := sessionCtx("s")
		served, err = scenarioChat(client, ctx, "", "m")
		requireServed(t, served, err, provB)
		requireHits(t, u, map[string]int{"sk-a1": 0, "sk-" + bKey: 1})
		if !strings.Contains(routingLogs(ctx), "Session reused key "+bKey+" for prov-b/m") {
			t.Fatalf("the turn should reuse the session's key on B:\n%s", routingLogs(ctx))
		}
	}
}

// A session keeps one key binding per provider and model, so alternating two models of one provider
// keeps each on its own key (plan row KS-17).
func TestScenarioKeyBindingsArePerModel(t *testing.T) {
	u := newScenarioUpstream(t)
	kv := newMockKVStore()
	client := scenarioClient(t, pinnedRuleAccount(u), nil, kv)
	for range 6 {
		for _, model := range []string{"m", "m2"} {
			served, err := scenarioChat(client, sessionCtx("s"), provA, model)
			requireServed(t, served, err, provA)
		}
	}
	for _, model := range []string{"m", "m2"} {
		bound, found := sessionState(kv, "s", SessionStateKindKey, provA, model)
		if !found {
			t.Fatalf("no key binding for prov-a/%s", model)
		}
		requireHits(t, u, map[string]int{"sk-" + bound + "|" + model: 6})
	}
}

// An attempt that ends on Bifrost's side leaves its class on the attempt trail like every other
// attempt (plan row LB-27: "the attempt trail shows failure_class per attempt"). A provider timeout
// and a request deadline are timeouts, 504, with fail_reason request_timed_out. A caller's cancel is
// cancelled, 499, with no fail_reason: the caller leaving says nothing about the key, and a
// fail_reason is what marks a key as failed.
func TestScenarioTimedOutAttemptCarriesItsClass(t *testing.T) {
	cases := []struct {
		name       string
		timeout    bool
		deadline   bool
		cancel     bool
		class      schemas.FailureClass
		status     int
		failReason string // "" wants none
	}{
		{name: "provider timeout", timeout: true, class: schemas.FailureClassTimeout, status: http.StatusGatewayTimeout, failReason: schemas.RequestTimedOut},
		{name: "request deadline", deadline: true, class: schemas.FailureClassTimeout, status: http.StatusGatewayTimeout, failReason: schemas.RequestTimedOut},
		{name: "caller cancel", cancel: true, class: schemas.FailureClassCancelled, status: 499},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := newScenarioUpstream(t)
			account := scenarioAccount(u, map[schemas.ModelProvider][]schemas.Key{provA: {scenarioKey("a1", 1)}})
			if tc.timeout {
				account.configs[provA].NetworkConfig.DefaultRequestTimeoutInSeconds = 1
			}
			router := &scenarioRouter{}
			client := scenarioClient(t, account, router, nil)
			u.reply("sk-a1", scenarioReply{stall: 3 * time.Second})
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			deadline := schemas.NoDeadline
			if tc.deadline {
				deadline = time.Now().Add(300 * time.Millisecond)
			}
			ctx := schemas.NewBifrostContext(parent, deadline)
			if tc.cancel {
				time.AfterFunc(300*time.Millisecond, cancel)
			}
			_, err := scenarioChat(client, ctx, provA, "m")
			if err == nil {
				t.Fatal("the request was served")
			}
			trail := router.awaitAttempts(t, 1)[0].trail
			if len(trail) != 1 {
				t.Fatalf("want one attempt, trail %+v", trail)
			}
			got := trail[0]
			if got.FailureClass != tc.class || got.StatusCode == nil || *got.StatusCode != tc.status {
				t.Fatalf("the attempt should carry class %q and status %d, got %+v", tc.class, tc.status, got)
			}
			switch {
			case tc.failReason == "" && got.FailReason != nil:
				t.Fatalf("the attempt should carry no fail_reason, got %q", *got.FailReason)
			case tc.failReason != "" && (got.FailReason == nil || *got.FailReason != tc.failReason):
				t.Fatalf("the attempt should carry fail_reason %q, got %v", tc.failReason, got.FailReason)
			}
		})
	}
}
