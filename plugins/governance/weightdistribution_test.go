package governance

import (
	"math"
	"testing"

	"github.com/maximhq/bifrost/core/schemas"
	configstoreTables "github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/stretchr/testify/require"
)

// The virtual key's weighted provider pick is random per request, so the only way to check that it
// honours the configured split is to draw it many times and test the counts. The draws call the real
// LoadBalanceProvider on a store built from the virtual key, and the counts are judged with a
// chi-square goodness-of-fit test whose critical values sit at p = 1e-6: a correct split fails about
// once in a million runs, while a real skew (60/40 where 70/30 was configured) fails by a wide margin.

// weightDraws is how many picks each distribution test makes.
const weightDraws = 20000

// chiSquareCritical maps degrees of freedom to the chi-square value a fit stays under at p = 1e-6.
var chiSquareCritical = map[int]float64{1: 23.928, 2: 27.631, 3: 30.664}

// requireSplit fails the test when counts do not fit the shares the weights imply.
func requireSplit(t *testing.T, names []string, counts map[string]int, weights []float64) {
	t.Helper()
	total, n := 0.0, 0
	for i, name := range names {
		total += weights[i]
		n += counts[name]
	}
	stat := 0.0
	for i, name := range names {
		expected := float64(n) * weights[i] / total
		diff := float64(counts[name]) - expected
		stat += diff * diff / expected
	}
	critical := chiSquareCritical[len(names)-1]
	require.Lessf(t, stat, critical, "picks %v do not fit weights %v (chi-square %.1f, critical %.1f)", counts, weights, stat, critical)
}

// weightedVK builds a virtual key that allows every model on each provider, with the given weights.
// A nil weight leaves the provider unweighted.
func weightedVK(providers []string, weights []*float64) *configstoreTables.TableVirtualKey {
	configs := make([]configstoreTables.TableVirtualKeyProviderConfig, len(providers))
	for i, provider := range providers {
		configs[i] = buildProviderConfig(provider, []string{"*"})
		configs[i].Weight = weights[i]
	}
	return buildVirtualKeyWithProviders("vk1", "sk-bf-lb", "LB VK", configs)
}

// pick runs one weighted provider pick for a bare model and returns the provider and the fallbacks
// it attached.
func pick(t *testing.T, p *GovernancePlugin) (schemas.ModelProvider, []schemas.Fallback) {
	t.Helper()
	req := &schemas.BifrostRequest{RequestType: schemas.ChatCompletionRequest, ChatRequest: &schemas.BifrostChatRequest{Model: "m"}}
	require.NoError(t, p.LoadBalanceProvider(lbCtx(), req))
	provider, _, fallbacks := req.GetRequestFields()
	return provider, fallbacks
}

// TestLoadBalanceProvider_SplitFollowsTheWeights checks that the share of requests each provider
// gets as primary matches its weight, for equal and asymmetric weights (plan rows GV-02, GV-03).
func TestLoadBalanceProvider_SplitFollowsTheWeights(t *testing.T) {
	providers := []string{"openai", "anthropic", "groq"}
	for _, tc := range []struct {
		name    string
		weights []float64
	}{
		{name: "equal weights", weights: []float64{1, 1, 1}},
		{name: "70/20/10", weights: []float64{0.7, 0.2, 0.1}},
		{name: "weights that do not sum to one", weights: []float64{3, 1, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newLoadBalanceTestPlugin(t, weightedVK(providers, []*float64{&tc.weights[0], &tc.weights[1], &tc.weights[2]}))
			counts := map[string]int{}
			for range weightDraws {
				provider, _ := pick(t, p)
				counts[string(provider)]++
			}
			requireSplit(t, providers, counts, tc.weights)
		})
	}
}

// TestLoadBalanceProvider_FallbacksFollowTheWeights checks that every pick carries the other weighted
// providers as fallbacks, heaviest first (plan row GV-03).
func TestLoadBalanceProvider_FallbacksFollowTheWeights(t *testing.T) {
	p := newLoadBalanceTestPlugin(t, weightedVK([]string{"openai", "anthropic", "groq"}, []*float64{schemas.Ptr(0.7), schemas.Ptr(0.2), schemas.Ptr(0.1)}))
	want := map[schemas.ModelProvider][]schemas.ModelProvider{
		"openai":    {"anthropic", "groq"},
		"anthropic": {"openai", "groq"},
		"groq":      {"openai", "anthropic"},
	}
	seen := map[schemas.ModelProvider]bool{}
	for range weightDraws {
		provider, fallbacks := pick(t, p)
		seen[provider] = true
		got := make([]schemas.ModelProvider, len(fallbacks))
		for i, fallback := range fallbacks {
			got[i] = fallback.Provider
		}
		require.Equal(t, want[provider], got, "fallbacks for a pick of %s", provider)
	}
	require.Len(t, seen, 3, "every weighted provider should be picked at least once in %d draws", weightDraws)
}

// TestLoadBalanceProvider_ZeroWeightIsAFallbackOnly checks that a weight-0 provider is never the
// primary but is still attached as the last fallback, and that with every weight at 0 the first
// configured provider is always picked (plan rows GV-04, GV-05).
func TestLoadBalanceProvider_ZeroWeightIsAFallbackOnly(t *testing.T) {
	t.Run("zero beside a positive weight", func(t *testing.T) {
		p := newLoadBalanceTestPlugin(t, weightedVK([]string{"anthropic", "openai"}, []*float64{schemas.Ptr(0.0), schemas.Ptr(1.0)}))
		for range weightDraws {
			provider, fallbacks := pick(t, p)
			require.Equal(t, schemas.ModelProvider("openai"), provider, "a weight-0 provider was picked as primary")
			require.Equal(t, []schemas.Fallback{{Provider: "anthropic", Model: "m"}}, fallbacks)
		}
	})
	t.Run("every weight zero", func(t *testing.T) {
		p := newLoadBalanceTestPlugin(t, weightedVK([]string{"anthropic", "openai"}, []*float64{schemas.Ptr(0.0), schemas.Ptr(0.0)}))
		for range 1000 {
			provider, fallbacks := pick(t, p)
			require.Equal(t, schemas.ModelProvider("anthropic"), provider, "with every weight at 0 the first configured provider is picked")
			require.Equal(t, []schemas.Fallback{{Provider: "openai", Model: "m"}}, fallbacks)
		}
	})
	t.Run("an unweighted provider is neither picked nor a fallback", func(t *testing.T) {
		p := newLoadBalanceTestPlugin(t, weightedVK([]string{"anthropic", "openai"}, []*float64{nil, schemas.Ptr(1.0)}))
		for range 1000 {
			provider, fallbacks := pick(t, p)
			require.Equal(t, schemas.ModelProvider("openai"), provider)
			require.Empty(t, fallbacks, "a single weighted provider gets no fallbacks")
		}
	})
}

// TestRequireSplitCatchesASkew guards the statistic behind plan rows GV-02 and GV-03: a 60/40 split
// must not pass for 70/30.
func TestRequireSplitCatchesASkew(t *testing.T) {
	stat := 0.0
	n := float64(weightDraws)
	for i, observed := range []float64{0.6 * n, 0.4 * n} {
		expected := n * []float64{0.7, 0.3}[i]
		stat += math.Pow(observed-expected, 2) / expected
	}
	require.Greater(t, stat, chiSquareCritical[1], "a 60/40 split should fail a 70/30 expectation")
}
