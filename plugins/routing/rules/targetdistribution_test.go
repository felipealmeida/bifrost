package rules

import (
	"testing"

	configstoreTables "github.com/maximhq/bifrost/framework/configstore/tables"
)

// A rule with several targets picks one at random per request, so whether it honours the weights
// can only be checked over many picks. The counts are judged with a chi-square goodness-of-fit test
// whose critical values sit at p = 1e-6, so a correct split essentially never fails.

// targetDraws is how many picks each distribution test makes.
const targetDraws = 20000

// targetChiSquareCritical maps degrees of freedom to the chi-square value a fit stays under at p = 1e-6.
var targetChiSquareCritical = map[int]float64{1: 23.928, 2: 27.631, 3: 30.664}

// weightedTargets builds one target per provider with the given weights.
func weightedTargets(providers []string, weights []float64) []configstoreTables.TableRoutingTarget {
	targets := make([]configstoreTables.TableRoutingTarget, len(providers))
	for i := range providers {
		provider := providers[i]
		targets[i] = configstoreTables.TableRoutingTarget{Provider: &provider, Weight: weights[i]}
	}
	return targets
}

// countTargetPicks draws selectWeightedTarget n times and counts the picks by provider.
func countTargetPicks(t *testing.T, targets []configstoreTables.TableRoutingTarget, n int) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for range n {
		target, ok := selectWeightedTarget(targets)
		if !ok {
			t.Fatalf("no target selected from %d targets", len(targets))
		}
		counts[*target.Provider]++
	}
	return counts
}

// requireTargetSplit fails the test when the counts do not fit the shares the weights imply.
func requireTargetSplit(t *testing.T, providers []string, counts map[string]int, weights []float64) {
	t.Helper()
	total, n := 0.0, 0
	for i, provider := range providers {
		total += weights[i]
		n += counts[provider]
	}
	stat := 0.0
	for i, provider := range providers {
		expected := float64(n) * weights[i] / total
		diff := float64(counts[provider]) - expected
		stat += diff * diff / expected
	}
	if critical := targetChiSquareCritical[len(providers)-1]; stat >= critical {
		t.Fatalf("picks %v do not fit weights %v (chi-square %.1f, critical %.1f)", counts, weights, stat, critical)
	}
}

// TestSelectWeightedTargetSplitFollowsTheWeights checks the share each target gets against its
// weight (plan row RR-05).
func TestSelectWeightedTargetSplitFollowsTheWeights(t *testing.T) {
	for _, tc := range []struct {
		name      string
		providers []string
		weights   []float64
	}{
		{name: "70/30", providers: []string{"openai", "anthropic"}, weights: []float64{0.7, 0.3}},
		{name: "50/30/20", providers: []string{"openai", "anthropic", "groq"}, weights: []float64{0.5, 0.3, 0.2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counts := countTargetPicks(t, weightedTargets(tc.providers, tc.weights), targetDraws)
			requireTargetSplit(t, tc.providers, counts, tc.weights)
		})
	}
}
