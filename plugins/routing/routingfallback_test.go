package routing

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	configstoreTables "github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/plugins/routing/rules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplyRoutingRules_CustomProviderFallbackDecodedBeforeRegistration covers rules loaded from the DB
// at boot: RoutingFallback.UnmarshalJSON runs before custom providers are registered, so the legacy
// "provider/model" string stays unsplit (Provider empty). By request time the provider is registered,
// and applyRoutingRules must still resolve it instead of dropping the fallback.
func TestApplyRoutingRules_CustomProviderFallbackDecodedBeforeRegistration(t *testing.T) {
	const customProvider = "boot-order-custom-provider"

	var fallbacks []configstoreTables.RoutingFallback
	require.NoError(t, json.Unmarshal([]byte(`["`+customProvider+`/some-model","not-a-provider/other-model"]`), &fallbacks))
	require.Len(t, fallbacks, 2)
	require.Empty(t, fallbacks[0].Provider, "provider is not registered yet, so decode cannot split it")

	// Registered after the rule was decoded, as happens at boot.
	schemas.RegisterKnownProvider(schemas.ModelProvider(customProvider))
	t.Cleanup(func() { schemas.UnregisterKnownProvider(schemas.ModelProvider(customProvider)) })

	store, err := rules.NewLocalStore(context.Background(), rules.NewMockLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, store.UpsertRule(context.Background(), &configstoreTables.TableRoutingRule{
		ID:            "late-provider-1",
		Name:          "Late Provider Rule",
		CelExpression: "model == 'gpt-4o'",
		Targets: []configstoreTables.TableRoutingTarget{
			{Provider: bifrost.Ptr("azure"), Model: bifrost.Ptr("gpt-4-turbo"), Weight: 1.0},
		},
		ParsedFallbacks: fallbacks,
		Enabled:         bifrost.Ptr(true),
		Scope:           "global",
		Priority:        0,
	}))

	plugin, err := InitFromStore(context.Background(), nil, rules.NewMockLogger(), nil, store, NewMockGovernance())
	require.NoError(t, err)

	req := &schemas.BifrostRequest{
		RequestType: schemas.ChatCompletionRequest,
		ChatRequest: &schemas.BifrostChatRequest{Provider: schemas.OpenAI, Model: "gpt-4o"},
	}
	root := schemas.NewBifrostContext(context.Background(), time.Now())
	root.BlockRestrictedWrites()
	pluginName := PluginName
	scoped := root.WithPluginScope(&pluginName)

	decision, err := plugin.applyRoutingRules(scoped, req, rules.GovernanceScope{})
	require.NoError(t, err)
	require.NotNil(t, decision)

	// The unparseable entry is still skipped.
	got := req.ChatRequest.Fallbacks
	require.Len(t, got, 1)
	assert.Equal(t, schemas.ModelProvider(customProvider), got[0].Provider)
	assert.Equal(t, "some-model", got[0].Model)
}
