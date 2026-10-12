package telemetry

import (
	"strings"

	schemas "github.com/maximhq/bifrost/core/schemas"
)

type fallbackObserver struct {
	plugin            *PrometheusPlugin
	route             string
	firstFallbackSeen bool
	resultRecorded    bool
	totalFallbacks    int
}

func newFallbackObserver(plugin *PrometheusPlugin, route string, totalFallbacks int) *fallbackObserver {
	if route == "" {
		route = "unknown"
	}
	return &fallbackObserver{plugin: plugin, route: route, totalFallbacks: totalFallbacks}
}

var _ schemas.FallbackObserver = (*fallbackObserver)(nil)

func (o *fallbackObserver) OnFallbackTransition(fromProvider, fromModel, toProvider, toModel, reason, localReason string) {
	if o == nil || o.plugin == nil {
		return
	}
	if !o.firstFallbackSeen {
		o.firstFallbackSeen = true
		o.plugin.FallbackRequestsTotal.WithLabelValues(o.route).Inc()
	}
	o.plugin.FallbackTransitionsTotal.WithLabelValues(
		normalizeFallbackLabel(fromProvider),
		normalizeFallbackModel(fromModel),
		normalizeFallbackLabel(toProvider),
		normalizeFallbackModel(toModel),
		normalizeFallbackReason(reason),
		normalizeLocalReason(fromProvider, localReason),
	).Inc()
}

// normalizeLocalReason bounds the local_reason label. A non-local origin can
// never claim a local cause, even if its body carried an lpcache object.
func normalizeLocalReason(fromProvider, localReason string) string {
	if inferenceEnvironment(fromProvider) != "local" {
		return "none"
	}
	switch localReason {
	case "backend_busy", "prefill_down", "template_mismatch", "transport":
		return localReason
	default:
		return "unknown"
	}
}

func (o *fallbackObserver) OnRetryAttempt(provider, model, reason string) {
	if o == nil || o.plugin == nil {
		return
	}
	o.plugin.RetryAttemptsTotal.WithLabelValues(
		normalizeFallbackLabel(provider),
		normalizeFallbackModel(model),
		normalizeFallbackReason(reason),
	).Inc()
}

func (o *fallbackObserver) OnRequestTerminal(outcome string, fallbackIndex int) {
	if o == nil || o.plugin == nil || !o.firstFallbackSeen || o.resultRecorded {
		return
	}
	if outcome != "success" && outcome != "cancelled" && fallbackIndex != o.totalFallbacks {
		return
	}
	o.resultRecorded = true
	o.plugin.FallbackResultsTotal.WithLabelValues(o.route, normalizeFallbackOutcome(outcome)).Inc()
}

var fallbackModels = map[string]struct{}{
	"ai-server1-local":            {},
	"bifrost-chatgpt-gpt-6-astra": {},
	"claude-haiku-4-5":            {},
	"claude-opus-5-5":             {},
	"claude-sonnet-5-5":           {},
}

func normalizeFallbackLabel(value string) string {
	if strings.TrimSpace(value) == "" {
		return "unknown"
	}
	return value
}

func normalizeFallbackModel(value string) string {
	value = schemas.NormalizeModelName(value)
	if _, ok := fallbackModels[value]; !ok {
		return "unknown"
	}
	return value
}

func normalizeFallbackReason(value string) string {
	switch value {
	case "capacity", "rate_limit", "timeout", "provider_error", "transport_error", "context_limit", "unknown":
		return value
	default:
		return "unknown"
	}
}

func normalizeFallbackOutcome(value string) string {
	switch value {
	case "success", "error", "cancelled":
		return value
	default:
		return "error"
	}
}
