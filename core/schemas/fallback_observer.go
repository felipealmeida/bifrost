package schemas

type FallbackObserver interface {
	OnFallbackTransition(fromProvider, fromModel, toProvider, toModel, reason, localReason string)
	OnRetryAttempt(provider, model, reason string)
}
