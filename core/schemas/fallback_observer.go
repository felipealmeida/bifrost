package schemas

type FallbackObserver interface {
	OnFallbackTransition(fromProvider, fromModel, toProvider, toModel, reason string)
	OnRetryAttempt(provider, model, reason string)
}
