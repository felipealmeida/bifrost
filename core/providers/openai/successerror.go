package openai

import (
	"net/http"
	"strings"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/tidwall/gjson"
)

// ErrorInSuccessfulChatBody recognizes explicit error envelopes in HTTP 200
// bodies and chat SSE data. It does not impose an empty-completion policy.
func ErrorInSuccessfulChatBody(body []byte) *schemas.BifrostError {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return nil
	}
	return errorInValidatedChatBody(body)
}

// errorInValidatedChatBody skips validation when the caller already decoded body.
func errorInValidatedChatBody(body []byte) *schemas.BifrostError {
	errObj := gjson.GetBytes(body, "error")
	objectError := gjson.GetBytes(body, "object").String() == "error"
	if objectError {
		errObj = gjson.ParseBytes(body)
	} else if !errObj.Exists() || errObj.Type == gjson.Null {
		return nil
	}

	field := &schemas.ErrorField{}
	statusCode := http.StatusBadGateway
	switch {
	case errObj.Type == gjson.String:
		field.Message = strings.TrimSpace(errObj.String())
	case errObj.IsObject():
		if message := errObj.Get("message"); message.Type == gjson.String {
			field.Message = strings.TrimSpace(message.String())
		}
		code := errObj.Get("code")
		if (code.Type == gjson.String || code.Type == gjson.Number) && strings.TrimSpace(code.String()) != "" {
			field.Code = schemas.Ptr(code.String())
			if code.Type == gjson.Number && code.Float() == float64(code.Int()) && code.Int() >= 400 && code.Int() <= 599 {
				statusCode = int(code.Int())
			}
		}
		errType := errObj.Get("metadata.error_type")
		if errType.Type != gjson.String || strings.TrimSpace(errType.String()) == "" {
			errType = errObj.Get("type")
		}
		if errType.Type == gjson.String && strings.TrimSpace(errType.String()) != "" {
			field.Type = schemas.Ptr(errType.String())
		}
		if param := errObj.Get("param"); param.Exists() {
			field.Param = param.Value()
		}
		if eventID := errObj.Get("event_id"); eventID.Type == gjson.String {
			field.EventID = schemas.Ptr(eventID.String())
		}
	}
	if field.Message == "" {
		switch {
		case field.Code != nil:
			field.Message = "provider error: " + *field.Code
		case field.Type != nil:
			field.Message = "provider error: " + *field.Type
		default:
			// A malformed optional error field must not discard valid nontext
			// choices. An explicit object:error, however, always denotes failure.
			if !objectError {
				choices := gjson.GetBytes(body, "choices")
				if choices.IsArray() {
					for _, choice := range choices.Array() {
						if choice.Get("message").IsObject() || choice.Get("delta").IsObject() || choice.Get("text").Type == gjson.String {
							return nil
						}
					}
				}
			}
			field.Message = "provider returned a malformed error envelope"
		}
	}
	return &schemas.BifrostError{
		IsBifrostError: false,
		StatusCode:     schemas.Ptr(statusCode),
		Error:          field,
	}
}
