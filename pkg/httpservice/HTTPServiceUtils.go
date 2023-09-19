package httpservice

import "github.com/google/uuid"


func (httpService *HTTPService) GenerateRequestUUID() string {
	id := uuid.New()
	return id.String()
}