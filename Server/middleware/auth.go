package middleware

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

const apiKeyHeader = "X-API-KEY"

func Auth(validKey string) gin.HandlerFunc {
	return func(c *gin.Context) {
		apiKey := c.GetHeader(apiKeyHeader)
		if apiKey != validKey {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Next()
	}
}
