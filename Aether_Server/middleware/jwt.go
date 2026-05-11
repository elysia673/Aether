package middleware

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

var (
	jwtSecret   []byte
	registerDir = "./data/registered_keys"
)

func InitJWTSecret() {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		log.Fatalf("生成 JWT 密钥失败: %v", err)
	}
	jwtSecret = b
	log.Println("JWT 密钥已随机生成（重启后旧 token 失效）")
}

type Claims struct {
	APIKey string `json:"api_key"`
	jwt.RegisteredClaims
}

func getKeyFilePath(apiKey string) string {
	hash := sha256.Sum256([]byte(apiKey))
	filename := hex.EncodeToString(hash[:])
	return filepath.Join(registerDir, filename)
}

func isKeyRegistered(apiKey string) bool {
	path := getKeyFilePath(apiKey)
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func markKeyRegistered(apiKey string) error {
	err := os.MkdirAll(registerDir, 0755)
	if err != nil {
		log.Printf("创建目录失败: %v (路径: %s)", err, registerDir)
		return err
	}
	path := getKeyFilePath(apiKey)
	log.Printf("创建占位文件: %s", path)
	err = os.WriteFile(path, []byte("registered"), 0644)
	if err != nil {
		log.Printf("写入文件失败: %v (路径: %s)", err, path)
		return err
	}
	return nil
}

func CleanupExpiredRegistrations() {
	os.MkdirAll(registerDir, 0755)

	files, err := os.ReadDir(registerDir)
	if err != nil {
		return
	}

	for _, file := range files {
		path := filepath.Join(registerDir, file.Name())
		info, err := file.Info()
		if err != nil {
			continue
		}

		// 删除超过 1 年的注册文件
		if time.Since(info.ModTime()) > 365*24*time.Hour {
			os.Remove(path)
		}
	}
}

// GenerateToken 生成 JWT Token
func GenerateToken(apiKey string) (string, error) {
	absPath, _ := filepath.Abs(getKeyFilePath(apiKey))
	registered := isKeyRegistered(apiKey)
	log.Printf("检查 API Key 是否已注册: %v (路径: %s, 绝对路径: %s)", registered, getKeyFilePath(apiKey), absPath)
	
	if registered {
		return "", fmt.Errorf("API Key 已注册，请使用已有的 token")
	}

	claims := Claims{
		APIKey: apiKey,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(365 * 24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, err := token.SignedString(jwtSecret)
	if err != nil {
		return "", fmt.Errorf("生成 Token 失败: %w", err)
	}

	if err := markKeyRegistered(apiKey); err != nil {
		return "", fmt.Errorf("记录 API Key 失败: %w", err)
	}

	return tokenStr, nil
}

func JWTAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(401, gin.H{"error": "missing authorization header"})
			return
		}

		tokenString := authHeader[7:]
		claims := &Claims{}

		token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
			return jwtSecret, nil
		})

		if err != nil || !token.Valid {
			c.AbortWithStatusJSON(401, gin.H{"error": "invalid token"})
			return
		}

		// 实时检查注册文件是否还存在
		if !isKeyRegistered(claims.APIKey) {
			c.AbortWithStatusJSON(401, gin.H{"error": "token revoked, please login again"})
			return
		}

		c.Set("api_key", claims.APIKey)
		c.Next()
	}
}
