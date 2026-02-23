package api

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dilukangelosl/public-nft-api/internal/cache"
	"golang.org/x/time/rate"
)

type IPRateLimiter struct {
	ips   map[string]*rate.Limiter
	mutex sync.RWMutex
	rate  rate.Limit // Request limit frequency
	burst int        // Burst size (token bucket size)
	seen  map[string]time.Time
}

func NewIPRateLimiter(r rate.Limit, burst int) *IPRateLimiter {
	i := &IPRateLimiter{
		ips:   make(map[string]*rate.Limiter),
		rate:  r,
		burst: burst,
		seen:  make(map[string]time.Time),
	}

	// Stale IP eviction goroutine
	go func() {
		for {
			time.Sleep(time.Minute)
			i.cleanupStale()
		}
	}()

	return i
}

func (i *IPRateLimiter) AddIP(ip string) *rate.Limiter {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	limiter := rate.NewLimiter(i.rate, i.burst)
	i.ips[ip] = limiter
	i.seen[ip] = time.Now()
	return limiter
}

func (i *IPRateLimiter) GetLimiter(ip string) *rate.Limiter {
	i.mutex.RLock()
	limiter, exists := i.ips[ip]
	i.mutex.RUnlock()

	if !exists {
		return i.AddIP(ip)
	}

	i.mutex.Lock()
	i.seen[ip] = time.Now()
	i.mutex.Unlock()

	return limiter
}

func (i *IPRateLimiter) cleanupStale() {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	for ip, lastSeen := range i.seen {
		if time.Since(lastSeen) > 5*time.Minute {
			delete(i.ips, ip)
			delete(i.seen, ip)
		}
	}
}

// RateLimitMiddleware applies generic 60 r/m limiting based on standard PRD mapping
func RateLimitMiddleware(limiter *IPRateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := getIP(r)
			l := limiter.GetLimiter(ip)

			if !l.Allow() {
				w.Header().Set("Retry-After", "1") // generic 1 sec backoff hint
				RespondError(w, http.StatusTooManyRequests, "rate_limit_exceeded", "Too many requests. Limit 60 per minute per IP.")
				return
			}
			
			// Set rate limit headers
			w.Header().Set("X-RateLimit-Limit", "60")
			w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%.0f", l.Tokens()))

			next.ServeHTTP(w, r)
		})
	}
}

// CacheMiddleware intercepts GET requests and serves byte response directly from Ristretto if TTL cache hits
func CacheMiddleware(c *cache.Cache, ttl time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				next.ServeHTTP(w, r)
				return
			}

			key := r.URL.Path + "?" + r.URL.RawQuery

			if cachedData, found := c.Get(key); found {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Cache", "HIT")
				w.Write(cachedData)
				return
			}

			// Capture response
			rw := &responseWriterInterceptor{
				ResponseWriter: w,
				statusCode:     http.StatusOK, // default
			}
			
			w.Header().Set("X-Cache", "MISS")
			next.ServeHTTP(rw, r)

			// Only cache fully qualified 200 OK responses
			if rw.statusCode == http.StatusOK && len(rw.body) > 0 {
				c.Set(key, rw.body, ttl)
			}
		})
	}
}

type responseWriterInterceptor struct {
	http.ResponseWriter
	statusCode int
	body       []byte
}

func (rw *responseWriterInterceptor) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriterInterceptor) Write(b []byte) (int, error) {
	rw.body = append(rw.body, b...)
	return rw.ResponseWriter.Write(b)
}

// getIP attempts to grab true IP from behind reverse bounds
func getIP(r *http.Request) string {
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded != "" {
		ips := strings.Split(forwarded, ",")
		return strings.TrimSpace(ips[0])
	}
	
	realIP := r.Header.Get("X-Real-IP")
	if realIP != "" {
		return realIP
	}

	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// Pagination details default limit standard
func GetPagination(r *http.Request) (int, int) {
	page, err := strconv.Atoi(r.URL.Query().Get("page"))
	if err != nil || page < 1 {
		page = 1
	}

	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || limit < 1 {
		limit = 50
	} else if limit > 200 {
		limit = 200
	}

	return page, limit
}
