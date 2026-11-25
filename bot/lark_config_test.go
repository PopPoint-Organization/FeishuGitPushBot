package bot

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	"github.com/stretchr/testify/assert"
)

func TestResolveLarkOpenBaseURL(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "empty defaults to feishu", raw: "", want: lark.FeishuBaseUrl},
		{name: "feishu alias", raw: "feishu", want: lark.FeishuBaseUrl},
		{name: "lark alias", raw: "lark", want: lark.LarkBaseUrl},
		{name: "lark alias uppercase with spaces", raw: "  LARK  ", want: lark.LarkBaseUrl},
		{name: "explicit lark url with trailing slash", raw: "https://open.larksuite.com/", want: "https://open.larksuite.com"},
		{name: "custom proxy url", raw: "https://proxy.example.com/open", want: "https://proxy.example.com/open"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, resolveLarkOpenBaseURL(tc.raw))
		})
	}
}

func TestLoadConfigOpenBaseURL(t *testing.T) {
	t.Setenv("FEISHU_OPEN_BASE_URL", "https://open.larksuite.com")
	LoadConfig()
	assert.Equal(t, "https://open.larksuite.com", C.Feishu.OpenBaseURL)
}

func TestHealthzEndpointBypassesWebhookIPWhitelist(t *testing.T) {
	prevAllowed := C.Security.AllowedIPs
	prevCIDRs := allowedCIDRs
	defer func() {
		C.Security.AllowedIPs = prevAllowed
		allowedCIDRs = prevCIDRs
		cidrOnce = sync.Once{}
	}()

	C.Security.AllowedIPs = "192.30.252.0/22"
	allowedCIDRs = nil
	cidrOnce = sync.Once{}

	r := InitRouter()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"status":"ok"}`, w.Body.String())
}
