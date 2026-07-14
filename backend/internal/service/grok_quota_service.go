package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/tidwall/gjson"
)

const (
	grokQuotaUpstreamTimeout       = 20 * time.Second
	grokQuotaManagementTimeout     = 15 * time.Second
	grokQuotaProbeInput            = "."
	grokQuotaDefaultModel          = "grok-4.3"
	grokQuotaManagementDefaultBase = "https://management-api.x.ai/v1"
)

type GrokQuotaProbeResult struct {
	Source          string             `json:"source"`
	Model           string             `json:"model"`
	Snapshot        *xai.QuotaSnapshot `json:"snapshot,omitempty"`
	StatusCode      int                `json:"status_code,omitempty"`
	HeadersObserved bool               `json:"headers_observed"`
	ResetSupported  bool               `json:"reset_supported"`
	FetchedAt       int64              `json:"fetched_at"`
}

type GrokQuotaResetResult struct {
	Supported bool   `json:"supported"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}

type GrokQuotaService struct {
	accountRepo   AccountRepository
	proxyRepo     ProxyRepository
	tokenProvider *GrokTokenProvider
	httpUpstream  HTTPUpstream
}

func NewGrokQuotaService(
	accountRepo AccountRepository,
	proxyRepo ProxyRepository,
	tokenProvider *GrokTokenProvider,
	httpUpstream HTTPUpstream,
) *GrokQuotaService {
	return &GrokQuotaService{
		accountRepo:   accountRepo,
		proxyRepo:     proxyRepo,
		tokenProvider: tokenProvider,
		httpUpstream:  httpUpstream,
	}
}

func (s *GrokQuotaService) ProbeUsage(ctx context.Context, accountID int64) (*GrokQuotaProbeResult, error) {
	account, token, proxyURL, err := s.prepareProbe(ctx, accountID)
	if err != nil {
		return nil, err
	}

	probeModel := grokQuotaProbeModel()
	body, err := buildGrokQuotaProbeBody(probeModel)
	if err != nil {
		return nil, infraerrors.Newf(http.StatusBadRequest, "GROK_QUOTA_PROBE_BODY_ERROR", "failed to build probe body: %v", err)
	}
	targetURL, err := xai.BuildResponsesURL(account.GetGrokBaseURL())
	if err != nil {
		return nil, infraerrors.Newf(http.StatusBadRequest, "GROK_QUOTA_BASE_URL_INVALID", "invalid Grok base_url: %v", err)
	}

	callCtx, cancel := context.WithTimeout(ctx, grokQuotaUpstreamTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return nil, infraerrors.Newf(http.StatusInternalServerError, "GROK_QUOTA_PROBE_REQUEST_BUILD_FAILED", "failed to build upstream request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "sub2api-grok-quota-probe/1.0")

	resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, maxInt(account.Concurrency, 1))
	if err != nil {
		return nil, infraerrors.Newf(http.StatusBadGateway, "GROK_QUOTA_PROBE_REQUEST_FAILED", "upstream probe failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	snapshot := xai.ObserveQuotaHeaders(resp.Header, resp.StatusCode, "active_probe")
	if credits := s.fetchManagementCredits(ctx, account, proxyURL); len(credits) > 0 {
		snapshot.Credits = mergeGrokCreditBalances(snapshot.Credits, credits)
		snapshot.HeadersObserved = true
		if snapshot.LastHeadersSeenAt == "" {
			snapshot.LastHeadersSeenAt = time.Now().UTC().Format(time.RFC3339)
		}
	}
	_ = s.accountRepo.UpdateExtra(ctx, account.ID, map[string]any{
		grokQuotaSnapshotExtraKey: snapshot,
	})

	result := &GrokQuotaProbeResult{
		Source:          "active_probe",
		Model:           probeModel,
		Snapshot:        snapshot,
		StatusCode:      resp.StatusCode,
		HeadersObserved: snapshot.HeadersObserved,
		ResetSupported:  false,
		FetchedAt:       time.Now().Unix(),
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return result, nil
	}
	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 240))
		bodyText := truncate(strings.TrimSpace(string(bodyBytes)), 240)
		slog.Warn("grok_quota_probe_failed", "account_id", account.ID, "model", probeModel, "status", resp.StatusCode, "body", bodyText)
		return nil, infraerrors.Newf(mapUpstreamStatus(resp.StatusCode), "GROK_QUOTA_PROBE_UPSTREAM_ERROR", "upstream returned %d for probe model %q: %s", resp.StatusCode, probeModel, bodyText)
	}
	return result, nil
}

func (s *GrokQuotaService) ResetQuota(ctx context.Context, accountID int64) (*GrokQuotaResetResult, error) {
	if _, err := s.loadGrokOAuthAccount(ctx, accountID); err != nil {
		return nil, err
	}
	return nil, infraerrors.New(http.StatusNotImplemented, "GROK_QUOTA_RESET_UNSUPPORTED", "xAI does not expose a Grok subscription quota reset endpoint for OAuth accounts")
}

func (s *GrokQuotaService) prepareProbe(ctx context.Context, accountID int64) (*Account, string, string, error) {
	if s == nil || s.tokenProvider == nil || s.httpUpstream == nil {
		return nil, "", "", infraerrors.New(http.StatusInternalServerError, "GROK_QUOTA_NOT_CONFIGURED", "grok quota service is not configured")
	}
	account, err := s.loadGrokOAuthAccount(ctx, accountID)
	if err != nil {
		return nil, "", "", err
	}

	token, err := s.tokenProvider.GetAccessToken(ctx, account)
	if err != nil {
		return nil, "", "", infraerrors.Newf(http.StatusBadGateway, "GROK_QUOTA_TOKEN_UNAVAILABLE", "failed to acquire access token: %v", err)
	}
	if strings.TrimSpace(token) == "" {
		return nil, "", "", infraerrors.New(http.StatusBadGateway, "GROK_QUOTA_TOKEN_UNAVAILABLE", "access token is empty")
	}

	return account, token, s.resolveProxyURL(ctx, account), nil
}

func (s *GrokQuotaService) resolveProxyURL(ctx context.Context, account *Account) string {
	if account == nil || account.ProxyID == nil {
		return ""
	}
	switch {
	case account.Proxy != nil:
		return account.Proxy.URL()
	case s != nil && s.proxyRepo != nil:
		if proxy, err := s.proxyRepo.GetByID(ctx, *account.ProxyID); err == nil && proxy != nil {
			return proxy.URL()
		}
	}
	return ""
}

func (s *GrokQuotaService) loadGrokOAuthAccount(ctx context.Context, accountID int64) (*Account, error) {
	if s == nil || s.accountRepo == nil {
		return nil, infraerrors.New(http.StatusInternalServerError, "GROK_QUOTA_NOT_CONFIGURED", "grok quota service is not configured")
	}
	account, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		return nil, infraerrors.Newf(http.StatusNotFound, "GROK_QUOTA_ACCOUNT_NOT_FOUND", "account not found: %v", err)
	}
	if account == nil {
		return nil, infraerrors.New(http.StatusNotFound, "GROK_QUOTA_ACCOUNT_NOT_FOUND", "account not found")
	}
	if account.Platform != PlatformGrok {
		return nil, infraerrors.New(http.StatusBadRequest, "GROK_QUOTA_INVALID_PLATFORM", "account is not a Grok account")
	}
	if account.Type != AccountTypeOAuth {
		return nil, infraerrors.New(http.StatusBadRequest, "GROK_QUOTA_INVALID_TYPE", "account is not an OAuth account")
	}
	return account, nil
}

func grokQuotaProbeModel() string {
	return grokQuotaDefaultModel
}

func buildGrokQuotaProbeBody(model string) ([]byte, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		model = grokQuotaDefaultModel
	}
	return json.Marshal(map[string]any{
		"model":             model,
		"input":             grokQuotaProbeInput,
		"max_output_tokens": 1,
		"store":             false,
	})
}

func (s *GrokQuotaService) fetchManagementCredits(ctx context.Context, account *Account, proxyURL string) []xai.CreditBalance {
	if s == nil || s.httpUpstream == nil || account == nil {
		return nil
	}
	managementKey := strings.TrimSpace(firstNonEmpty(
		account.GetCredential("management_key"),
		account.GetCredential("management_api_key"),
		account.GetCredential("xai_management_key"),
	))
	teamID := strings.TrimSpace(firstNonEmpty(
		account.GetCredential("team_id"),
		account.GetCredential("xai_team_id"),
	))
	if managementKey == "" || teamID == "" {
		return nil
	}

	baseURL := strings.TrimRight(strings.TrimSpace(account.GetCredential("management_base_url")), "/")
	if baseURL == "" {
		baseURL = grokQuotaManagementDefaultBase
	}
	credits := make([]xai.CreditBalance, 0, 3)
	for _, endpoint := range []struct {
		path   string
		parser func([]byte) []xai.CreditBalance
	}{
		{path: "/billing/teams/" + url.PathEscape(teamID) + "/postpaid/invoice/preview", parser: parseGrokPostpaidInvoicePreviewCredits},
		{path: "/billing/teams/" + url.PathEscape(teamID) + "/postpaid/spending-limits", parser: parseGrokPostpaidSpendingLimitCredits},
		{path: "/billing/teams/" + url.PathEscape(teamID) + "/prepaid/balance", parser: parseGrokPrepaidBalanceCredits},
	} {
		body, statusCode, err := s.fetchManagementBillingEndpoint(ctx, account, proxyURL, baseURL+endpoint.path, managementKey)
		if err != nil {
			slog.Debug("grok_management_billing_probe_failed", "account_id", account.ID, "path", endpoint.path, "error", err)
			continue
		}
		if statusCode >= 400 {
			slog.Debug("grok_management_billing_probe_non_success", "account_id", account.ID, "path", endpoint.path, "status", statusCode)
			continue
		}
		credits = append(credits, endpoint.parser(body)...)
	}
	return credits
}

func (s *GrokQuotaService) fetchManagementBillingEndpoint(ctx context.Context, account *Account, proxyURL, targetURL, managementKey string) ([]byte, int, error) {
	callCtx, cancel := context.WithTimeout(ctx, grokQuotaManagementTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+managementKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "sub2api-grok-management-quota/1.0")
	resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, maxInt(account.Concurrency, 1))
	if err != nil {
		return nil, 0, err
	}
	if resp == nil {
		return nil, 0, nil
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

func parseGrokPostpaidInvoicePreviewCredits(body []byte) []xai.CreditBalance {
	credits := make([]xai.CreditBalance, 0, 3)
	if value := centsResultToDollars(gjson.GetBytes(body, "defaultCredits")); value != nil {
		credits = append(credits, xai.CreditBalance{
			CreditType: "monthly_credits",
			Label:      "Monthly credits",
			Amount:     value,
			Currency:   "USD",
			Source:     "management_postpaid_invoice_preview",
		})
	}
	if value := centsResultToDollars(firstGJSON(body, "effectiveSpendingLimit", "spendingLimit", "softSpendingLimit")); value != nil {
		credits = append(credits, xai.CreditBalance{
			CreditType: "pay_as_you_go",
			Label:      "Pay-as-you-go",
			Limit:      value,
			Currency:   "USD",
			Source:     "management_postpaid_invoice_preview",
		})
	}
	if prepaid := centsResultToDollars(gjson.GetBytes(body, "coreInvoice.prepaidCredits")); prepaid != nil {
		credit := xai.CreditBalance{
			CreditType: "prepaid_credits",
			Label:      "Prepaid credits",
			Amount:     prepaid,
			Currency:   "USD",
			Source:     "management_postpaid_invoice_preview",
		}
		if used := centsResultToDollars(gjson.GetBytes(body, "coreInvoice.prepaidCreditsUsed")); used != nil {
			credit.Used = used
			remaining := *prepaid - *used
			if remaining < 0 {
				remaining = 0
			}
			credit.Remaining = &remaining
		}
		credits = append(credits, credit)
	}
	return credits
}

func parseGrokPostpaidSpendingLimitCredits(body []byte) []xai.CreditBalance {
	if value := centsResultToDollars(firstGJSON(body, "effectiveSpendingLimit", "effectiveSl", "softSpendingLimit", "softSl")); value != nil {
		return []xai.CreditBalance{{
			CreditType: "pay_as_you_go",
			Label:      "Pay-as-you-go",
			Limit:      value,
			Currency:   "USD",
			Source:     "management_postpaid_spending_limits",
		}}
	}
	return nil
}

func parseGrokPrepaidBalanceCredits(body []byte) []xai.CreditBalance {
	if value := centsResultToDollars(firstGJSON(body, "total", "balance", "remaining", "prepaidCredits")); value != nil {
		return []xai.CreditBalance{{
			CreditType: "prepaid_credits",
			Label:      "Prepaid credits",
			Remaining:  value,
			Currency:   "USD",
			Source:     "management_prepaid_balance",
		}}
	}
	return nil
}

func mergeGrokCreditBalances(existing, incoming []xai.CreditBalance) []xai.CreditBalance {
	if len(existing) == 0 {
		return incoming
	}
	out := append([]xai.CreditBalance(nil), existing...)
	for _, next := range incoming {
		merged := false
		for i := range out {
			if out[i].CreditType != "" && out[i].CreditType == next.CreditType {
				if next.Label != "" {
					out[i].Label = next.Label
				}
				if next.Amount != nil {
					out[i].Amount = next.Amount
				}
				if next.Limit != nil {
					out[i].Limit = next.Limit
				}
				if next.Used != nil {
					out[i].Used = next.Used
				}
				if next.Remaining != nil {
					out[i].Remaining = next.Remaining
				}
				if next.Currency != "" {
					out[i].Currency = next.Currency
				}
				if next.ResetAt != "" {
					out[i].ResetAt = next.ResetAt
				}
				if next.Source != "" {
					out[i].Source = next.Source
				}
				merged = true
				break
			}
		}
		if !merged {
			out = append(out, next)
		}
	}
	return out
}

func firstGJSON(body []byte, paths ...string) gjson.Result {
	for _, path := range paths {
		if result := gjson.GetBytes(body, path); result.Exists() {
			return result
		}
	}
	return gjson.Result{}
}

func centsResultToDollars(result gjson.Result) *float64 {
	if !result.Exists() {
		return nil
	}
	var cents float64
	switch result.Type {
	case gjson.Number:
		cents = result.Float()
	case gjson.String:
		raw := strings.TrimSpace(result.String())
		if raw == "" {
			return nil
		}
		value, err := strconv.ParseFloat(strings.ReplaceAll(strings.TrimPrefix(raw, "$"), ",", ""), 64)
		if err != nil {
			return nil
		}
		cents = value
	default:
		return nil
	}
	dollars := cents / 100
	return &dollars
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
