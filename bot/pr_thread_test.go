package bot

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memoryPRThreadStore struct {
	mu         sync.Mutex
	deliveries map[string]bool
	prs        map[string]*PRThreadRecord
	bySHA      map[string]int
	byBranch   map[string]int
}

func newMemoryPRThreadStore() *memoryPRThreadStore {
	return &memoryPRThreadStore{
		deliveries: map[string]bool{},
		prs:        map[string]*PRThreadRecord{},
		bySHA:      map[string]int{},
		byBranch:   map[string]int{},
	}
}

func (m *memoryPRThreadStore) ClaimDelivery(_ context.Context, deliveryID string, _ time.Duration) (bool, error) {
	if deliveryID == "" {
		return true, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deliveries[deliveryID] {
		return false, nil
	}
	m.deliveries[deliveryID] = true
	return true, nil
}

func (m *memoryPRThreadStore) GetPR(_ context.Context, repo string, number int) (*PRThreadRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.prs[fmt.Sprintf("%s#%d", repo, number)]
	if !ok {
		return nil, nil
	}
	b, _ := json.Marshal(rec)
	var cp PRThreadRecord
	_ = json.Unmarshal(b, &cp)
	return &cp, nil
}

func (m *memoryPRThreadStore) SavePR(_ context.Context, rec *PRThreadRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, _ := json.Marshal(rec)
	var cp PRThreadRecord
	_ = json.Unmarshal(b, &cp)
	m.prs[fmt.Sprintf("%s#%d", rec.Repo, rec.Number)] = &cp
	if rec.HeadSHA != "" {
		m.bySHA[fmt.Sprintf("%s#%s", rec.Repo, rec.HeadSHA)] = rec.Number
	}
	if rec.HeadBranch != "" {
		m.byBranch[fmt.Sprintf("%s#%s", rec.Repo, rec.HeadBranch)] = rec.Number
	}
	return nil
}

func (m *memoryPRThreadStore) FindPRNumberBySHA(_ context.Context, repo, sha string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bySHA[fmt.Sprintf("%s#%s", repo, sha)], nil
}

func (m *memoryPRThreadStore) FindPRNumberByBranch(_ context.Context, repo, branch string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.byBranch[fmt.Sprintf("%s#%s", repo, branch)], nil
}

type recordedMessage struct {
	Kind      string // "send", "update", "reply"
	TargetID  string
	CardJSON  string
	MessageID string
}

type fakeLarkMessenger struct {
	mu      sync.Mutex
	seq     int
	actions []recordedMessage
}

func (f *fakeLarkMessenger) SendToChat(chatID string, card *Card) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	id := fmt.Sprintf("om_msg_%d", f.seq)
	f.actions = append(f.actions, recordedMessage{
		Kind:      "send",
		TargetID:  chatID,
		CardJSON:  card.String(),
		MessageID: id,
	})
	return id, nil
}

func (f *fakeLarkMessenger) UpdateMessage(messageID string, card *Card) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.actions = append(f.actions, recordedMessage{
		Kind:     "update",
		TargetID: messageID,
		CardJSON: card.String(),
	})
	return nil
}

func (f *fakeLarkMessenger) ReplyToMessage(parentID string, card *Card) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	id := fmt.Sprintf("om_reply_%d", f.seq)
	f.actions = append(f.actions, recordedMessage{
		Kind:      "reply",
		TargetID:  parentID,
		CardJSON:  card.String(),
		MessageID: id,
	})
	return id, nil
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func TestPRThreadLifecycleOnePRMasterCardAndReplies(t *testing.T) {
	store := newMemoryPRThreadStore()
	messenger := &fakeLarkMessenger{}
	svc := &PRThreadService{
		Store:     store,
		Messenger: messenger,
		ChatID:    "oc_pr_channel",
	}
	ctx := context.Background()

	// 1. Open PR -> sends 1 root card to channel
	openPayload := mustJSON(t, map[string]any{
		"action": "opened",
		"number": 42,
		"pull_request": map[string]any{
			"number":        42,
			"title":         "feat: add PR thread aggregation",
			"body":          "Aggregate all PR updates into one Lark topic.",
			"html_url":      "https://github.com/org/repo/pull/42",
			"state":         "open",
			"draft":         false,
			"additions":     120,
			"deletions":     15,
			"changed_files": 4,
			"user":          map[string]any{"login": "alice", "html_url": "https://github.com/alice"},
			"head":          map[string]any{"ref": "feat/pr-threads", "sha": "sha111"},
			"base":          map[string]any{"ref": "main", "sha": "base000"},
		},
		"repository": map[string]any{"full_name": "org/repo", "html_url": "https://github.com/org/repo"},
		"sender":     map[string]any{"login": "alice", "html_url": "https://github.com/alice"},
	})
	status, err := svc.HandleWebhook(ctx, "del-1", "pull_request", openPayload)
	require.NoError(t, err)
	assert.Equal(t, "sent", status)
	require.Len(t, messenger.actions, 1)
	assert.Equal(t, "send", messenger.actions[0].Kind)
	assert.Equal(t, "oc_pr_channel", messenger.actions[0].TargetID)
	assert.Contains(t, messenger.actions[0].CardJSON, "#42")
	assert.Contains(t, messenger.actions[0].CardJSON, "feat: add PR thread aggregation")
	rootID := messenger.actions[0].MessageID

	// Duplicate delivery is ignored
	status, err = svc.HandleWebhook(ctx, "del-1", "pull_request", openPayload)
	require.NoError(t, err)
	assert.Equal(t, "duplicate", status)
	require.Len(t, messenger.actions, 1)

	// 2. Synchronize (push new commit) -> updates root card in place, no new send or reply
	syncPayload := mustJSON(t, map[string]any{
		"action": "synchronize",
		"number": 42,
		"pull_request": map[string]any{
			"number":        42,
			"title":         "feat: add PR thread aggregation",
			"html_url":      "https://github.com/org/repo/pull/42",
			"state":         "open",
			"draft":         false,
			"additions":     140,
			"deletions":     18,
			"changed_files": 5,
			"user":          map[string]any{"login": "alice", "html_url": "https://github.com/alice"},
			"head":          map[string]any{"ref": "feat/pr-threads", "sha": "sha222"},
			"base":          map[string]any{"ref": "main", "sha": "base000"},
		},
		"repository": map[string]any{"full_name": "org/repo", "html_url": "https://github.com/org/repo"},
		"sender":     map[string]any{"login": "alice"},
	})
	status, err = svc.HandleWebhook(ctx, "del-2", "pull_request", syncPayload)
	require.NoError(t, err)
	assert.Equal(t, "updated", status)
	require.Len(t, messenger.actions, 2)
	assert.Equal(t, "update", messenger.actions[1].Kind)
	assert.Equal(t, rootID, messenger.actions[1].TargetID)

	// 3. workflow_run in_progress (matched via sha222) -> updates root card only, no spam
	ciRunPayload := mustJSON(t, map[string]any{
		"action": "in_progress",
		"workflow_run": map[string]any{
			"name":          "CI",
			"status":        "in_progress",
			"conclusion":    "",
			"head_sha":      "sha222",
			"head_branch":   "feat/pr-threads",
			"html_url":      "https://github.com/org/repo/actions/runs/100",
			"pull_requests": []any{},
		},
		"repository": map[string]any{"full_name": "org/repo", "html_url": "https://github.com/org/repo"},
		"sender":     map[string]any{"login": "alice"},
	})
	status, err = svc.HandleWebhook(ctx, "del-3", "workflow_run", ciRunPayload)
	require.NoError(t, err)
	assert.Equal(t, "updated", status)
	require.Len(t, messenger.actions, 3)
	assert.Equal(t, "update", messenger.actions[2].Kind)
	assert.Equal(t, rootID, messenger.actions[2].TargetID)
	assert.Contains(t, messenger.actions[2].CardJSON, "CI")

	// 4. workflow_run failure -> updates root card AND replies in thread
	ciFailPayload := mustJSON(t, map[string]any{
		"action": "completed",
		"workflow_run": map[string]any{
			"name":          "CI",
			"status":        "completed",
			"conclusion":    "failure",
			"head_sha":      "sha222",
			"head_branch":   "feat/pr-threads",
			"html_url":      "https://github.com/org/repo/actions/runs/100",
			"pull_requests": []any{},
		},
		"repository": map[string]any{"full_name": "org/repo", "html_url": "https://github.com/org/repo"},
		"sender":     map[string]any{"login": "alice"},
	})
	status, err = svc.HandleWebhook(ctx, "del-4", "workflow_run", ciFailPayload)
	require.NoError(t, err)
	assert.Equal(t, "replied", status)
	require.Len(t, messenger.actions, 5)
	assert.Equal(t, "update", messenger.actions[3].Kind)
	assert.Equal(t, rootID, messenger.actions[3].TargetID)
	assert.Equal(t, "reply", messenger.actions[4].Kind)
	assert.Equal(t, rootID, messenger.actions[4].TargetID)
	assert.Contains(t, messenger.actions[4].CardJSON, "CI")

	// 5. Non-PR workflow_run (e.g. main branch) -> ignored
	mainCIPayload := mustJSON(t, map[string]any{
		"action": "completed",
		"workflow_run": map[string]any{
			"name":          "CI",
			"status":        "completed",
			"conclusion":    "success",
			"head_sha":      "mainsha999",
			"head_branch":   "main",
			"html_url":      "https://github.com/org/repo/actions/runs/101",
			"pull_requests": []any{},
		},
		"repository": map[string]any{"full_name": "org/repo"},
		"sender":     map[string]any{"login": "alice"},
	})
	status, err = svc.HandleWebhook(ctx, "del-5", "workflow_run", mainCIPayload)
	require.NoError(t, err)
	assert.Equal(t, "ignored", status)
	require.Len(t, messenger.actions, 5)

	// 6. pull_request_review empty commented container -> ignored
	emptyReviewPayload := mustJSON(t, map[string]any{
		"action": "submitted",
		"review": map[string]any{
			"state":    "commented",
			"body":     "",
			"html_url": "https://github.com/org/repo/pull/42#pullrequestreview-1",
			"user":     map[string]any{"login": "bob"},
		},
		"pull_request": map[string]any{"number": 42, "title": "feat: add PR thread aggregation", "html_url": "https://github.com/org/repo/pull/42"},
		"repository":   map[string]any{"full_name": "org/repo"},
		"sender":       map[string]any{"login": "bob"},
	})
	status, err = svc.HandleWebhook(ctx, "del-6", "pull_request_review", emptyReviewPayload)
	require.NoError(t, err)
	assert.Equal(t, "ignored", status)
	require.Len(t, messenger.actions, 5)

	// 7. pull_request_review approved -> updates root card AND replies in thread
	approvePayload := mustJSON(t, map[string]any{
		"action": "submitted",
		"review": map[string]any{
			"state":    "approved",
			"body":     "LGTM!",
			"html_url": "https://github.com/org/repo/pull/42#pullrequestreview-2",
			"user":     map[string]any{"login": "bob", "html_url": "https://github.com/bob"},
		},
		"pull_request": map[string]any{"number": 42, "title": "feat: add PR thread aggregation", "html_url": "https://github.com/org/repo/pull/42"},
		"repository":   map[string]any{"full_name": "org/repo"},
		"sender":       map[string]any{"login": "bob"},
	})
	status, err = svc.HandleWebhook(ctx, "del-7", "pull_request_review", approvePayload)
	require.NoError(t, err)
	assert.Equal(t, "replied", status)
	require.Len(t, messenger.actions, 7)
	assert.Equal(t, "update", messenger.actions[5].Kind)
	assert.Contains(t, messenger.actions[5].CardJSON, "bob")
	assert.Equal(t, "reply", messenger.actions[6].Kind)
	assert.Equal(t, rootID, messenger.actions[6].TargetID)
	assert.Contains(t, messenger.actions[6].CardJSON, "LGTM!")

	// 8. pull_request_review_comment -> replies in thread with file:line
	lineCommentPayload := mustJSON(t, map[string]any{
		"action": "created",
		"comment": map[string]any{
			"body":     "Consider adding a timeout here.",
			"path":     "bot/pr_thread.go",
			"line":     88,
			"html_url": "https://github.com/org/repo/pull/42#discussion_r123",
			"user":     map[string]any{"login": "carol", "html_url": "https://github.com/carol"},
		},
		"pull_request": map[string]any{"number": 42, "title": "feat: add PR thread aggregation", "html_url": "https://github.com/org/repo/pull/42"},
		"repository":   map[string]any{"full_name": "org/repo"},
		"sender":       map[string]any{"login": "carol"},
	})
	status, err = svc.HandleWebhook(ctx, "del-8", "pull_request_review_comment", lineCommentPayload)
	require.NoError(t, err)
	assert.Equal(t, "replied", status)
	require.Len(t, messenger.actions, 8)
	assert.Equal(t, "reply", messenger.actions[7].Kind)
	assert.Equal(t, rootID, messenger.actions[7].TargetID)
	assert.Contains(t, messenger.actions[7].CardJSON, "bot/pr_thread.go:88")

	// 9. issue_comment on PR -> replies in thread; non-PR issue comment -> ignored
	prCommentPayload := mustJSON(t, map[string]any{
		"action": "created",
		"issue": map[string]any{
			"number":       42,
			"title":        "feat: add PR thread aggregation",
			"html_url":     "https://github.com/org/repo/pull/42",
			"pull_request": map[string]any{"url": "https://api.github.com/repos/org/repo/pulls/42"},
		},
		"comment": map[string]any{
			"body":     "Ready to merge once CI is green.",
			"html_url": "https://github.com/org/repo/pull/42#issuecomment-1",
			"user":     map[string]any{"login": "dave"},
		},
		"repository": map[string]any{"full_name": "org/repo"},
		"sender":     map[string]any{"login": "dave"},
	})
	status, err = svc.HandleWebhook(ctx, "del-9", "issue_comment", prCommentPayload)
	require.NoError(t, err)
	assert.Equal(t, "replied", status)
	require.Len(t, messenger.actions, 9)
	assert.Equal(t, "reply", messenger.actions[8].Kind)
	assert.Equal(t, rootID, messenger.actions[8].TargetID)

	nonPRIssueComment := mustJSON(t, map[string]any{
		"action": "created",
		"issue": map[string]any{
			"number":   7,
			"title":    "Regular bug issue",
			"html_url": "https://github.com/org/repo/issues/7",
		},
		"comment":    map[string]any{"body": "Me too", "user": map[string]any{"login": "dave"}},
		"repository": map[string]any{"full_name": "org/repo"},
		"sender":     map[string]any{"login": "dave"},
	})
	status, err = svc.HandleWebhook(ctx, "del-10", "issue_comment", nonPRIssueComment)
	require.NoError(t, err)
	assert.Equal(t, "ignored", status)
	require.Len(t, messenger.actions, 9)

	// 10. PR merged -> updates root card to merged AND replies in thread
	mergePayload := mustJSON(t, map[string]any{
		"action": "closed",
		"number": 42,
		"pull_request": map[string]any{
			"number":        42,
			"title":         "feat: add PR thread aggregation",
			"html_url":      "https://github.com/org/repo/pull/42",
			"state":         "closed",
			"merged":        true,
			"merged_by":     map[string]any{"login": "alice"},
			"additions":     140,
			"deletions":     18,
			"changed_files": 5,
			"user":          map[string]any{"login": "alice"},
			"head":          map[string]any{"ref": "feat/pr-threads", "sha": "sha222"},
			"base":          map[string]any{"ref": "main"},
		},
		"repository": map[string]any{"full_name": "org/repo"},
		"sender":     map[string]any{"login": "alice"},
	})
	status, err = svc.HandleWebhook(ctx, "del-11", "pull_request", mergePayload)
	require.NoError(t, err)
	assert.Equal(t, "replied", status)
	require.Len(t, messenger.actions, 11)
	assert.Equal(t, "update", messenger.actions[9].Kind)
	assert.Equal(t, rootID, messenger.actions[9].TargetID)
	assert.Contains(t, messenger.actions[9].CardJSON, "Merged")
	assert.Equal(t, "reply", messenger.actions[10].Kind)
	assert.Equal(t, rootID, messenger.actions[10].TargetID)
}

func TestPRThreadCreatesRootCardOnFirstCommentForLegacyPR(t *testing.T) {
	store := newMemoryPRThreadStore()
	messenger := &fakeLarkMessenger{}
	svc := &PRThreadService{
		Store:     store,
		Messenger: messenger,
		ChatID:    "oc_pr_channel",
	}

	commentPayload := mustJSON(t, map[string]any{
		"action": "created",
		"issue": map[string]any{
			"number":       108,
			"title":        "fix: legacy open PR",
			"html_url":     "https://github.com/org/repo/pull/108",
			"state":        "open",
			"user":         map[string]any{"login": "alice"},
			"pull_request": map[string]any{"url": "https://api.github.com/repos/org/repo/pulls/108"},
		},
		"comment": map[string]any{
			"body":     "Ping for review",
			"html_url": "https://github.com/org/repo/pull/108#issuecomment-99",
			"user":     map[string]any{"login": "bob"},
		},
		"repository": map[string]any{"full_name": "org/repo", "html_url": "https://github.com/org/repo"},
		"sender":     map[string]any{"login": "bob"},
	})

	status, err := svc.HandleWebhook(context.Background(), "del-legacy-1", "issue_comment", commentPayload)
	require.NoError(t, err)
	assert.Equal(t, "replied", status)
	require.Len(t, messenger.actions, 2)
	assert.Equal(t, "send", messenger.actions[0].Kind)
	assert.Equal(t, "reply", messenger.actions[1].Kind)
	assert.Equal(t, messenger.actions[0].MessageID, messenger.actions[1].TargetID)
}
