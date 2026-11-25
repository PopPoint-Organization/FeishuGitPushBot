package bot

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"
)

// PRCIRun 记录单个 workflow 在当前 PR 上的最新运行状态
type PRCIRun struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	URL        string `json:"url"`
	UpdatedAt  int64  `json:"updated_at"`
}

// PRThreadRecord 保存一个 GitHub PR 与其 Lark 主卡片/话题的聚合状态
type PRThreadRecord struct {
	Repo          string             `json:"repo"`
	Number        int                `json:"number"`
	RootMessageID string             `json:"root_message_id"`
	Title         string             `json:"title"`
	Body          string             `json:"body,omitempty"`
	URL           string             `json:"url"`
	RepoURL       string             `json:"repo_url,omitempty"`
	Author        string             `json:"author,omitempty"`
	AuthorURL     string             `json:"author_url,omitempty"`
	HeadBranch    string             `json:"head_branch,omitempty"`
	BaseBranch    string             `json:"base_branch,omitempty"`
	HeadSHA       string             `json:"head_sha,omitempty"`
	State         string             `json:"state"`
	Draft         bool               `json:"draft"`
	Merged        bool               `json:"merged"`
	MergedBy      string             `json:"merged_by,omitempty"`
	Additions     int                `json:"additions"`
	Deletions     int                `json:"deletions"`
	ChangedFiles  int                `json:"changed_files"`
	Reviews       map[string]string  `json:"reviews,omitempty"`
	CIRuns        map[string]PRCIRun `json:"ci_runs,omitempty"`
	UpdatedAt     int64              `json:"updated_at"`
}

// PRThreadStore 抽象 PR 话题映射与幂等存储
type PRThreadStore interface {
	ClaimDelivery(ctx context.Context, deliveryID string, ttl time.Duration) (bool, error)
	GetPR(ctx context.Context, repo string, number int) (*PRThreadRecord, error)
	SavePR(ctx context.Context, rec *PRThreadRecord) error
	FindPRNumberBySHA(ctx context.Context, repo, sha string) (int, error)
	FindPRNumberByBranch(ctx context.Context, repo, branch string) (int, error)
}

// LarkMessenger 抽象飞书/Lark 消息发送、原地更新与话题回复能力
type LarkMessenger interface {
	SendToChat(chatID string, card *Card) (string, error)
	UpdateMessage(messageID string, card *Card) error
	ReplyToMessage(parentID string, card *Card) (string, error)
}

// PRThreadService 实现「一 PR 一话题」聚合同步（单向 GitHub -> Lark）
type PRThreadService struct {
	Store     PRThreadStore
	Messenger LarkMessenger
	ChatID    string
}

// HandleWebhook 同步处理 GitHub Webhook，返回处理状态（sent / updated / replied / duplicate / ignored）
func (s *PRThreadService) HandleWebhook(ctx context.Context, deliveryID, eventType string, payload []byte) (string, error) {
	if s.Store != nil && deliveryID != "" {
		claimed, err := s.Store.ClaimDelivery(ctx, deliveryID, 7*24*time.Hour)
		if err != nil {
			return "", fmt.Errorf("claim delivery: %w", err)
		}
		if !claimed {
			return "duplicate", nil
		}
	}

	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return "", fmt.Errorf("unmarshal webhook payload: %w", err)
	}

	switch eventType {
	case "pull_request":
		return s.handlePullRequest(ctx, m)
	case "pull_request_review":
		return s.handlePullRequestReview(ctx, m)
	case "pull_request_review_comment":
		return s.handlePullRequestReviewComment(ctx, m)
	case "issue_comment":
		return s.handleIssueComment(ctx, m)
	case "workflow_run":
		return s.handleWorkflowRun(ctx, m)
	default:
		return "ignored", nil
	}
}

func (s *PRThreadService) handlePullRequest(ctx context.Context, m map[string]any) (string, error) {
	action := ext(m, "action")
	switch action {
	case "opened", "reopened", "synchronize", "edited", "ready_for_review", "converted_to_draft", "closed", "review_requested":
	default:
		return "ignored", nil
	}

	repo := ext(m, "repository", "full_name")
	number := extInt(m, "number")
	if number == 0 {
		number = extInt(m, "pull_request", "number")
	}
	if repo == "" || number == 0 {
		return "ignored", nil
	}

	rec, err := s.Store.GetPR(ctx, repo, number)
	if err != nil {
		return "", err
	}
	isNew := rec == nil || rec.RootMessageID == ""
	if rec == nil {
		rec = &PRThreadRecord{
			Repo:    repo,
			Number:  number,
			Reviews: map[string]string{},
			CIRuns:  map[string]PRCIRun{},
		}
	}

	applyPRFieldsFromMap(rec, m)
	rec.UpdatedAt = time.Now().Unix()

	if isNew {
		card := BuildPRMasterCard(rec)
		msgID, err := s.Messenger.SendToChat(s.ChatID, card)
		if err != nil {
			return "", err
		}
		rec.RootMessageID = msgID
		if err := s.Store.SavePR(ctx, rec); err != nil {
			return "", err
		}
		return "sent", nil
	}

	if err := s.Store.SavePR(ctx, rec); err != nil {
		return "", err
	}
	if err := s.Messenger.UpdateMessage(rec.RootMessageID, BuildPRMasterCard(rec)); err != nil {
		return "", err
	}

	// 仅对关键里程碑在话题内追加精简动态；edited / synchronize 只静默更新主卡片不刷屏
	var replyCard *Card
	sender := ext(m, "sender", "login")
	switch action {
	case "closed":
		if rec.Merged {
			by := rec.MergedBy
			if by == "" {
				by = sender
			}
			replyCard = buildPRThreadReplyCard("🟣 PR 已合并", "purple", fmt.Sprintf("**%s** 合并了该 PR 到 `%s`。", by, rec.BaseBranch), rec.URL, "查看 PR")
		} else {
			replyCard = buildPRThreadReplyCard("⚪ PR 已关闭", "grey", fmt.Sprintf("**%s** 关闭了该 PR。", sender), rec.URL, "查看 PR")
		}
	case "reopened":
		replyCard = buildPRThreadReplyCard("🔵 PR 已重新打开", "blue", fmt.Sprintf("**%s** 重新打开了该 PR。", sender), rec.URL, "查看 PR")
	case "ready_for_review":
		replyCard = buildPRThreadReplyCard("👀 PR 已准备就绪", "blue", fmt.Sprintf("**%s** 将该 PR 标记为 Ready for review。", sender), rec.URL, "去 Review")
	case "review_requested":
		reviewer := ext(m, "requested_reviewer", "login")
		if reviewer == "" {
			reviewer = ext(m, "requested_team", "name")
		}
		if reviewer != "" {
			replyCard = buildPRThreadReplyCard("👀 请求 Review", "blue", fmt.Sprintf("**%s** 请求 **%s** Review 该 PR。", sender, reviewer), rec.URL, "去 Review")
		}
	}

	if replyCard != nil {
		if _, err := s.Messenger.ReplyToMessage(rec.RootMessageID, replyCard); err != nil {
			return "", err
		}
		return "replied", nil
	}
	return "updated", nil
}

func (s *PRThreadService) handlePullRequestReview(ctx context.Context, m map[string]any) (string, error) {
	if ext(m, "action") != "submitted" {
		return "ignored", nil
	}

	state := strings.ToUpper(strings.TrimSpace(ext(m, "review", "state")))
	body := strings.TrimSpace(ext(m, "review", "body"))
	// GitHub 提交行内代码评审时会伴随一个 body 为空的 commented review 容器，跳过以避免重复刷屏
	if state == "COMMENTED" && body == "" {
		return "ignored", nil
	}
	if state == "" {
		return "ignored", nil
	}

	repo := ext(m, "repository", "full_name")
	number := extInt(m, "pull_request", "number")
	if repo == "" || number == 0 {
		return "ignored", nil
	}

	rec, err := s.ensurePRRootCard(ctx, repo, number, m, false)
	if err != nil {
		return "", err
	}

	reviewer := ext(m, "review", "user", "login")
	if reviewer == "" {
		reviewer = ext(m, "sender", "login")
	}
	if rec.Reviews == nil {
		rec.Reviews = map[string]string{}
	}
	if state == "APPROVED" || state == "CHANGES_REQUESTED" || rec.Reviews[reviewer] == "" {
		rec.Reviews[reviewer] = state
	}
	rec.UpdatedAt = time.Now().Unix()

	if err := s.Store.SavePR(ctx, rec); err != nil {
		return "", err
	}
	if err := s.Messenger.UpdateMessage(rec.RootMessageID, BuildPRMasterCard(rec)); err != nil {
		return "", err
	}

	title, color := "💬 PR Review", "blue"
	switch state {
	case "APPROVED":
		title, color = fmt.Sprintf("✅ %s Approved", reviewer), "green"
	case "CHANGES_REQUESTED":
		title, color = fmt.Sprintf("🔴 %s Changes Requested", reviewer), "orange"
	default:
		title, color = fmt.Sprintf("💬 %s Reviewed", reviewer), "blue"
	}

	content := body
	if content == "" {
		content = fmt.Sprintf("**%s** 提交了 `%s`。", reviewer, state)
	} else {
		content = fmt.Sprintf("**%s**:\n%s", reviewer, truncateText(content, 1500))
	}
	reviewURL := ext(m, "review", "html_url")
	if reviewURL == "" {
		reviewURL = rec.URL
	}

	if _, err := s.Messenger.ReplyToMessage(rec.RootMessageID, buildPRThreadReplyCard(title, color, content, reviewURL, "查看 Review")); err != nil {
		return "", err
	}
	return "replied", nil
}

func (s *PRThreadService) handlePullRequestReviewComment(ctx context.Context, m map[string]any) (string, error) {
	if ext(m, "action") != "created" {
		return "ignored", nil
	}

	repo := ext(m, "repository", "full_name")
	number := extInt(m, "pull_request", "number")
	if repo == "" || number == 0 {
		return "ignored", nil
	}

	rec, err := s.ensurePRRootCard(ctx, repo, number, m, false)
	if err != nil {
		return "", err
	}

	author := ext(m, "comment", "user", "login")
	if author == "" {
		author = ext(m, "sender", "login")
	}
	path := ext(m, "comment", "path")
	line := extInt(m, "comment", "line")
	if line == 0 {
		line = extInt(m, "comment", "original_line")
	}
	loc := path
	if path != "" && line > 0 {
		loc = fmt.Sprintf("%s:%d", path, line)
	}

	body := truncateText(strings.TrimSpace(ext(m, "comment", "body")), 1500)
	var content string
	if loc != "" {
		content = fmt.Sprintf("**%s** 评论了 `%s`\n\n%s", author, loc, body)
	} else {
		content = fmt.Sprintf("**%s**:\n\n%s", author, body)
	}

	commentURL := ext(m, "comment", "html_url")
	if commentURL == "" {
		commentURL = rec.URL
	}

	card := buildPRThreadReplyCard(fmt.Sprintf("📝 代码行评论 · %s", author), "wathet", content, commentURL, "查看代码位置")
	if _, err := s.Messenger.ReplyToMessage(rec.RootMessageID, card); err != nil {
		return "", err
	}
	return "replied", nil
}

func (s *PRThreadService) handleIssueComment(ctx context.Context, m map[string]any) (string, error) {
	if ext(m, "action") != "created" {
		return "ignored", nil
	}
	// PR channel 仅同步 PR 下的评论，忽略普通 Issue 评论
	if !hasNestedField(m, "issue", "pull_request") {
		return "ignored", nil
	}

	repo := ext(m, "repository", "full_name")
	number := extInt(m, "issue", "number")
	if repo == "" || number == 0 {
		return "ignored", nil
	}

	rec, err := s.ensurePRRootCard(ctx, repo, number, m, true)
	if err != nil {
		return "", err
	}

	author := ext(m, "comment", "user", "login")
	if author == "" {
		author = ext(m, "sender", "login")
	}
	body := truncateText(strings.TrimSpace(ext(m, "comment", "body")), 1500)
	commentURL := ext(m, "comment", "html_url")
	if commentURL == "" {
		commentURL = rec.URL
	}

	card := buildPRThreadReplyCard(fmt.Sprintf("💬 %s 评论了 PR", author), "blue", body, commentURL, "查看评论")
	if _, err := s.Messenger.ReplyToMessage(rec.RootMessageID, card); err != nil {
		return "", err
	}
	return "replied", nil
}

func (s *PRThreadService) handleWorkflowRun(ctx context.Context, m map[string]any) (string, error) {
	repo := ext(m, "repository", "full_name")
	if repo == "" {
		return "ignored", nil
	}

	prNumber := extractWorkflowPRNumber(m)
	headSHA := ext(m, "workflow_run", "head_sha")
	headBranch := ext(m, "workflow_run", "head_branch")

	if prNumber == 0 && headSHA != "" {
		n, err := s.Store.FindPRNumberBySHA(ctx, repo, headSHA)
		if err != nil {
			return "", err
		}
		prNumber = n
	}
	if prNumber == 0 && headBranch != "" {
		n, err := s.Store.FindPRNumberByBranch(ctx, repo, headBranch)
		if err != nil {
			return "", err
		}
		prNumber = n
	}
	if prNumber == 0 {
		return "ignored", nil
	}

	rec, err := s.Store.GetPR(ctx, repo, prNumber)
	if err != nil {
		return "", err
	}
	if rec == nil || rec.RootMessageID == "" {
		return "ignored", nil
	}

	wfName := ext(m, "workflow_run", "name")
	if wfName == "" {
		wfName = "CI"
	}
	wfStatus := ext(m, "workflow_run", "status")
	wfConclusion := ext(m, "workflow_run", "conclusion")
	wfURL := ext(m, "workflow_run", "html_url")

	if rec.CIRuns == nil {
		rec.CIRuns = map[string]PRCIRun{}
	}
	rec.CIRuns[wfName] = PRCIRun{
		Name:       wfName,
		Status:     wfStatus,
		Conclusion: wfConclusion,
		URL:        wfURL,
		UpdatedAt:  time.Now().Unix(),
	}
	rec.UpdatedAt = time.Now().Unix()

	if err := s.Store.SavePR(ctx, rec); err != nil {
		return "", err
	}
	if err := s.Messenger.UpdateMessage(rec.RootMessageID, BuildPRMasterCard(rec)); err != nil {
		return "", err
	}

	// 仅在 CI 失败时在话题中追加提醒，其他状态（queued / in_progress / success）只静默更新主卡片
	if wfStatus == "completed" && (wfConclusion == "failure" || wfConclusion == "timed_out") {
		content := fmt.Sprintf("Workflow **%s** 运行失败（`%s`）。", wfName, wfConclusion)
		card := buildPRThreadReplyCard(fmt.Sprintf("❌ CI 失败 · %s", wfName), "red", content, wfURL, "查看日志")
		if _, err := s.Messenger.ReplyToMessage(rec.RootMessageID, card); err != nil {
			return "", err
		}
		return "replied", nil
	}
	return "updated", nil
}

// ensurePRRootCard 确保已有 PR 主卡片；如果这是接入前就已存在的老 PR，自动补发一张主卡片
func (s *PRThreadService) ensurePRRootCard(ctx context.Context, repo string, number int, m map[string]any, fromIssue bool) (*PRThreadRecord, error) {
	rec, err := s.Store.GetPR(ctx, repo, number)
	if err != nil {
		return nil, err
	}
	if rec != nil && rec.RootMessageID != "" {
		return rec, nil
	}

	rec = &PRThreadRecord{
		Repo:      repo,
		Number:    number,
		RepoURL:   ext(m, "repository", "html_url"),
		State:     "open",
		Reviews:   map[string]string{},
		CIRuns:    map[string]PRCIRun{},
		UpdatedAt: time.Now().Unix(),
	}
	if fromIssue {
		rec.Title = ext(m, "issue", "title")
		rec.URL = ext(m, "issue", "html_url")
		rec.Author = ext(m, "issue", "user", "login")
		rec.AuthorURL = ext(m, "issue", "user", "html_url")
		if st := ext(m, "issue", "state"); st != "" {
			rec.State = st
		}
	} else {
		applyPRFieldsFromMap(rec, m)
	}
	if rec.Title == "" {
		rec.Title = fmt.Sprintf("Pull Request #%d", number)
	}
	if rec.URL == "" {
		rec.URL = fmt.Sprintf("https://github.com/%s/pull/%d", repo, number)
	}

	msgID, err := s.Messenger.SendToChat(s.ChatID, BuildPRMasterCard(rec))
	if err != nil {
		return nil, err
	}
	rec.RootMessageID = msgID
	if err := s.Store.SavePR(ctx, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// BuildPRMasterCard 构建单个 PR 的聚合主卡片（原地更新，展示最新状态、Review 与 CI）
func BuildPRMasterCard(rec *PRThreadRecord) *Card {
	card := NewCard()

	badge, template := prMasterStatus(rec)
	card.Header.Title = CardText{
		Tag:     "plain_text",
		Content: fmt.Sprintf("%s · #%d %s", badge, rec.Number, rec.Title),
	}
	card.Header.Template = template

	subtitle := rec.Repo
	if rec.HeadBranch != "" && rec.BaseBranch != "" {
		subtitle = fmt.Sprintf("%s  |  %s → %s", rec.Repo, rec.HeadBranch, rec.BaseBranch)
	}
	card.Header.Subtitle = &CardText{Tag: "plain_text", Content: subtitle}

	var metaParts []string
	if rec.Author != "" {
		if rec.AuthorURL != "" {
			metaParts = append(metaParts, fmt.Sprintf("**作者:** [%s](%s)", rec.Author, rec.AuthorURL))
		} else {
			metaParts = append(metaParts, fmt.Sprintf("**作者:** %s", rec.Author))
		}
	}
	if rec.ChangedFiles > 0 || rec.Additions > 0 || rec.Deletions > 0 {
		metaParts = append(metaParts, fmt.Sprintf("**变更:** +%d / -%d (%d files)", rec.Additions, rec.Deletions, rec.ChangedFiles))
	}
	if rec.Merged && rec.MergedBy != "" {
		metaParts = append(metaParts, fmt.Sprintf("**合并人:** %s", rec.MergedBy))
	}
	if len(metaParts) > 0 {
		card.AddMarkdown(strings.Join(metaParts, "  ·  "))
	}

	if body := strings.TrimSpace(rec.Body); body != "" {
		card.AddMarkdown(truncateText(body, 600))
	}

	card.AddDivider()
	card.AddMarkdown(fmt.Sprintf("**Review:** %s", formatReviewsSummary(rec.Reviews)))
	card.AddMarkdown(fmt.Sprintf("**CI:** %s", formatCIRunsSummary(rec.CIRuns)))

	if rec.URL != "" {
		card.AddActions("bisect",
			ActionButton{Text: "查看 PR", URL: rec.URL, Type: "primary"},
			ActionButton{Text: "Files Changed", URL: strings.TrimRight(rec.URL, "/") + "/files", Type: "default"},
		)
	}
	card.AddNote("💬 请直接在此话题下讨论该 PR；后续 Review、评论与 CI 将自动同步至此话题。")
	return card
}

func prMasterStatus(rec *PRThreadRecord) (string, string) {
	if rec.Merged {
		return "🟣 Merged", "purple"
	}
	if rec.State == "closed" {
		return "⚪ Closed", "grey"
	}
	if rec.Draft {
		return "📝 Draft", "grey"
	}
	hasApproved := false
	for _, st := range rec.Reviews {
		if st == "CHANGES_REQUESTED" {
			return "🟠 Changes Requested", "orange"
		}
		if st == "APPROVED" {
			hasApproved = true
		}
	}
	if hasApproved {
		return "🟢 Approved", "green"
	}
	return "🔵 Open", "blue"
}

func formatReviewsSummary(reviews map[string]string) string {
	if len(reviews) == 0 {
		return "⏳ 待 Review"
	}
	names := make([]string, 0, len(reviews))
	for k := range reviews {
		names = append(names, k)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		icon := "💬"
		switch reviews[name] {
		case "APPROVED":
			icon = "✅"
		case "CHANGES_REQUESTED":
			icon = "🔴"
		}
		parts = append(parts, fmt.Sprintf("%s %s", icon, name))
	}
	return strings.Join(parts, "  ")
}

func formatCIRunsSummary(runs map[string]PRCIRun) string {
	if len(runs) == 0 {
		return "⏳ 暂无记录"
	}
	names := make([]string, 0, len(runs))
	for k := range runs {
		names = append(names, k)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		r := runs[name]
		icon := "🔄"
		if r.Status == "completed" {
			switch r.Conclusion {
			case "success":
				icon = "✅"
			case "failure", "timed_out":
				icon = "❌"
			case "cancelled", "skipped":
				icon = "⚪"
			default:
				icon = "⚠️"
			}
		}
		label := fmt.Sprintf("%s %s", icon, r.Name)
		if r.URL != "" {
			label = fmt.Sprintf("%s [%s](%s)", icon, r.Name, r.URL)
		}
		parts = append(parts, label)
	}
	return strings.Join(parts, "  ·  ")
}

func buildPRThreadReplyCard(title, template, content, url, btnText string) *Card {
	card := NewCard()
	card.Header.Title = CardText{Tag: "plain_text", Content: title}
	card.Header.Template = template
	if content != "" {
		card.AddMarkdown(content)
	}
	if url != "" {
		card.AddActions("flow", ActionButton{Text: btnText, URL: url, Type: "default"})
	}
	return card
}

func applyPRFieldsFromMap(rec *PRThreadRecord, m map[string]any) {
	if v := ext(m, "repository", "html_url"); v != "" {
		rec.RepoURL = v
	}
	if v := ext(m, "pull_request", "title"); v != "" {
		rec.Title = v
	}
	if v := ext(m, "pull_request", "body"); v != "" {
		rec.Body = v
	}
	if v := ext(m, "pull_request", "html_url"); v != "" {
		rec.URL = v
	}
	if v := ext(m, "pull_request", "user", "login"); v != "" {
		rec.Author = v
	}
	if v := ext(m, "pull_request", "user", "html_url"); v != "" {
		rec.AuthorURL = v
	}
	if v := ext(m, "pull_request", "head", "ref"); v != "" {
		rec.HeadBranch = v
	}
	if v := ext(m, "pull_request", "head", "sha"); v != "" {
		rec.HeadSHA = v
	}
	if v := ext(m, "pull_request", "base", "ref"); v != "" {
		rec.BaseBranch = v
	}
	if v := ext(m, "pull_request", "state"); v != "" {
		rec.State = v
	}
	if b, ok := extBool(m, "pull_request", "draft"); ok {
		rec.Draft = b
	}
	if b, ok := extBool(m, "pull_request", "merged"); ok {
		rec.Merged = b
	}
	if v := ext(m, "pull_request", "merged_by", "login"); v != "" {
		rec.MergedBy = v
	}
	if n := extInt(m, "pull_request", "additions"); n > 0 || hasNestedField(m, "pull_request", "additions") {
		rec.Additions = n
	}
	if n := extInt(m, "pull_request", "deletions"); n > 0 || hasNestedField(m, "pull_request", "deletions") {
		rec.Deletions = n
	}
	if n := extInt(m, "pull_request", "changed_files"); n > 0 || hasNestedField(m, "pull_request", "changed_files") {
		rec.ChangedFiles = n
	}
}

func extractWorkflowPRNumber(m map[string]any) int {
	wf, ok := m["workflow_run"].(map[string]any)
	if !ok {
		return 0
	}
	prs, ok := wf["pull_requests"].([]any)
	if !ok || len(prs) == 0 {
		return 0
	}
	first, ok := prs[0].(map[string]any)
	if !ok {
		return 0
	}
	return extInt(first, "number")
}

func extInt(m map[string]any, keys ...string) int {
	s := ext(m, keys...)
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

func extBool(m map[string]any, keys ...string) (bool, bool) {
	var cur any = m
	for _, k := range keys {
		cm, ok := cur.(map[string]any)
		if !ok {
			return false, false
		}
		cur, ok = cm[k]
		if !ok {
			return false, false
		}
	}
	b, ok := cur.(bool)
	return b, ok
}

func hasNestedField(m map[string]any, keys ...string) bool {
	var cur any = m
	for _, k := range keys {
		cm, ok := cur.(map[string]any)
		if !ok {
			return false
		}
		val, exists := cm[k]
		if !exists || val == nil {
			return false
		}
		cur = val
	}
	return true
}

func truncateText(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "\n..."
}

// defaultLarkMessenger 将 PRThreadService 接到现有飞书/Lark SDK 客户端
type defaultLarkMessenger struct{}

func (defaultLarkMessenger) SendToChat(chatID string, card *Card) (string, error) {
	return SendToChat(chatID, card)
}

func (defaultLarkMessenger) UpdateMessage(messageID string, card *Card) error {
	return UpdateMessage(messageID, card)
}

func (defaultLarkMessenger) ReplyToMessage(parentID string, card *Card) (string, error) {
	return ReplyToMessage(parentID, card)
}

var PRThreadSvc *PRThreadService

// InitPRThreadService 在配置了 DYNAMODB_TABLE 时启用一 PR 一话题聚合服务
func InitPRThreadService(ctx context.Context) {
	table := strings.TrimSpace(C.DynamoDB.Table)
	if table == "" {
		return
	}
	store, err := NewDynamoDBPRThreadStore(ctx, table)
	if err != nil {
		slog.Error("Failed to initialize DynamoDB PR thread store", "table", table, "error", err)
		panic(err)
	}
	PRThreadSvc = &PRThreadService{
		Store:     store,
		Messenger: defaultLarkMessenger{},
		ChatID:    C.Feishu.ChatID,
	}
	slog.Info("PR thread aggregation enabled (DynamoDB)", "table", table)
}
