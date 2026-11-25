package bot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DynamoDBPRThreadStore 使用单表 DynamoDB 持久化 PR 主卡片映射与 delivery 去重
type DynamoDBPRThreadStore struct {
	client *dynamodb.Client
	table  string
}

func NewDynamoDBPRThreadStore(ctx context.Context, table string) (*DynamoDBPRThreadStore, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return &DynamoDBPRThreadStore{
		client: dynamodb.NewFromConfig(cfg),
		table:  table,
	}, nil
}

func (s *DynamoDBPRThreadStore) ClaimDelivery(ctx context.Context, deliveryID string, ttl time.Duration) (bool, error) {
	if deliveryID == "" {
		return true, nil
	}
	exp := time.Now().Add(ttl).Unix()
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item: map[string]types.AttributeValue{
			"pk":  &types.AttributeValueMemberS{Value: "delivery#" + deliveryID},
			"ttl": &types.AttributeValueMemberN{Value: strconv.FormatInt(exp, 10)},
		},
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
	})
	if err != nil {
		var cce *types.ConditionalCheckFailedException
		if errors.As(err, &cce) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *DynamoDBPRThreadStore) GetPR(ctx context.Context, repo string, number int) (*PRThreadRecord, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("pr#%s#%d", repo, number)},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, nil
	}
	dataAttr, ok := out.Item["data"].(*types.AttributeValueMemberS)
	if !ok || dataAttr.Value == "" {
		return nil, nil
	}
	var rec PRThreadRecord
	if err := json.Unmarshal([]byte(dataAttr.Value), &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (s *DynamoDBPRThreadStore) SavePR(ctx context.Context, rec *PRThreadRecord) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	exp := time.Now().Add(180 * 24 * time.Hour).Unix()
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item: map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: fmt.Sprintf("pr#%s#%d", rec.Repo, rec.Number)},
			"data": &types.AttributeValueMemberS{Value: string(raw)},
			"ttl":  &types.AttributeValueMemberN{Value: strconv.FormatInt(exp, 10)},
		},
	})
	if err != nil {
		return err
	}

	idxTTL := strconv.FormatInt(time.Now().Add(90*24*time.Hour).Unix(), 10)
	prNumStr := strconv.Itoa(rec.Number)
	if rec.HeadSHA != "" {
		_, _ = s.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(s.table),
			Item: map[string]types.AttributeValue{
				"pk":        &types.AttributeValueMemberS{Value: fmt.Sprintf("sha#%s#%s", rec.Repo, rec.HeadSHA)},
				"pr_number": &types.AttributeValueMemberN{Value: prNumStr},
				"ttl":       &types.AttributeValueMemberN{Value: idxTTL},
			},
		})
	}
	if rec.HeadBranch != "" {
		_, _ = s.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(s.table),
			Item: map[string]types.AttributeValue{
				"pk":        &types.AttributeValueMemberS{Value: fmt.Sprintf("branch#%s#%s", rec.Repo, rec.HeadBranch)},
				"pr_number": &types.AttributeValueMemberN{Value: prNumStr},
				"ttl":       &types.AttributeValueMemberN{Value: idxTTL},
			},
		})
	}
	return nil
}

func (s *DynamoDBPRThreadStore) FindPRNumberBySHA(ctx context.Context, repo, sha string) (int, error) {
	return s.getIndexPRNumber(ctx, fmt.Sprintf("sha#%s#%s", repo, sha))
}

func (s *DynamoDBPRThreadStore) FindPRNumberByBranch(ctx context.Context, repo, branch string) (int, error) {
	return s.getIndexPRNumber(ctx, fmt.Sprintf("branch#%s#%s", repo, branch))
}

func (s *DynamoDBPRThreadStore) getIndexPRNumber(ctx context.Context, pk string) (int, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
		},
	})
	if err != nil || out.Item == nil {
		return 0, err
	}
	numAttr, ok := out.Item["pr_number"].(*types.AttributeValueMemberN)
	if !ok {
		return 0, nil
	}
	n, err := strconv.Atoi(numAttr.Value)
	if err != nil {
		return 0, nil
	}
	return n, nil
}
