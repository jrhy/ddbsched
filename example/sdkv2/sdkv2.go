package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	ddbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/jrhy/ddbsched"
)

var table = "ddbsched"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

type persister struct {
	client *ddbv2.Client

	ctx context.Context
}

func (p *persister) UpdateMetrics(tcu ddbsched.Metrics, pt ddbsched.ProvisionedThroughput) {
	log.Printf("current provisioned throughput: %+v\n", pt)
	log.Printf("new metrics: %+v\n", tcu)
}

func (p *persister) UpdateCountersItem(updateExpression string, eavs map[string]uint64) (ddbsched.StateItem, bool) {
	res, err := p.client.UpdateItem(p.ctx, &ddbv2.UpdateItemInput{TableName: &table,
		ExpressionAttributeValues: toEAV(eavs),
		Key: map[string]types.AttributeValue{
			"P": &types.AttributeValueMemberS{Value: "ddbsched"},
			"S": &types.AttributeValueMemberS{Value: "current"},
		},
		ReturnValues:     "ALL_NEW",
		UpdateExpression: &updateExpression,
	})
	var s ddbsched.StateItem
	if err != nil {
		log.Printf("update ddbsched state: %v", err)
		return s, false
	}
	err = attributevalue.UnmarshalMap(res.Attributes, &s)
	if err != nil {
		log.Printf("unable to unmarshal ddbsched state: %v", err)
		return s, false
	}
	log.Printf("updated OK: %s; now: %+v\n", updateExpression, s)
	return s, true
}

func toEAV(eav map[string]uint64) map[string]types.AttributeValue {
	res := make(map[string]types.AttributeValue)
	for k, v := range eav {
		res[k] = &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}
	}
	return res
}

func (p *persister) GetProvisionedThroughput() (ddbsched.ProvisionedThroughput, bool) {
	o, err := p.client.DescribeTable(p.ctx, &ddbv2.DescribeTableInput{TableName: &table})
	if err != nil {
		return ddbsched.ProvisionedThroughput{}, false
	}
	return ddbsched.ProvisionedThroughput{
		RCU: uint64(*o.Table.ProvisionedThroughput.ReadCapacityUnits),
		WCU: uint64(*o.Table.ProvisionedThroughput.WriteCapacityUnits),
	}, true
}

func run() error {
	sess := session.Must(session.NewSession(&aws.Config{
		Endpoint: aws.String("http://localhost:8000"),
	}))

	dc := dynamodb.New(sess)
	fmt.Println(dc)
	{
		o, err := dc.ListTables(&dynamodb.ListTablesInput{})
		if err != nil {
			return fmt.Errorf("listTables: %w", err)
		}
		fmt.Printf("%+v\n", o)
	}
	{
		o, err := dc.DescribeTable(&dynamodb.DescribeTableInput{TableName: &table})
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeResourceNotFoundException:
				_, err = dc.CreateTable(&dynamodb.CreateTableInput{
					AttributeDefinitions: []*dynamodb.AttributeDefinition{
						{AttributeName: aws.String("P"), AttributeType: aws.String("S")},
						{AttributeName: aws.String("S"), AttributeType: aws.String("S")},
					},
					BillingMode: aws.String(dynamodb.BillingModeProvisioned),
					KeySchema: []*dynamodb.KeySchemaElement{
						{AttributeName: aws.String("P"), KeyType: aws.String("HASH")},
						{AttributeName: aws.String("S"), KeyType: aws.String("RANGE")},
					},
					ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(2),
						WriteCapacityUnits: aws.Int64(2),
					},
					TableName: &table,
				})
				if err != nil {
					return fmt.Errorf("createTable: %w", err)
				}
				var o2 *dynamodb.DescribeTableOutput
				for {
					o2, err = dc.DescribeTable(&dynamodb.DescribeTableInput{TableName: &table})
					if err != nil {
						return fmt.Errorf("describeTable: %w", err)
					}
					if *o2.Table.TableStatus == "ACTIVE" {
						break
					}
					fmt.Printf("polling for table creation, status %s...\n", *o2.Table.TableStatus)
					time.Sleep(time.Second)
				}
			}
		}
		if err != nil {
			return fmt.Errorf("describeTable: %w", err)
		}
		fmt.Printf("%+v\n", o)
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return fmt.Errorf("aws-sdk-v2: %w", err)
	}
	var v2Client *ddbv2.Client = ddbv2.NewFromConfig(cfg, func(o *ddbv2.Options) {
		o.BaseEndpoint = aws.String("http://localhost:8000/")
	})
	sched := ddbsched.New(
		&persister{client: v2Client, ctx: context.Background()},
		3*time.Second)
	time.Sleep(4 * time.Second)
	sched.UsedWCU(15)
	time.Sleep(8 * time.Second)
	if !sched.WantWCU(15) {
		panic("i should be able to have some now")
	}
	time.Sleep(15 * time.Second)
	return nil
}
