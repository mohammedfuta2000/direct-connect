package directconnect

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/overmindtech/aws-source/sources"
	"github.com/overmindtech/sdp-go"
)

func connectionOutputMapper(_ context.Context, _ *directconnect.Client, scope string, _ *directconnect.DescribeConnectionsInput, output *directconnect.DescribeConnectionsOutput) ([]*sdp.Item, error) {
	items := make([]*sdp.Item, 0)

	for _, cr := range output.Connections {
		attributes, err := sources.ToAttributesCase(cr, "tags")

		if err != nil {
			return nil, err
		}

		item := sdp.Item{
			Type:            "directconnect-connection",
			UniqueAttribute: "connectionId",
			Attributes:      attributes,
			Scope:           scope,
			Tags:            tagsToMap(cr.Tags),
		}

		if cr.awsDeviceV2 != nil {
			// +overmind:link directconnect-aws-Device-V2
			item.LinkedItemQueries = append(item.LinkedItemQueries, &sdp.LinkedItemQuery{
				Query: &sdp.Query{
					ConnectionFleetId
					Type:   "directconnect-aws-Device-V2",
					Method: sdp.QueryMethod_GET,
					Query:  *cr.ConnectionFleetId,
					Scope:  scope,
				},
				BlastPropagation: &sdp.BlastPropagation{
					// Changes to the awsDevice will affect this
					In: true,
					// We can't affect the awsDevice
					Out: false,
				},
			})
		}

		if cr.awsLogicalDeviceId != nil {
			if arn, err := sources.ParseARN(*cr.OutpostArn); err == nil {
				// +overmind:link directconnect-aws-logical-device
				item.LinkedItemQueries = append(item.LinkedItemQueries, &sdp.LinkedItemQuery{
					Query: &sdp.Query{
						Type:   "directconnect-aws-logical-device",
						Method: sdp.QueryMethod_SEARCH,
						Query:  *cr.OutpostArn,
						Scope:  sources.FormatScope(arn.AccountID, arn.Region),
					},
					BlastPropagation: &sdp.BlastPropagation{
						// Changes to the outpost will affect this
						In: true,
						// We can't affect the outpost
						Out: false,
					},
				})
			}
		}

		if cr.LagId != nil {
			if arn, err := sources.ParseARN(*cr.PlacementGroupArn); err == nil {
				// +overmind:link directconnect-lag
				item.LinkedItemQueries = append(item.LinkedItemQueries, &sdp.LinkedItemQuery{
					Query: &sdp.Query{
						Type:   "directconnect-lag",
						Method: sdp.QueryMethod_SEARCH,
						Query:  *cr.PlacementGroupArn,
						Scope:  sources.FormatScope(arn.AccountID, arn.Region),
					},
					BlastPropagation: &sdp.BlastPropagation{
						// Changes to the placement group will affect this
						In: true,
						// We can't affect the placement group
						Out: false,
					},
				})
			}
		}

		if cr.VLAN != nil {
			if arn, err := sources.ParseARN(*cr.PlacementGroupArn); err == nil {
				// +overmind:link directconnect-vlan
				item.LinkedItemQueries = append(item.LinkedItemQueries, &sdp.LinkedItemQuery{
					Query: &sdp.Query{
						Type:   "directconnect-vlan",
						Method: sdp.QueryMethod_SEARCH,
						Query:  *cr.PlacementGroupArn,
						Scope:  sources.FormatScope(arn.AccountID, arn.Region),
					},
					BlastPropagation: &sdp.BlastPropagation{
						// Changes to the placement group will affect this
						In: true,
						// We can't affect the placement group
						Out: false,
					},
				})
			}
		}

		items = append(items, &item)
	}

	return items, nil
}

//go:generate docgen ../../docs-data
// +overmind:type directconnect-connection
// +overmind:descriptiveType Connection
// +overmind:get Get a connection by ID
// +overmind:list List all connections
// +overmind:search Search connections by ARN
// +overmind:group AWS
// +overmind:terraform:queryMap aws_dx_connection.id

func NewConnectionSource(config aws.Config, accountID string, limit *sources.LimitBucket) *sources.DescribeOnlySource[*directconnect.DescribeConnectionsInput, *directconnect.DescribeConnectionsOutput, *directconnect.Client, *directconnect.Options] {
	return &sources.DescribeOnlySource[*directconnect.DescribeConnectionsInput, *directconnect.DescribeConnectionsOutput, *directconnect.Client, *directconnect.Options]{
		Config:    config,
		Client:    directconnect.NewFromConfig(config),
		AccountID: accountID,
		ItemType:  "directconnect-connection",
		DescribeFunc: func(ctx context.Context, client *directconnect.Client, input *directconnect.DescribeConnectionsInput) (*directconnect.DescribeConnectionsOutput, error) {
			limit.Wait(ctx) // Wait for rate limiting // Wait for late limiting
			return client.DescribeConnections(ctx, input)
		},
		InputMapperGet: func(scope, query string) (*directconnect.DescribeConnectionsInput, error) {
			return &directconnect.DescribeConnectionsInput{
				ConnectionIds: []string{query},
			}, nil
		},
		InputMapperList: func(scope string) (*directconnect.DescribeConnectionsInput, error) {
			return &directconnect.DescribeConnectionsInput{}, nil
		},
		PaginatorBuilder: func(client *directconnect.Client, params *directconnect.DescribeConnectionsInput) sources.Paginator[*directconnect.DescribeConnectionsOutput, *directconnect.Options] {
			return directconnect.NewDescribeConnectionsPaginator(client, params)
		},
		OutputMapper: connectionOutputMapper,
	}
}