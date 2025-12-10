// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sentryexporter

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type mockSentryClient struct {
	mock.Mock
}

func (m *mockSentryClient) GetAllProjects(ctx context.Context, orgSlug string) ([]ProjectInfo, error) {
	args := m.Called(ctx, orgSlug)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]ProjectInfo), args.Error(1)
}

func (m *mockSentryClient) GetProjectKeys(ctx context.Context, orgSlug, projectSlug string) ([]ProjectKey, error) {
	args := m.Called(ctx, orgSlug, projectSlug)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]ProjectKey), args.Error(1)
}

func (m *mockSentryClient) GetOTLPEndpoints(ctx context.Context, orgSlug, projectSlug string) (*OTLPEndpoints, error) {
	args := m.Called(ctx, orgSlug, projectSlug)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*OTLPEndpoints), args.Error(1)
}

func (m *mockSentryClient) CreateProject(ctx context.Context, orgSlug, teamSlug, projectSlug, projectName, platform string) (*ProjectInfo, error) {
	args := m.Called(ctx, orgSlug, teamSlug, projectSlug, projectName, platform)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ProjectInfo), args.Error(1)
}
