// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sentryexporter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetProjectKeys(t *testing.T) {
	mockKeys := []ProjectKey{
		{
			ID:        "1",
			Name:      "Default",
			Public:    "public_key_123",
			ProjectID: 12345,
			IsActive:  true,
			DSN: DSNField{
				Public: "https://public_key_123@o123456.ingest.sentry.io/7654321",
			},
		},
	}

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/0/projects/my-org/my-project/keys/", r.URL.Path)
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(mockKeys)
	}))
	defer testServer.Close()

	client := NewSentryClient(testServer.URL, "test-token", nil)
	keys, err := client.GetProjectKeys(context.Background(), "my-org", "my-project")

	require.NoError(t, err)
	assert.Len(t, keys, 1)
	assert.Equal(t, "1", keys[0].ID)
	assert.Equal(t, "public_key_123", keys[0].Public)
	assert.True(t, keys[0].IsActive)
}

func TestGetProjectKeysError(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("Unauthorized"))
	}))
	defer testServer.Close()

	client := NewSentryClient(testServer.URL, "invalid-token", nil)
	_, err := client.GetProjectKeys(context.Background(), "my-org", "my-project")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "401")
}

func TestGetOTLPEndpoints(t *testing.T) {
	mockKeys := []ProjectKey{
		{
			ID:        "1",
			Name:      "Default",
			Public:    "public_key_123",
			ProjectID: 12345,
			IsActive:  true,
			DSN: DSNField{
				Public: "https://public_key_123@o123456.ingest.sentry.io/7654321",
			},
		},
	}

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(mockKeys)
	}))
	defer testServer.Close()

	client := NewSentryClient(testServer.URL, "test-token", nil)
	endpoints, err := client.GetOTLPEndpoints(context.Background(), "my-org", "my-project")

	require.NoError(t, err)
	assert.NotNil(t, endpoints)
	assert.Equal(t, "public_key_123", endpoints.PublicKey)
	assert.Contains(t, endpoints.TracesURL, "7654321")
	assert.Contains(t, endpoints.LogsURL, "7654321")
}

func TestGetOTLPEndpointsNoKeys(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode([]ProjectKey{})
	}))
	defer testServer.Close()

	client := NewSentryClient(testServer.URL, "test-token", nil)
	_, err := client.GetOTLPEndpoints(context.Background(), "my-org", "my-project")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no project keys found")
}

func TestGetOTLPEndpointsNoActiveKeys(t *testing.T) {
	mockKeys := []ProjectKey{
		{
			ID:        "1",
			Name:      "Inactive",
			Public:    "public_key_123",
			ProjectID: 12345,
			IsActive:  false,
			DSN: DSNField{
				Public: "https://public_key_123@o123456.ingest.sentry.io/7654321",
			},
		},
	}

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(mockKeys)
	}))
	defer testServer.Close()

	client := NewSentryClient(testServer.URL, "test-token", nil)
	_, err := client.GetOTLPEndpoints(context.Background(), "my-org", "my-project")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no active project keys found")
}

func TestGetAllProjects(t *testing.T) {
	mockProjects := []ProjectInfo{
		{
			ID:   "1",
			Slug: "project-one",
			Name: "Project One",
			Team: TeamInfo{
				ID:   "team1",
				Slug: "team-one",
				Name: "Team One",
			},
			Teams: []TeamInfo{
				{
					ID:   "team1",
					Slug: "team-one",
					Name: "Team One",
				},
			},
		},
		{
			ID:   "2",
			Slug: "project-two",
			Name: "Project Two",
			Team: TeamInfo{
				ID:   "team2",
				Slug: "team-two",
				Name: "Team Two",
			},
			Teams: []TeamInfo{
				{
					ID:   "team2",
					Slug: "team-two",
					Name: "Team Two",
				},
			},
		},
	}

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/0/organizations/my-org/projects/", r.URL.Path)
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		assert.Equal(t, http.MethodGet, r.Method)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(mockProjects)
	}))
	defer testServer.Close()

	client := NewSentryClient(testServer.URL, "test-token", nil)
	projects, err := client.GetAllProjects(context.Background(), "my-org")

	require.NoError(t, err)
	assert.Len(t, projects, 2)
	assert.Equal(t, "1", projects[0].ID)
	assert.Equal(t, "project-one", projects[0].Slug)
	assert.Equal(t, "Project One", projects[0].Name)
	assert.Equal(t, "team-one", projects[0].Team.Slug)
	assert.Equal(t, "2", projects[1].ID)
	assert.Equal(t, "project-two", projects[1].Slug)
}

func TestGetAllProjectsError(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("Forbidden"))
	}))
	defer testServer.Close()

	client := NewSentryClient(testServer.URL, "invalid-token", nil)
	_, err := client.GetAllProjects(context.Background(), "my-org")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "403")
}

func TestGetAllProjectsEmpty(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode([]ProjectInfo{})
	}))
	defer testServer.Close()

	client := NewSentryClient(testServer.URL, "test-token", nil)
	projects, err := client.GetAllProjects(context.Background(), "my-org")

	require.NoError(t, err)
	assert.Len(t, projects, 0)
}

func TestCreateProject(t *testing.T) {
	mockProject := ProjectInfo{
		ID:   "123",
		Slug: "my-new-project",
		Name: "My New Project",
		Team: TeamInfo{
			ID:   "team1",
			Slug: "my-team",
			Name: "My Team",
		},
		Teams: []TeamInfo{
			{
				ID:   "team1",
				Slug: "my-team",
				Name: "My Team",
			},
		},
	}

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/0/teams/my-org/my-team/projects/", r.URL.Path)
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, http.MethodPost, r.Method)

		// Validate request body
		var payload map[string]string
		err := json.NewDecoder(r.Body).Decode(&payload)
		require.NoError(t, err)
		assert.Equal(t, "My New Project", payload["name"])
		assert.Equal(t, "my-new-project", payload["slug"])
		assert.Equal(t, "other", payload["platform"])

		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(mockProject)
	}))
	defer testServer.Close()

	client := NewSentryClient(testServer.URL, "test-token", nil)
	project, err := client.CreateProject(context.Background(), "my-org", "my-team", "my-new-project", "My New Project", "other")

	require.NoError(t, err)
	require.NotNil(t, project)
	assert.Equal(t, "123", project.ID)
	assert.Equal(t, "my-new-project", project.Slug)
	assert.Equal(t, "My New Project", project.Name)
	assert.Equal(t, "my-team", project.Team.Slug)
}

func TestCreateProjectError(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("Invalid project slug"))
	}))
	defer testServer.Close()

	client := NewSentryClient(testServer.URL, "test-token", nil)
	_, err := client.CreateProject(context.Background(), "my-org", "my-team", "invalid slug", "Project", "other")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "400")
}
