// SPDX-License-Identifier: Apache-2.0

package testcontainers

import (
	"context"
	"fmt"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type cleanup func() error

type PostgresImage string

const (
	Postgres14 PostgresImage = "debezium/postgres:14-alpine"
	Postgres17 PostgresImage = "debezium/postgres:17-alpine"
)

func SetupPostgresContainer(ctx context.Context, cfg ContainerConfig, url *string) (cleanup, error) {
	waitForLogs := wait.
		ForLog("database system is ready to accept connections").
		WithOccurrence(2).
		WithStartupTimeout(5 * time.Second)

	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithWaitStrategy(waitForLogs),
	}
	if cfg.ConfigFile != "" {
		opts = append(opts, postgres.WithConfigFile(cfg.ConfigFile))
	}

	image := cfg.Image
	if image == "" {
		image = Postgres14
	}

	if cfg.PortBindings != nil {
		opts = append(opts, testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = cfg.PortBindings
		}))
	}

	if len(cfg.Env) > 0 {
		opts = append(opts, testcontainers.WithEnv(cfg.Env))
	}

	ctr, err := postgres.Run(ctx, string(image), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres container: %w", err)
	}

	*url, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("retrieving connection string for postgres container: %w", err)
	}

	return func() error {
		return ctr.Terminate(ctx)
	}, nil
}

type ContainerConfig struct {
	Image        PostgresImage
	ConfigFile   string
	PortBindings nat.PortMap
	Env          map[string]string
}
