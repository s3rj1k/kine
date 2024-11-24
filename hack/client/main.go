// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/k3s-io/kine/pkg/client"
	"github.com/k3s-io/kine/pkg/endpoint"
	kinetls "github.com/k3s-io/kine/pkg/tls"
	"github.com/urfave/cli/v2"
)

type Config struct {
	Endpoints []string
	TLS       *TLSConfig
}

type TLSConfig struct {
	CertFile string
	KeyFile  string
	CAFile   string
}

func main() {
	app := &cli.App{
		Name:  "kine-cli",
		Usage: "A command line interface for kine",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "endpoints",
				Value:   "http://127.0.0.1:2379",
				Usage:   "Comma-separated etcd endpoints",
				EnvVars: []string{"KINE_ENDPOINTS"},
			},
			&cli.StringFlag{
				Name:    "cert",
				Usage:   "TLS certificate file path",
				EnvVars: []string{"KINE_CERT_FILE"},
			},
			&cli.StringFlag{
				Name:    "key",
				Usage:   "TLS key file path",
				EnvVars: []string{"KINE_KEY_FILE"},
			},
			&cli.StringFlag{
				Name:    "ca",
				Usage:   "TLS CA certificate file path",
				EnvVars: []string{"KINE_CA_FILE"},
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "get",
				Usage: "Get a key",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "key",
						Usage:    "Key to get",
						Required: true,
					},
				},
				Action: getCLI,
			},
			{
				Name:  "put",
				Usage: "Put a key-value pair",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "key",
						Usage:    "Key to put",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "value",
						Usage:    "Value to put",
						Required: true,
					},
				},
				Action: putCLI,
			},
			{
				Name:  "create",
				Usage: "Create a new key-value pair",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "key",
						Usage:    "Key to create",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "value",
						Usage:    "Value to create",
						Required: true,
					},
				},
				Action: createCLI,
			},
			{
				Name:  "update",
				Usage: "Update an existing key-value pair",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "key",
						Usage:    "Key to update",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "value",
						Usage:    "Value to update",
						Required: true,
					},
					&cli.Int64Flag{
						Name:     "rev",
						Usage:    "Revision to update",
						Required: true,
					},
				},
				Action: updateCLI,
			},
			{
				Name:  "list",
				Usage: "List keys with a prefix",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "prefix",
						Usage: "Prefix to list",
					},
					&cli.Int64Flag{
						Name:  "rev",
						Usage: "Revision to list from",
					},
				},
				Action: listCLI,
			},
			{
				Name:  "delete",
				Usage: "Delete a key",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "key",
						Usage:    "Key to delete",
						Required: true,
					},
					&cli.Int64Flag{
						Name:  "rev",
						Usage: "Revision to delete",
					},
				},
				Action: deleteCLI,
			},
			{
				Name:  "compact",
				Usage: "Compact the event history",
				Flags: []cli.Flag{
					&cli.Int64Flag{
						Name:     "rev",
						Usage:    "Revision to compact to",
						Required: true,
					},
				},
				Action: compactCLI,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}

func createClient(c *cli.Context) (client.Client, error) {
	config := endpoint.ETCDConfig{
		Endpoints: strings.Split(c.String("endpoints"), ","),
	}

	if c.String("cert") != "" && c.String("key") != "" && c.String("ca") != "" {
		config.TLSConfig = kinetls.Config{
			CertFile: c.String("cert"),
			KeyFile:  c.String("key"),
			CAFile:   c.String("ca"),
		}
	}

	return client.New(config)
}

func printValue(value client.Value) {
	fmt.Printf("Key: %s\n", string(value.Key))
	fmt.Printf("Revision: %d\n", value.Modified)
	fmt.Printf("Value: %s\n", string(value.Data))
}

func getCLI(c *cli.Context) error {
	etcdClient, err := createClient(c)
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}
	defer etcdClient.Close()

	value, err := etcdClient.Get(context.Background(), c.String("key"))
	if err != nil {
		return fmt.Errorf("failed to get key: %v", err)
	}

	printValue(value)

	return nil
}

func putCLI(c *cli.Context) error {
	etcdClient, err := createClient(c)
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}
	defer etcdClient.Close()

	err = etcdClient.Put(context.Background(), c.String("key"), []byte(c.String("value")))
	if err != nil {
		return fmt.Errorf("failed to put key: %v", err)
	}

	fmt.Printf("Successfully set key '%s'\n", c.String("key"))
	return nil
}

func createCLI(c *cli.Context) error {
	etcdClient, err := createClient(c)
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}
	defer etcdClient.Close()

	err = etcdClient.Create(context.Background(), c.String("key"), []byte(c.String("value")))
	if err != nil {
		return fmt.Errorf("failed to create key: %v", err)
	}

	fmt.Printf("Successfully created key '%s'\n", c.String("key"))
	return nil
}

func updateCLI(c *cli.Context) error {
	etcdClient, err := createClient(c)
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}
	defer etcdClient.Close()

	err = etcdClient.Update(context.Background(), c.String("key"), c.Int64("rev"), []byte(c.String("value")))
	if err != nil {
		return fmt.Errorf("failed to update key: %v", err)
	}

	fmt.Printf("Successfully updated key '%s' at revision %d\n", c.String("key"), c.Int64("rev"))
	return nil
}

func listCLI(c *cli.Context) error {
	etcdClient, err := createClient(c)
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}
	defer etcdClient.Close()

	prefix := c.String("prefix")
	if prefix == "" {
		prefix = "*"
	}

	values, err := etcdClient.List(context.Background(), prefix, int(c.Int64("rev")))
	if err != nil {
		return fmt.Errorf("failed to list keys: %v", err)
	}

	if len(values) == 0 {
		fmt.Printf("No keys found with prefix '%s'\n", prefix)
		return nil
	}

	fmt.Printf("Found %d keys:\n---\n", len(values))
	for _, value := range values {
		printValue(value)
		fmt.Println("---")
	}

	return nil
}

func deleteCLI(c *cli.Context) error {
	etcdClient, err := createClient(c)
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}
	defer etcdClient.Close()

	err = etcdClient.Delete(context.Background(), c.String("key"), c.Int64("rev"))
	if err != nil {
		return fmt.Errorf("failed to delete key: %v", err)
	}

	fmt.Printf("Successfully deleted key '%s'\n", c.String("key"))
	return nil
}

func compactCLI(c *cli.Context) error {
	etcdClient, err := createClient(c)
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}
	defer etcdClient.Close()

	newRev, err := etcdClient.Compact(context.Background(), c.Int64("rev"))
	if err != nil {
		return fmt.Errorf("failed to compact: %v", err)
	}

	fmt.Printf("Successfully compacted to revision %d\n", newRev)
	return nil
}
