package common

import "context"

type Config struct {
	BackOffTime  int
	MaximumRetry int
	Version      string
	Group        string
	Host         []string
	Debug        bool
}

type Handler func(ctx context.Context, msg []byte) (err error)