package framework

import (
	"time"

	"github.com/flier/curator.go"
)

func CreateSimple(connString string) curator.CuratorFramework {
	// these are reasonable arguments for the ExponentialBackoffRetry.
	// the first retry will wait 1 second,
	// the second will wait up to 2 seconds,
	// the third will wait up to 4 seconds.
	retryPolicy := curator.NewExponentialBackoffRetry(time.Second, 3, 15*time.Second)

	return curator.NewClient(connString, retryPolicy)
}

func createWithOptions(connString string, retryPolicy curator.RetryPolicy, connectionTimeout, sessionTimeout time.Duration) curator.CuratorFramework {
	// using the CuratorFrameworkBuilder/Builder() gives fine grained control
	return curator.Builder().ConnectString(connString).RetryPolicy(retryPolicy).ConnectionTimeout(connectionTimeout).SessionTimeout(sessionTimeout).Build()
}
