package framework

import (
	"github.com/flier/curator.go"
)

func Create(client curator.CuratorFramework, path string, payload []byte) (string, error) {
	// this will create the given ZNode with the given data
	return client.Create().ForPathWithData(path, payload)
}

func CreateEphemeral(client curator.CuratorFramework, path string, payload []byte) (string, error) {
	// this will create the given EPHEMERAL ZNode with the given data
	return client.Create().WithMode(curator.EPHEMERAL).ForPathWithData(path, payload)
}
