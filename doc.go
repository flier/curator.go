/*
Curator.go is a Golang porting for Curator, which base on the go-zookeeper.


Learn ZooKeeper


Curator.go users are assumed to know ZooKeeper. A good place to start is http://zookeeper.apache.org/doc/trunk/zookeeperStarted.html


Using Curator


The Curator.go are available from github.com.

	$ go get github.com/flier/curator.go

You can easily include Curator.go into your code.

	import (
   		"github.com/flier/curator.go"
	)


Getting a Connection


Curator uses Fluent Style. If you haven't used this before, it might seem odd so it's suggested that you familiarize yourself with the style.

Curator connection instances (CuratorFramework) are allocated from the CuratorFrameworkBuilder. You only need one CuratorFramework object for each ZooKeeper cluster you are connecting to:

	curator.NewClient(connString, retryPolicy)

This will create a connection to a ZooKeeper cluster using default values. The only thing that you need to specify is the retry policy. For most cases, you should use:

	retryPolicy := curator.NewExponentialBackoffRetry(time.Second, 3, 15*time.Second)

	client := curator.NewClient(connString, retryPolicy)

	client.Start()
	defer client.Close()

The client must be started (and closed when no longer needed).


Calling ZooKeeper Directly


Once you have a CuratorFramework instance, you can make direct calls to ZooKeeper in a similar way to using the raw ZooKeeper object provided in the ZooKeeper distribution. E.g.:

	client.Create().ForPathWithData(path, payload)

The benefit here is that Curator manages the ZooKeeper connection and will retry operations if there are connection problems.


Recipes


Distributed Lock


	lock := curator.NewInterProcessMutex(client, lockPath)

	if ( lock.Acquire(maxWait, waitUnit) )
	{
    	defer lock.Release()

    	// do some work inside of the critical section here
	}


Leader Election


	listener := curator.NewLeaderSelectorListener(func(CuratorFramework client) error {
    	// this callback will get called when you are the leader
    	// do whatever leader work you need to and only exit
 	    // this method when you want to relinquish leadership
	}))

	selector := curator.NewLeaderSelector(client, path, listener)
	selector.AutoRequeue()  // not required, but this is behavior that you will probably expect
	selector.Start()

*/
package curator
