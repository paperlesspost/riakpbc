package riakpbc

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	NODE_WRITE_RETRY time.Duration = time.Second * 5
	NODE_READ_RETRY  time.Duration = time.Second * 5
)

type Pool struct {
	cluster []string
	nodes   chan *Node
	sync.Mutex
}

// NewPool returns an instantiated pool given a slice of node addresses.
func NewPool(cluster []string) (*Pool, error) {
	nodes := make(chan *Node, len(cluster))

	nodesUp := false
	for _, node := range cluster {
		newNode, err := NewNode(node, NODE_READ_RETRY, NODE_WRITE_RETRY)
		if err == nil {
			nodesUp = true
			nodes <- newNode

		}
	}
	if nodesUp == false {
		return nil, ErrAllNodesDown
	}
	pool := &Pool{
		cluster: cluster,
		nodes:   nodes,
	}

	return pool, nil
}

// SelectNode returns a node from the pool using weighted error selection.
//
// Each node has an assignable error rate, which is incremented when an error
// occurs, and decays over time - 50% each 10 seconds by default.
func (pool *Pool) SelectNode() (*Node, error) {
	pool.Lock()
	defer pool.Unlock()

	select {
	case n := <-pool.nodes:
		return n, nil
	default:
		return NewNode(pool.cluster[rand.Intn(len(pool.cluster))], NODE_READ_RETRY, NODE_WRITE_RETRY)
	}
	return nil, ErrAllNodesDown
}

func (pool *Pool) ReturnNode(node *Node) {
	pool.Lock()
	defer pool.Unlock()
	select {
	case pool.nodes <- node:
	default:
		node.Close()
	}
}

func (pool *Pool) Close() {
	for node := range pool.nodes {
		node.Close()
	}
}

func (pool *Pool) String() string {
	return fmt.Sprintf("riakpbc cluster %v", pool.cluster)
}
