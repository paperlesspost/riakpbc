package riakpbc

type Client struct {
	cluster []string
	pool    *Pool
	Coder   *Coder // Coder for (un)marshalling data
	logging bool
	closed  chan struct{}
}

// NewClient accepts a slice of node address strings and returns a Client object.
//
// Illegally addressed nodes will be rejected in the NewPool call.
func NewClient(cluster []string) (*Client, error) {
	pool, err := NewPool(cluster)
	if err != nil {
		return nil, err
	}
	return &Client{
		cluster: cluster,
		pool:    pool,
		logging: false,
		closed:  make(chan struct{}),
	}, nil
}

// NewClientWihtCoder accepts a slice of node address strings, a Coder for processing structs into data, and returns a Client object.
//
// Illegally addressed nodes will be rejected in the NewPool call.
func NewClientWithCoder(cluster []string, coder *Coder) (*Client, error) {
	pool, err := NewPool(cluster)
	if err != nil {
		return nil, err
	}
	return &Client{
		cluster: cluster,
		pool:    pool,
		Coder:   coder,
		logging: false,
		closed:  make(chan struct{}),
	}, nil
}

// Close closes the node TCP connections.
func (c *Client) Close() {
	close(c.closed)
	c.pool.Close()
}

// Pool returns the pool associated with the client.
func (c *Client) Pool() *Pool {
	return c.pool
}

// Do executes a prepared query and returns the results.
func (c *Client) Do(opts interface{}) (interface{}, error) {
	// Bucket
	if _, ok := opts.(*RpbListKeysReq); ok {
		return c.listKeys(opts.(*RpbListKeysReq), string(opts.(*RpbListKeysReq).GetBucket()))
	}
	if _, ok := opts.(*RpbGetBucketReq); ok {
		return c.getBucket(opts.(*RpbGetBucketReq), string(opts.(*RpbGetBucketReq).GetBucket()))
	}
	if _, ok := opts.(*RpbSetBucketReq); ok {
		nval := opts.(*RpbSetBucketReq).Props.GetNVal()
		allowMulti := opts.(*RpbSetBucketReq).Props.GetAllowMult()
		return c.setBucket(opts.(*RpbSetBucketReq), string(opts.(*RpbSetBucketReq).GetBucket()), &nval, &allowMulti)
	}

	// Object
	if _, ok := opts.(*RpbGetReq); ok {
		return c.fetchObject(opts.(*RpbGetReq), string(opts.(*RpbGetReq).GetBucket()), string(opts.(*RpbGetReq).GetKey()))
	}
	if _, ok := opts.(*RpbDelReq); ok {
		return c.deleteObject(opts.(*RpbDelReq), string(opts.(*RpbDelReq).GetBucket()), string(opts.(*RpbDelReq).GetKey()))
	}

	// Query
	if _, ok := opts.(*RpbMapRedReq); ok {
		return c.mapReduce(opts.(*RpbMapRedReq), string(opts.(*RpbMapRedReq).GetRequest()), string(opts.(*RpbMapRedReq).GetContentType()))
	}
	if _, ok := opts.(*RpbIndexReq); ok {
		return c.index(opts.(*RpbIndexReq), string(opts.(*RpbIndexReq).GetBucket()), string(opts.(*RpbIndexReq).GetIndex()), string(opts.(*RpbIndexReq).GetKey()), string(opts.(*RpbIndexReq).GetRangeMin()), string(opts.(*RpbIndexReq).GetRangeMax()))
	}
	if _, ok := opts.(*RpbSearchQueryReq); ok {
		return c.search(opts.(*RpbSearchQueryReq), string(opts.(*RpbSearchQueryReq).GetIndex()), string(opts.(*RpbSearchQueryReq).GetQ()))
	}

	// Server
	if _, ok := opts.(*RpbSetClientIdReq); ok {
		return c.setClientId(opts.(*RpbSetClientIdReq), string(opts.(*RpbSetClientIdReq).GetClientId()))
	}

	return nil, nil
}

// DoObject executes a prepared query with data and returns the results.
func (c *Client) DoObject(opts interface{}, in interface{}) (interface{}, error) {
	if _, ok := opts.(*RpbPutReq); ok {
		return c.storeObject(opts.(*RpbPutReq), string(opts.(*RpbPutReq).GetBucket()), string(opts.(*RpbPutReq).GetKey()), in)
	}

	return nil, nil
}

// DoStruct executes a prepared query on a struct with the coder and returns the results.
func (c *Client) DoStruct(opts interface{}, in interface{}) (interface{}, error) {
	if _, ok := opts.(*RpbGetReq); ok {
		return c.fetchStruct(opts.(*RpbGetReq), string(opts.(*RpbGetReq).GetBucket()), string(opts.(*RpbGetReq).GetKey()), in)
	}
	if _, ok := opts.(*RpbPutReq); ok {
		return c.storeStruct(opts.(*RpbPutReq), string(opts.(*RpbPutReq).GetBucket()), string(opts.(*RpbPutReq).GetKey()), in)
	}

	return nil, nil
}

// ReqResp is the top level interface for the client for a bulk of Riak operations
func (c *Client) ReqResp(reqstruct interface{}, structname string, raw bool) (response interface{}, err error) {
	node, err := c.pool.SelectNode()
	if err != nil {
		return nil, err
	}
	response, err = node.ReqResp(reqstruct, structname, raw)
	// if the error is just a not found error, return the node
	// to the pool, otherwise, close the conn and dont return it
	if err != nil && err == ErrObjectNotFound {
		c.pool.ReturnNode(node)
	} else {
		node.Close()
	}
	return
}

// ReqMultiResp is the top level interface for the client for the few
// operations which have to hit the server multiple times to guarantee
// a complete response: List keys, Map Reduce, etc.
func (c *Client) ReqMultiResp(reqstruct interface{}, structname string) (response interface{}, err error) {
	node, err := c.pool.SelectNode()
	if err != nil {
		return nil, err
	}
	defer c.pool.ReturnNode(node)
	return node.ReqMultiResp(reqstruct, structname)
}

func (c *Client) EnableLogging() {
	c.logging = true
}

func (c *Client) DisableLogging() {
	c.logging = false
}

func (c *Client) LoggingEnabled() bool {
	return c.logging
}
