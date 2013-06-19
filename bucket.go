package riakpbc

// List all buckets
func (c *Conn) ListBuckets() (*RpbListBucketsResp, error) {
	reqdata := []byte{}

	node := c.SelectNode()

	if err := node.RawRequest(reqdata, "RpbListBucketsReq"); err != nil {
		return &RpbListBucketsResp{}, err
	}

	response, err := node.Response()
	if err != nil {
		return &RpbListBucketsResp{}, err
	}

	return response.(*RpbListBucketsResp), nil
}

// List all keys from bucket
func (c *Conn) ListKeys(bucket string) ([][]byte, error) {
	reqstruct := &RpbListKeysReq{
		Bucket: []byte(bucket),
	}

	node := c.SelectNode()

	if err := node.Request(reqstruct, "RpbListKeysReq"); err != nil {
		return nil, err
	}

	response, err := node.Response()
	if err != nil {
		return nil, err
	}

	keys := response.(*RpbListKeysResp).GetKeys()
	done := response.(*RpbListKeysResp).GetDone()
	for done != true {
		response, err := node.Response()
		if err != nil {
			return nil, err
		}
		keys = append(keys, response.(*RpbListKeysResp).GetKeys()...)
		done = response.(*RpbListKeysResp).GetDone()
	}

	return keys, nil
}

// Get bucket info
func (c *Conn) GetBucket(bucket string) (*RpbGetBucketResp, error) {
	reqstruct := &RpbGetBucketReq{
		Bucket: []byte(bucket),
	}

	node := c.SelectNode()

	if err := node.Request(reqstruct, "RpbGetBucketReq"); err != nil {
		return &RpbGetBucketResp{}, err
	}

	response, err := node.Response()
	if err != nil {
		return &RpbGetBucketResp{}, err
	}

	return response.(*RpbGetBucketResp), nil
}

// Create bucket
func (c *Conn) SetBucket(bucket string, nval *uint32, allowmult *bool) ([]byte, error) {
	reqstruct := &RpbSetBucketReq{}
	if opts := c.Opts(); opts != nil {
		reqstruct = opts.(*RpbSetBucketReq)
	}
	reqstruct.Bucket = []byte(bucket)
	if reqstruct.Props == nil {
		reqstruct.Props = &RpbBucketProps{}
		reqstruct.Props.NVal = nval
		reqstruct.Props.AllowMult = allowmult
	}

	node := c.SelectNode()

	if err := node.Request(reqstruct, "RpbSetBucketReq"); err != nil {
		return nil, err
	}

	response, err := node.Response()
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}
