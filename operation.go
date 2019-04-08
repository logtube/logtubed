package main

// Operation marshaled record
type Operation struct {
	Index string `json:"index"`
	Body  []byte `json:"body"`
}
