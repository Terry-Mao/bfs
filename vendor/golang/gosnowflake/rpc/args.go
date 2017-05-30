package rpc

type NextIdsArgs struct {
	WorkerId int64 // snowflake worker id
	Num      int   // batch next id number
}
