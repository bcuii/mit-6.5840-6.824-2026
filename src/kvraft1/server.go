package kvraft

import (
	"bytes"
	"log"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"

)

// fields are capitalized so that labgob can encode them in snapshots.
type entry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	// only rsm's reader goroutine touches data (Restore at startup
	// runs before the reader starts), so no lock is needed.
	data map[string]entry
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case rpc.GetArgs:
		e, ok := kv.data[args.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: e.Value, Version: e.Version, Err: rpc.OK}
	case rpc.PutArgs:
		e, ok := kv.data[args.Key] // a missing key has version 0
		if !ok && args.Version != 0 {
			return rpc.PutReply{Err: rpc.ErrNoKey}
		}
		if args.Version != e.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		kv.data[args.Key] = entry{Value: args.Value, Version: e.Version + 1}
		return rpc.PutReply{Err: rpc.OK}
	}
	return nil
}

// The snapshot is just the key/value map: versions already make a
// retried Put harmless, so there is no duplicate-detection state.
func (kv *KVServer) Snapshot() []byte {
	// Your code here
	w := new(bytes.Buffer)
	labgob.NewEncoder(w).Encode(kv.data)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	var m map[string]entry
	if err := labgob.NewDecoder(bytes.NewBuffer(data)).Decode(&m); err != nil {
		log.Fatalf("%v: Restore: %v", kv.me, err)
	}
	kv.data = m
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.PutReply)
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	// data must exist before MakeRSM starts the reader.
	kv := &KVServer{me: me, data: make(map[string]entry)}


	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
