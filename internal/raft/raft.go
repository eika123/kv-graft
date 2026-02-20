package raft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/rpc"
	"os"
)

RAFT_PORT = 8950

type LogEntry struct {
	Position uint64
	Term     uint64
	Command  string
}

type Log struct {
	logfile        string
	memoryCapacity int
}

type RaftNode struct {
	id            uint64
	ip            string
	port          uint16
	RaftNodeState RaftState
}

func calculateRaftNodeId(ip string, port uint16) uint64 {
	// hash ip:port
	hash := fnv.New64a()
	fmt.Fprintf(hash, "%s:%d", ip, port)
	return hash.Sum64()
}

func NewRaftNode(ip string, port uint16, logFile string, applicationStateMachine ApplicationState) (*RaftNode, error) {
	rn := &RaftNode{
		ip:   ip,
		port: port,
		id:   calculateRaftNodeId(ip, port),
		RaftNodeState: RaftState{
			currentTerm: 0,
			votedFor:    0,
			log: Log{
				logfile:        logFile,
				memoryCapacity: 1000, // TODO: make this configurable
			},
			commitIndex: 0,
			lastApplied: 0,
			nextIndex:   make(map[uint64]uint64),
			matchIndex:  make(map[uint64]uint64),
			isLeader:    false,
			lastEntry: LogEntry{
				Position: 0,
				Term:     0,
				Command:  "",
			},
			ApplicationStateMachine: applicationStateMachine,
		},
	}
	// read last log entry from log file and update RaftNodeState.lastEntry
	lastEntry, err := rn.RaftNodeState.log._readLastLogEntry()
	if err != nil {
		return nil, err
	}
	rn.RaftNodeState.lastEntry = lastEntry
	return rn, nil
}

func (rn *RaftNode) RegisterRPCs() error {
	if err := rpc.Register(rn.RaftNodeState); err != nil {
		return err
	}
	rpc.HandleHTTP()
	return nil
}

type ApplicationState interface {
	applyStateChange(command string) error
}

// Same as the State struct in the Raft paper
type RaftState struct {
	// Persistent state on all servers
	currentTerm uint64
	votedFor    uint64
	log         Log

	// Volatile state on all servers
	commitIndex uint64 // index of highest log entry known to be committed, initialized to 0, increases monotonically
	lastApplied uint64 // index of highest log entry applied to state machine, initialized to 0, increases monotonically

	// Volatile state on leaders
	// ----------------------------------------------------------------------------------------------------------------------
	// for each server, index of the next log entry to send to that server, initialized to leader last log index + 1
	nextIndex map[uint64]uint64

	// for each server, index of highest log entry known to be replicated on server, initialized to 0, increases monotonically
	matchIndex map[uint64]uint64
	// ----------------------------------------------------------------------------------------------------------------------

	// volatile state on all servers
	isLeader                bool
	lastEntry               LogEntry
	ApplicationStateMachine ApplicationState
}

type requestVoteArgs struct {
	Term, CandidateID, LastLogIndex, LastLogTerm uint64
}

type requestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

func (rs *RaftState) RequestVoteRPC(args requestVoteArgs, reply *requestVoteReply) error {

	if args.Term < rs.currentTerm {
		reply.Term = rs.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.LastLogIndex < rs.commitIndex {
		reply.Term = rs.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.LastLogIndex == rs.commitIndex && args.LastLogTerm < rs.currentTerm {
		reply.Term = rs.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if rs.votedFor == 0 || rs.votedFor == args.CandidateID {
		rs.votedFor = args.CandidateID
		rs.currentTerm = args.Term
		reply.Term = args.Term
		reply.VoteGranted = true
		return nil
	}

	return fmt.Errorf("unexpected case in RequestVoteRPC")
}

// used by leader only
func (rn *RaftNode) newLogEntry(command string) (LogEntry, error) {
	if rn.RaftNodeState.isLeader == false {
		return LogEntry{}, fmt.Errorf("not leader")
	}
	return LogEntry{
		Position: rn.RaftNodeState.lastEntry.Position + 1,
		Term:     rn.RaftNodeState.currentTerm,
		Command:  command,
	}, nil
}

func (rn *RaftNode) ReplicateCommand(command string) error {
	if rn.RaftNodeState.isLeader == false {
		return fmt.Errorf("only leader can replicate commands")
	}
	newEntry, err := rn.newLogEntry(command)
	if err != nil {
		return err
	}

	// send newEntry to replicas
	appendArgs := appendEntriesArgs{
		Term:         rn.RaftNodeState.currentTerm,
		LeaderID:     rn.id,
		PrevLogIndex: rn.RaftNodeState.lastEntry.Position,
		PrevLogTerm:  rn.RaftNodeState.lastEntry.Term,
		Entries:      []LogEntry{newEntry},
		LeaderCommit: rn.RaftNodeState.commitIndex,
	}

	reply := appendEntriesReply{}
	// send appendArgs to replicas and get reply, if majority of replicas reply success, then update commitIndex and apply log entry to state machine

	rn.RaftNodeState.AppendEntriesRPC(appendArgs, &reply)

	return nil
}

type appendEntriesArgs struct {
	Term         uint64
	LeaderID     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type appendEntriesReply struct {
	Term    uint64
	Success bool
}

func (rs *RaftState) AppendEntriesRPC(args appendEntriesArgs, reply *appendEntriesReply) error {
	return nil
}

// ================================  Log management functions ================================
func (rs *RaftState) appendLogEntry(entry LogEntry) error {
	if entry.Position != rs.lastEntry.Position+1 {
		return fmt.Errorf("log entry position must be last log entry position + 1")
	}

	return rs.log._writeLogEntry(entry)
}

// Replays the log up to the given position and updates the lastEntry field of the RaftState
// uptoPosition can be e.g commitIndex triggered after agreement from leader AppendEntriesRPC.
// Server restarts should wait for log replay until commitIndex is negotiated with leader through
// AppendEntriesRPC, as the commitIndex and lastApplied are volatile
func (rs *RaftState) replayLog(uptoPosition uint64) error {
	logfile := rs.log.logfile
	f, err := os.Open(logfile)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(f)
	var lastEntry LogEntry
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			entry := LogEntry{}
			err := json.Unmarshal([]byte(line), &entry)
			if err != nil {
				return err
			}
			if entry.Position <= uptoPosition {
				err := rs.ApplicationStateMachine.applyStateChange(entry.Command)
				if err != nil {
					return err
				}
			}

			lastEntry = entry
		}

	}
	rs.lastEntry = lastEntry
	return nil
}

func (l *Log) _writeLogEntry(entry LogEntry) error {
	f, err := os.OpenFile(l.logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	jsonEntry, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	fmt.Fprintf(f, "%s\n", jsonEntry)
	return nil
}

func (l *Log) _readLastLogEntry() (LogEntry, error) {
	logfile, err := os.Open(l.logfile)
	if err != nil {
		return LogEntry{}, err
	}
	defer logfile.Close()

	var lastLine string
	scanner := bufio.NewScanner(logfile)

	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			lastLine = line
		}
	}
	lastEntry := LogEntry{}
	err = json.Unmarshal([]byte(lastLine), &lastEntry)
	if err != nil {
		return LogEntry{}, err
	}
	return lastEntry, nil
}
