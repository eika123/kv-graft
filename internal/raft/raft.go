package raft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
)

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
	id   uint64
	ip   string
	port uint16
}

func (rn *RaftNode) generateRaftNodeID() uint64 {
	// hash ip:port
	hash := fnv.New64a()
	fmt.Fprintf(hash, "%s:%d", rn.ip, rn.port)
	return hash.Sum64()
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
	Term         uint64
	CandidateID  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
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
func (rs *RaftState) newLogEntry(command string) LogEntry {
	return LogEntry{
		Position: rs.lastEntry.Position + 1,
		Term:     rs.currentTerm,
		Command:  command,
	}
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
