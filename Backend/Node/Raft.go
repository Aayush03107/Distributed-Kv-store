package main;

import (
	"fmt"
	"bufio"
	"encoding/binary"
	"encoding/json"
	"flag"
	//"bytes"
	"strconv"
	"io"
	"net"
	"os"
	"time"
	"strings"
	"sync"
	"math/rand/v2"
	"kv/set";
	"math"
)
type Envelope struct {
	Type    string
	LbId string
	Message json.RawMessage
}
type VoteReturn struct {
	Term        int
	VoteGranted bool
	Id string;
}
type HeartbeatReturn struct{
	Term int;
	Success bool;
	Id string;
}
type Message struct {
	Id      string
	Resp    []byte
	Port    string
	Counter int
}
type clientResponse struct {
	Response []byte
	Enduser  string
}
type Log struct {
	M     Message
	Index int
	Term  int
}
type AppendEntry struct {
	Term         int
	LeaderId     string
	Entries      []Log
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}
type AskVote struct {
	Type         string
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

type Node struct {
    mp map[string]string;
	currentTerm int;
	mu sync.Mutex;
	state string;
	isLeader bool;
	currentLeader string;
	votesRecieved *set.Set;
	votedFor string;
	servers []string;
	id string;
	commitIndex int;
	lastApplied int;
	logs []Log;
	LbId string;
	ShardId string;
	leaderState *LeaderState;
	timer *time.Timer
	inbox chan Envelope
	peerConn map[string]net.Conn
}
type LeaderState struct {
	NextIndex map[string]int;
	MatchIndex map[string]int;
	Sentlen map[string]int;
}
func decode(conn net.Conn) (Envelope, error) {
	var length int32

	err := binary.Read(conn, binary.BigEndian, &length)
	if err != nil {
		return Envelope{}, err
	}
	data := make([]byte, length)
	_, err = io.ReadFull(conn, data)
	if err != nil {
		return Envelope{}, err
	}
	var msg Envelope
	err = json.Unmarshal(data, &msg)
	// fmt.Println("Type:", msg.Type)
	// fmt.Println("Payload:", string(data))
	if err != nil {
		return Envelope{}, err
	}
	return msg, nil
}

func SendMessage(conn net.Conn, msg Envelope) error{
	
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	err = binary.Write(conn, binary.BigEndian, int32(len(data)))
	if err != nil {
		return err
	}
	_, err = conn.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (node *Node) send(peer string, msg Envelope) {
	go func() {
		node.mu.Lock();
		conn, _:= node.peerConn[peer];
		node.mu.Unlock();
		//fmt.Println("Sending", msg.Type, "to", peer)
		if conn == nil {
			var err error
			conn, err = net.Dial("tcp", peer)
			if err != nil {
				fmt.Println("Failed to connect to", peer, ":", err)
				return ;
			}
			node.mu.Lock();
			fmt.Println("Connected to", conn.RemoteAddr());
			node.peerConn[peer] = conn
			node.mu.Unlock();
		}
		//defer conn.Close()
        err := SendMessage(conn, msg)
        if err != nil {
            conn.Close()
			node.mu.Lock()
			node.peerConn[peer] = nil
			node.mu.Unlock();
        }
    }();
}

func (node *Node) Listen(ln net.Listener){
	for{
		conn,err := ln.Accept();
		if(err != nil){
			panic(err);
		}
		go func(conn net.Conn){
			for{
				msg, err := decode(conn);
				if err != nil {
					if(err == io.EOF){
						return ;
					}
					return ;
				}
				node.inbox <- msg
			}
		}(conn)
	}
}

func (node *Node) handleAskVote(msg Envelope){
	var mesg AskVote;
	json.Unmarshal(msg.Message,&mesg);
	if(node.currentTerm < mesg.Term){
		node.votedFor = "";
		node.currentTerm = mesg.Term;
		node.state = "follower";
	};
	
	lastTerm := 0 ;
	if(len(node.logs) > 0){
		lastTerm = node.logs[len(node.logs)-1].Term;
	};
	
	logok := (mesg.LastLogTerm > lastTerm) || (mesg.LastLogTerm == lastTerm && mesg.LastLogIndex+1 >=len(node.logs));
	
	
	if(node.currentTerm == mesg.Term && logok && (node.votedFor == "" || node.votedFor == mesg.CandidateId)){
		node.votedFor = mesg.CandidateId;
		retmsg := VoteReturn{Term : node.currentTerm, VoteGranted : true, Id: node.id};
		data , err := json.Marshal(retmsg);
		if(err != nil){
			panic(err);
		}
		mg := Envelope{Type: "AskVoteReturn", Message : data};
		node.resetTimer();
		node.send(mesg.CandidateId,mg);
	}else {
		retmsg := VoteReturn{Term : node.currentTerm, VoteGranted : false, Id: node.id};
		data , err := json.Marshal(retmsg);
		if(err != nil){
			panic(err);
		}
		mg := Envelope{Type: "AskVoteReturn", Message : data};
		fmt.Println("Sending AskVoteReturn to", mesg.CandidateId);
		node.send(mesg.CandidateId, mg);
	}
};

func (node* Node) handleAppendEntry(msg Envelope){
	var ae AppendEntry;
	json.Unmarshal(msg.Message, &ae);
	//fmt.Println("Received AppendEntry from",ae.LeaderId);
	// fmt.Println("\n==== APPEND ENTRY RECEIVED ====")
	// fmt.Println("Node:", node.id)
	// fmt.Println("Leader:", ae.LeaderId)
	// fmt.Println("Term:", ae.Term)x
	// fmt.Println("PrevLogIndex:", ae.PrevLogIndex)
	// fmt.Println("PrevLogTerm:", ae.PrevLogTerm)
	// fmt.Println("Entries count:", len(ae.Entries));
	// for _, e := range ae.Entries {
	// 	fmt.Printf("Entry -> Index:%d Term:%d Data:%s\n",
	// 		e.Index, e.Term, string(e.M.Resp))
	// }
	if(ae.Term < node.currentTerm){
		var mesg HeartbeatReturn;
		mesg = HeartbeatReturn{Term : node.currentTerm , Success:false , Id : node.id};
		data, err := json.Marshal(mesg);
		if(err != nil){
			panic(err);
		}
		mg := Envelope{Type: "AEret", Message : data};
		node.send(ae.LeaderId, mg);
		return;
	}
	
	if( ae.Term >= node.currentTerm){
		node.currentTerm = ae.Term;
		node.votedFor = "";
		node.currentLeader = ae.LeaderId;
		node.resetTimer();
		if(ae.PrevLogIndex == -1 && len(node.logs) > 0){
			var mesg HeartbeatReturn;
			mesg = HeartbeatReturn{Term : node.currentTerm, Success: false, Id: node.id};
			data ,err := json.Marshal(&mesg);
			if(err != nil){
				panic(err);
			};
			mg := Envelope{Type: "AEret", Message: data};
			node.resetTimer();
			node.send(ae.LeaderId,mg);
			return ;
		}
		
		if(ae.PrevLogIndex == -1 && len(node.logs) == 0){
			
			node.state = "follower";
			// fmt.Println("hi this is special ");
			for _,entry := range ae.Entries{
				node.logs = append(node.logs,entry);
			}
			mse := HeartbeatReturn{Term : node.currentTerm, Success: true, Id : node.id};
			msgbytes,err := json.Marshal(&mse);
			if(err != nil){
				panic(err);
			}
			mg := Envelope{Type:"AEret",Message: msgbytes};
			node.send(ae.LeaderId,mg);
			return ;
		}
		// if(ae.PrevLogIndex == 0 && len(node.logs) == 0){
		// 	node.state = "follower";
		// 	fmt.Println("here i have to do changes or it is heartbeat");
		// 	//node.logs = node.logs[:ae.PrevLogIndex+1];
		// 	for _,entry := range ae.Entries{
		// 		node.logs = append(node.logs,entry);
		// 	}
		// 	if(ae.LeaderCommit > node.commitIndex){
		// 		for i:= node.commitIndex ; i < ae.LeaderCommit  ; i++{
		// 			node.Deliver(i);
		// 		}
		// 		node.commitIndex = ae.LeaderCommit;
		// 	}
		// 	mse := HeartbeatReturn{Term : node.currentTerm, Success: true, Id : node.id};
		// 	msgbytes,err := json.Marshal(&mse);
		// 	if(err != nil){
		// 		panic(err);
		// 	}
		// 	mg := Envelope{Type:"AEret",Message: msgbytes};
		// 	send(ae.LeaderId,mg);
		// }
		if( ae.PrevLogIndex >= len(node.logs) || node.logs[ae.PrevLogIndex].Term != ae.PrevLogTerm  || (ae.PrevLogIndex == node.logs[ae.PrevLogIndex].Index && ae.PrevLogTerm != node.logs[ae.PrevLogIndex].Term)){
			var mesg HeartbeatReturn;
			mesg = HeartbeatReturn{Term : node.currentTerm, Success: false, Id: node.id};
			data ,err := json.Marshal(&mesg);
			if(err != nil){
				panic(err);
			};
			mg := Envelope{Type: "AEret", Message: data};
			node.resetTimer();
			node.send(ae.LeaderId,mg);
			// for _, l := range node.logs {
			//     fmt.Print(string(l.M.Resp), " ")
			// }
			return ;
		}
		
		node.state = "follower";
		// fmt.Println("here i have to do changes or it is heartbeat");
		node.logs = node.logs[:ae.PrevLogIndex+1];
		for _,entry := range ae.Entries{
			node.logs = append(node.logs,entry);
		}
		if(ae.LeaderCommit > node.commitIndex){
			for i:= node.commitIndex ; i < ae.LeaderCommit  ; i++{
				node.Deliver(i);
			}
			node.commitIndex = ae.LeaderCommit;
		}
		mse := HeartbeatReturn{Term : node.currentTerm, Success: true, Id : node.id};
		msgbytes,err := json.Marshal(&mse);
		if(err != nil){
			panic(err);
		}
		mg := Envelope{Type:"AEret",Message: msgbytes};
		node.send(ae.LeaderId,mg);
	}
	
	// for _, l := range node.logs {
	//     fmt.Print(l.Term,string(l.M.Resp), " ")
	// }
}

func (node *Node) handleVoteCounting(msg Envelope){
	var retvote VoteReturn;
	json.Unmarshal(msg.Message, &retvote);
	if(retvote.Term > node.currentTerm){
		node.currentTerm = retvote.Term;
		node.state = "follower";
		node.votedFor = "";
		node.isLeader = false;
		node.votesRecieved.Clear();
		node.resetTimer();
	}
	fmt.Println("checkpointtt");
	if(node.state == "candidate" && node.currentTerm == retvote.Term && retvote.VoteGranted){
		node.votesRecieved.Add(retvote.Id);
		if(node.votesRecieved.Size() >= int(math.Ceil((float64(len(node.servers)+1))/2))){
			node.isLeader = true;
			node.currentLeader = node.id;
			node.state = "leader";
			node.votesRecieved.Clear();
			node.votesRecieved.Add(node.id);
			fmt.Println("Raft I am the leader", node.id);
			for _,s := range node.servers{
				node.leaderState.NextIndex[s] = len(node.logs);
				node.leaderState.MatchIndex[s] = 0;
			}
			go node.sendLogEntries();
		}
	}
}

// func (node *Node) LeadersJob(){
// 	node.isLeader = true;
// 	node.currentLeader = node.id;
// 	node.state = "leader";
// 	for _,s := range node.servers{
// 		node.leaderState.NextIndex[s] = len(node.logs);
// 		node.leaderState.MatchIndex[s] = 0;
// 	}
// 	//go node.sendHeartbeat();

// };

func (node *Node) sendLogEntries(){
	for {
		if node.state != "leader" {
			return
		}
		for _, s := range node.servers {
			if s == node.id {
				continue
			}
			// fmt.Println(" Leader Side logs")
			// fmt.Println("From Leader:", node.id)
			// fmt.Println("To:", s)
			//fmt.Println("NextIndex of node ",s, " is ", node.leaderState.NextIndex[s]);
			nextIdx := node.leaderState.NextIndex[s];
			if nextIdx > len(node.logs) {
				nextIdx = len(node.logs)
			}
			if nextIdx < 0 {
				nextIdx = 0
			}
			// if nextIdx < len(node.logs) {
			// 	fmt.Println("Sending entries:", node.logs[nextIdx:])
			// }
			prevIdx := nextIdx - 1;
			prevTerm := 0;
			if prevIdx >= 0 && prevIdx < len(node.logs) {
				prevTerm = node.logs[prevIdx].Term
			}

			var entries []Log;
			if nextIdx >= 0 && nextIdx <= len(node.logs) {
				entries = node.logs[nextIdx:]
			}
			node.mu.Lock()
			node.leaderState.Sentlen[s] = len(entries);
			node.mu.Unlock()
			msg := AppendEntry{
				Term: node.currentTerm,
				LeaderId: node.id,
				Entries: entries,
				PrevLogIndex: prevIdx,
				PrevLogTerm: prevTerm,
				LeaderCommit : node.commitIndex,
			}
			data, err := json.Marshal(msg)
			if err != nil {
				panic(err)
			}
			msgBytes := Envelope{Type: "AppendEntry", Message: data}
			//fmt.Println("Sending AppendEntry to", s);
			node.send(s, msgBytes)
		}
		time.Sleep(51 * time.Millisecond)
	}
}

// func (node *Node) sendHeartbeat(){
// 	for {
// 		if node.state != "leader" {
// 			return 
// 		}
// 		for _, s := range node.servers {
// 			if s != node.id {
// 				lastIndex := node.leaderState.NextIndex[s]-1;
// 				lastTerm := -1;
// 				if(lastIndex >= 0 && lastIndex < len(node.logs)){
// 					lastTerm = node.logs[lastIndex].Term;
// 				}
// 				node.mu.Lock()
// 				node.leaderState.Sentlen[s] = 0;
// 				node.mu.Unlock()
// 				msg := AppendEntry{Term: node.currentTerm, LeaderId: node.id, Entries: []Log{}, PrevLogIndex: lastIndex, PrevLogTerm: lastTerm}
// 				data, err := json.Marshal(msg)
// 				if err != nil {
// 					panic(err)
// 				}
// 				msgBytes := Envelope{Type: "AppendEntry", Message: data}
// 				send(s, msgBytes)
// 			}
// 		}
// 		time.Sleep(time.Second * 4)
// 	}
// }
// 
// func Parse(s []byte) ([]byte, error) {
// 	reader := bufio.NewReader(bytes.NewReader(s));
// 	line, err := reader.ReadBytes('\n');
// 	if err != nil {
// 		return nil, err;
// 	}
// 	if(line[0] == '+'){
// 		return bytes.TrimSpace(line[1:]), nil
// 	}
// 	if(line[0] != '*'){
// 		return nil, fmt.Errorf("expected '+', got %s", line[0]);
// 	};
// 	countStr := strings.TrimSpace(string(line[1:]))
// 	count, err := strconv.Atoi(countStr)
// 	if err != nil {
// 			return nil, err
// 	}
// 	var res []byte;
// 	for i := 0 ; i < int(count) ; i++{
// 		lenline,err := reader.ReadBytes('\n');
// 		if(lenline[0] != '$'){
// 			return nil, fmt.Errorf("expected '$', got %s", lenline[0]);
// 		}
// 		lenStr := strings.TrimSpace(string(lenline[1:]))
// 		li, err := strconv.Atoi(lenStr)
// 		if err != nil {
// 					return nil, err
// 		}
// 		temp:= make([]byte,li+2);
// 		_,err = io.ReadFull(reader, temp);
// 		if(err != nil){
// 			return nil,err;
// 		}
// 		res = append(res, temp[:li]...);
// 		res = append(res, ' ')
// 	}
// 	return bytes.TrimSpace(res),nil;
// }

func Parse(s []byte) ([][]byte, error) {
	if(s[0] != byte('*')){
		return nil, fmt.Errorf("expected '*', got %s", s[0]);
	}
	i := 1;
	var count int;
	fmt.Println("checkpoint1");
	for i = 1 ; i < len(s); i++{
		if(s[i] == '\r' && s[i+1] == '\n'){
			countStr := string(s[1:i])
			count, _ = strconv.Atoi(countStr)
			i+=2
			break;
		}
	};
	fmt.Println(count);
	var ans [][]byte;
	for count >0 {
		fmt.Println(i);
		if(s[i] != byte('$')){
			return nil, fmt.Errorf("not $");
		}
		i++;
		var countint int; 
		for j:=i ; j < len(s); j++{
			if(s[j] == '\r' && s[j+1] == '\n'){
				countstr := string(s[i:j]);
				countint, _= strconv.Atoi(countstr);
				i = j+2;
				break;
			}
		};
		var temp []byte;
		k := i;
		for countint>0{
			temp = append(temp,s[k]);
			k++;
			countint--;
		}
		ans = append(ans,temp);
		i = k+2;
		for _,u := range ans{
			fmt.Println(u);
		}
		count--;
	};
	fmt.Println("checkpoint2");
	return ans,nil;
	
}
func (node *Node) Deliver(i int) string{
	entry := node.logs[i];
	msg := entry.M;
	parts,err := Parse(msg.Resp);
	if(err != nil){
		panic(err);
	}
	//parts := strings.Fields(string(res));
	cmd := string(parts[0]);
	key := string(parts[1]);
	val := "";
	if len(parts) > 2 {
		val = string(parts[2]);
	}
	var response string
	fmt.Println(key);
	fmt.Println(val);
	if(cmd == "SET"){
		node.mu.Lock();
		node.mp[key] = val;
		node.mu.Unlock();
		response = "Ok";
	}else if(cmd == "GET"){
		node.mu.Lock();
		val = node.mp[key];
		node.mu.Unlock();
		response = val;
	}else if(cmd == "DEL"){
		node.mu.Lock();
		_,ok := node.mp[key];
		node.mu.Unlock();
		if (ok){
			node.mu.Lock();
			delete(node.mp,key);
			node.mu.Unlock();
			response = "1";
		}else{
			response = "0"
		}
	}
	return response;
}
func createAck(resp []byte) []byte{
			final := "+" + string(resp) + "\r\n"
			return []byte(final)
};
func sendToClient(ack []byte, clientPort string, enduser string) {
	fmt.Println("Raft sendToClient", clientPort);
	conn,err := net.Dial("tcp",clientPort);
	if err != nil {
		fmt.Println("err connecting to client");
		panic(err);
		return ;
	}
	msg := clientResponse{Response: ack, Enduser: enduser};
	jsonMsg, _ := json.Marshal(msg);
	mesg := Envelope{Type: "ClientResponse", Message: jsonMsg};

	defer conn.Close()
	_ = SendMessage(conn, mesg);
}
func (node *Node) sendToleader(s string , msg Envelope){
	for{
		node.mu.Lock()
		conn, ok := node.peerConn[s]
		node.mu.Unlock()
		if ok {
			err := SendMessage(conn, msg)
			if(err == nil){return }
		}
		conn, err := net.Dial("tcp", s)
		if err == nil {
			err := SendMessage(conn, msg)
			if(err == nil){
				node.mu.Lock()
				node.peerConn[s] = conn
				node.mu.Unlock()
				return
			}
			if( err != nil){
				node.mu.Lock()
				delete(node.peerConn, s)
				node.mu.Unlock()
				continue
			}
		}
		time.Sleep(time.Millisecond * 25)
	}
}
func (node *Node) applyLogs(){
	// fmt.Println("hi12312");
	// fmt.Println(node.lastApplied, " ", node.commitIndex);
	clientPort := node.LbId;
	fmt.Println("Raft applyLogs", clientPort);
	for i :=node.lastApplied ; i < min(node.commitIndex, len(node.logs)) ; i++{
		response :=node.Deliver(i);
		enduser := node.logs[i].M.Port;
		ack := createAck([]byte(response))
		// fmt.Println("haha");
		go sendToClient(ack, clientPort,enduser);
		// fmt.Println("yeah yeah");
	}
	node.lastApplied = node.commitIndex;
}
func (node *Node) handleAEreturn(msg Envelope){
	var mesg HeartbeatReturn
	err := json.Unmarshal(msg.Message, &mesg)
	if err != nil {
		panic(err)
	};
	// fmt.Println("\n📩 AE RESPONSE RECEIVED")
	// fmt.Println("From:", mesg.Id)
	// fmt.Println("Success:", mesg.Success)
	// fmt.Println("Term:", mesg.Term)
	
	if mesg.Term > node.currentTerm {
        node.currentTerm = mesg.Term
        node.state = "follower"
        node.isLeader = false
        node.votedFor = ""
        node.resetTimer()
        return
    }
	if node.leaderState.NextIndex[mesg.Id] >= len(node.logs) {
		node.leaderState.NextIndex[mesg.Id] = len(node.logs)
	}
	if node.leaderState.NextIndex[mesg.Id] < 0 {
		node.leaderState.NextIndex[mesg.Id] = 0
	}
	if mesg.Success{
		if mesg.Term > node.currentTerm {
			node.state = "follower"
			node.currentTerm = mesg.Term
			node.votedFor = ""
			return
		}
		//fmt.Println(node.leaderState.Sentlen[mesg.Id], " szf ",mesg.Id );
		node.leaderState.NextIndex[mesg.Id] += node.leaderState.Sentlen[mesg.Id] ;
		node.leaderState.MatchIndex[mesg.Id] = node.leaderState.NextIndex[mesg.Id]-1;
		//node.votesRecieved.Add(mesg.Id);
		for node.commitIndex < len(node.logs){
			acks:=1 ;
			for _,s := range node.servers {
				if node.leaderState.MatchIndex[s] >= node.commitIndex {
					if (s != node.id){
							acks++
						}
					}
				}
				
			if acks >= int(math.Ceil(float64(len(node.servers)+1) / 2.0)) {
				node.commitIndex++;
				node.applyLogs();
			}else{
				break ;
			}
		}
	}else{
		if node.leaderState.NextIndex[mesg.Id] > 0 {
			node.leaderState.NextIndex[mesg.Id]--;
		}
	}
	//fmt.Println("id: ",mesg.Id," ",node.leaderState.NextIndex[mesg.Id]);
}
func (node *Node) handleClientCommand(msg Envelope){
	if(node.isLeader == true){
		var message Message;
		
		err := json.Unmarshal(msg.Message, &message);
		if err != nil {
			panic(err)
		}
		node.logs = append(node.logs,Log{M:message,Index: len(node.logs), Term : node.currentTerm});
		for _,s := range node.servers{
			node.leaderState.NextIndex[s] = len(node.logs);
		}
	}else{
		go func(){
			for node.currentLeader == "" {
				
			}
			node.sendToleader(node.currentLeader, msg)
		}()
	}
}
func (node *Node) startElection(){
	node.state = "candidate";
	node.currentTerm++;
	node.votesRecieved.Add(node.id);
	node.votedFor = node.id;
	llt := 0
	var lli int;
	if(len(node.logs) == 0){
		lli = -1;
	}else{
		lli = node.logs[len(node.logs)-1].Index;
		llt = node.logs[lli].Term;
	}
	for _, s := range node.servers {
		if s != node.id {
			msg := AskVote{ Term: node.currentTerm, CandidateId: node.id, LastLogTerm: llt, LastLogIndex: lli };
			data,err := json.Marshal(msg);
			if err != nil {
				panic(err)
			}
			msgBytes := Envelope{Type: "AskVote", Message: data}
			//fmt.Println("Sending AskVote to", s)
			node.send(s, msgBytes);
		}
	}	
};
func (node *Node) handlemsg(msg Envelope){
	if msg.Type == "AskVote"{
		node.handleAskVote(msg);
	}else if (msg.Type == "AppendEntry"){
		node.handleAppendEntry(msg);
	}else if (msg.Type == "AskVoteReturn"){
		fmt.Println("Received AskVoteReturn ");
		node.handleVoteCounting(msg);
	}else if (msg.Type == "AEret"){
		node.handleAEreturn(msg);
	}else if( msg.Type == "ClientCommand"){
		fmt.Println("Raft handleClientCommand", msg.LbId);
		node.LbId = msg.LbId
		fmt.Println("Raft handleClientCommand", node.LbId);
		node.handleClientCommand(msg);
	}
};
func (node *Node) Hello(){
	fmt.Println("hello");
};
func randomTimeout() time.Duration {
    return time.Duration(150+rand.IntN(151)) * time.Millisecond
}
func (node *Node) resetTimer() {
	if(!node.timer.Stop()){
		select{
			case <-node.timer.C:
			default:
		}
	}
	node.timer.Reset(randomTimeout())
}
func (node *Node) Run(){
	fmt.Println("run");
	for {
		select {
			case msg := <-node.inbox:
			   node.handlemsg(msg);
			case <-node.timer.C:
			   if node.state != "leader" {
							
				   fmt.Println("Timeout → election")
				   node.startElection()
				   node.resetTimer()
				}	
		}
	}
}
// func StartNode(id string, nodes []string) *Node{
// 	fmt.Println("Starting node", id)
// 	ln, err := net.Listen("tcp", ":"+id)
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	leaderState := &LeaderState{
// 		NextIndex: make(map[string]int),
// 		MatchIndex: make(map[string]int),
// 		Sentlen: make(map[string]int),
// 	}
// 	node := &Node{
// 		mp : make(map[string]string),
// 		currentTerm: 0,
// 		state: "follower",
// 		isLeader: false,
// 		currentLeader: "",
// 		votesRecieved : set.NewSet(),
// 		votedFor: "",
// 		servers: nodes,
// 		commitIndex: 0,
// 		lastApplied: 0,
// 		id: id,
// 		ShardId: shardId,
// 		logs: []Log{},
// 		leaderState: leaderState,
// 		timer: time.NewTimer(randomTimeout()),
// 		inbox: make(chan Envelope,100),
// 		peerConn: make(map[string]net.Conn),
// 	}
// 	go node.Listen(ln);
// 	node.Run();
// 	fmt.Println("Node", id, "started");
// 	return node;
// }
func getNodes(filename string) []string {
	var nodes []string;
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return nodes;
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for {
		node, err := reader.ReadString('\n')
		node = strings.TrimSpace(node)
		if node != "" {
			nodes = append(nodes, node)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
	}
	return nodes;
}
func main() {
	var id string;
	var port string;
	var shardId string;
	var filename string;
	flag.StringVar(&id, "id", "", "node id")
	flag.StringVar(&port, "port", "", "port")
	flag.StringVar(&shardId, "shardId", "", "shard id")
	flag.StringVar(&filename, "filename", "", "filename")
	
	flag.Parse()
	fmt.Println("id", id, "port", port, "shardId", shardId, "filename", filename)
	if id == "" {
		fmt.Println("id is required")
		return
	}
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println(err)
		return
	}
	nodes := getNodes(filename);
	for _, nodeId := range nodes {
		fmt.Println("nodeId", nodeId)
	}
	leaderState := &LeaderState{
		NextIndex: make(map[string]int),
		MatchIndex: make(map[string]int),
		Sentlen: make(map[string]int),
	}
	node := &Node{
		mp : make(map[string]string),
		currentTerm: 0,
		state: "follower",
		isLeader: false,
		currentLeader: "",
		votesRecieved : set.NewSet(),
		votedFor: "",
		servers: nodes,
		ShardId : shardId,	
		commitIndex: 0,
		lastApplied: 0,
		LbId: "",
		id: "raft"+id+":"+port,
		logs: []Log{},
		leaderState: leaderState,
		timer: time.NewTimer(randomTimeout()),
		inbox: make(chan Envelope,100),
		peerConn: make(map[string]net.Conn),
	}
	go node.Listen(ln);
	node.Run();
}