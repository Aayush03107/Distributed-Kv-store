package main

import (
	"fmt";
	"KV/Backend/node"
	"encoding/binary"
	"encoding/json"
	"io"
	"flag"
	"net"
	"strings";
	"os"
	"bufio";
)
type Envelope struct {
	Type    string
	ShardId string
	Message json.RawMessage
}
type Shard struct{
 	id string;
	inbox chan Envelope;
	nodes []string;
	lbid string;
 };
 
type Message struct {
	Id      string
	Resp    []byte
	Port    string
	Counter int
}
type clientResponse struct {
	Response []byte
	Enduser string
}

func (shard *Shard) makeconn(node string){
	Node.StartNode(node,shard.nodes);
};
func SendMessage(conn net.Conn, msg Envelope) {	
	data, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	err = binary.Write(conn, binary.BigEndian, int32(len(data)))
	if err != nil {
		panic(err)
	}
	_, err = conn.Write(data)
	if err != nil {
		panic(err)
	}
}

// func send(node string, msg Envelope) {
// 	go func() {
//         conn, err := net.Dial("tcp", ":"+node)
//         if err != nil {
//             return
//         }
//         defer conn.Close()
//         SendMessage(conn, msg)
//     }();
// }
func getNodes(id string) []string {
	fd, err := os.Open("nodes" + id+ ".txt");
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	reader := bufio.NewReader(fd)
	var nodes []string
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
	return nodes
}
func (shard *Shard) startCluster(){
	for _,s := range(shard.nodes){
		go shard.makeconn(s);
	}
}
func (shard *Shard) sendtocluster(msg Envelope){
	var message Message;
	typeStr := msg.Type
	fmt.Println("Shard",msg.ShardId);
	json.Unmarshal(msg.Message,&message);
	fmt.Println("Shard sendtocluster", message.Port);
	
	jsonMsg, err := json.Marshal(message);
	if(err != nil){
		panic(err);
	}
	msgbyte := Envelope{Type: typeStr,ShardId: shard.lbid, Message: jsonMsg};
	for{
		for _,s := range(shard.nodes){
			conn,err := net.Dial("tcp",":"+s);
			if(err == nil){
				SendMessage(conn,msgbyte);
				conn.Close()
				return ;
			}
		}
	}
}

func(shard *Shard) respondToClient(msg Envelope){
	// var message clientResponse ;
	// json.Unmarshal(msg.Message,&message);
	//fmt.Println(message.Enduser);
	for{
		conn,err := net.Dial("tcp",":"+msg.ShardId);
		if(err == nil){
			SendMessage(conn,msg);
			conn.Close()
			return ;
		}
	}
}


func (shard *Shard) requesthandle(msg Envelope){
	fmt.Println("Shard requesthandle", msg.Type, msg.ShardId);
	if msg.Type == "ClientCommand"{
		fmt.Println("hihi");
		
		shard.sendtocluster(msg);
	}else if msg.Type == "ClientResponse"{
		shard.respondToClient(msg);
	}
}
func (shard *Shard) handlemsg(){
	for{
		select{
			case msg := <-shard.inbox:
			  fmt.Println("iam");
				shard.lbid = msg.ShardId;
				shard.requesthandle(msg);
		}
	}

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
func main() {
	fmt.Println("hello world");
	id:= flag.String("port","6000","enter port");
	flag.Parse();
	nodes := getNodes(*id);
	shard := &Shard{
		id: *id,
		inbox: make(chan Envelope,100),
		nodes: nodes,
		lbid: "",
	}
	shard.startCluster();

	for{
		ln, err := net.Listen("tcp",":" + *id);
		if(err != nil){
			panic(err);
		};
		for{
			conn,err := ln.Accept();
			msg,err := decode(conn);
			defer conn.Close()
			fmt.Println("shard",msg);
			if(err != nil){
				panic(err);
			};
			fmt.Println(":12");
			shard.inbox <- msg;
			go shard.handlemsg();
		}
	}
}
