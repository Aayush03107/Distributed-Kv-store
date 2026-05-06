package main

import (
	"fmt";
	"net";
	"flag";
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"io"
	"math/rand/v2"
	// "bufio"
	// "bytes"
	//"strings"
	"strconv"
	
);
type Envelope struct {
	Type    string
	LbId string
	Message json.RawMessage
}
type Message struct{
	Id string;
	Resp []byte;
	Port string;
	Counter int;
};
type Lb struct{
	id string;
	inbox chan Envelope;
	Shardinfo map[uint64] []string;
}
type clientResponse struct {
	Response []byte
	Enduser string
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
	if err != nil {
		return Envelope{}, err
	}
	return msg, nil
}
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

// 		if(string(lenline[0]) != "$"){
// 			return nil, fmt.Errorf("expected '$', got %s", lenline[0]);
// 		}
// 		lenStr := strings.TrimSpace(string(lenline[1:]))
// 		li, err := strconv.Atoi(lenStr)
// 		if err != nil {
// 					return nil, err
// 		}
// 		temp:= make([]byte,li+2);
// 		_,err = reader.Read(temp);
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

func (lb *Lb) handlemsg(msg Envelope){
	if(msg.Type == "ClientCommand"){
		var mesg Message;
		json.Unmarshal(msg.Message,&mesg);
		fmt.Println(mesg);
		fmt.Println(mesg.Resp);
		parts,err := Parse(mesg.Resp);
		if(err != nil){
			panic(err);
		}
		// parts := strings.Fields(string(parsed))
		if len(parts) < 2 {
			fmt.Println("Invalid command format")
			return
		}
		key := string(parts[1]);

		fmt.Println(key);
		hash := md5.Sum([]byte(key));
		num := binary.BigEndian.Uint64(hash[:8]);
		fmt.Println(num);
		index := num % uint64(3);
		nodelist := lb.Shardinfo[index];
		fmt.Println(nodelist);
		data,err := json.Marshal(mesg);
		if(err != nil){
			panic(err);
		};
		fmt.Println(lb.id);
		meg := Envelope{Type : "ClientCommand", LbId : lb.id, Message : data};
		for {
			idx := rand.IntN(len(nodelist));
			nodeid := nodelist[idx];
			conn, err := net.Dial("tcp",nodeid);
			if( err == nil){
				SendMessage(conn, meg);
				conn.Close()
				return ;
			}
			panic(err);
		}
	}else if msg.Type == "ClientResponse" {
		var message clientResponse ;
		json.Unmarshal(msg.Message,&message);
		fmt.Println("clientresponse: ",string(message.Response))
		fmt.Println(message.Enduser);
		for {
			conn, err := net.Dial("tcp",message.Enduser);
			if(err != nil){
				panic(err);
			}
			conn.Write(message.Response);
			conn.Close();
			break;
		}
	}
}

func (lb *Lb) handleMessages(){
	for{
		select {
			case msg := <-lb.inbox:
				lb.handlemsg(msg);
		}
	}
}

func main() {
	lbport := flag.String("port","7000","Lb port");
	flag.Parse();
	lb := &Lb{
		id : "lb:"+*lbport,
		inbox : make(chan Envelope,100),
		Shardinfo : map[uint64][]string{
			0 : {"raft1:5001","raft2:5002","raft3:5003","raft4:5004","raft5:5005"},
			1 : {"raft6:5006","raft7:5007","raft8:5008","raft9:5009","raft10:5010"},
			2 : {"raft11:5011","raft12:5013","raft13:5014","raft14:5015","raft15:5018"},
		},
	}
	ln, err := net.Listen("tcp","lb:"+*lbport);
	if err != nil {
		panic(err)
	}
	fmt.Println("Load balancer started on port", *lbport);
	go lb.handleMessages()
	for {
		conn, err := ln.Accept();
		if err != nil {
			continue
		}
		go func(c net.Conn){
			msg, err := decode(c);
			if err != nil {
				return
			}
			lb.inbox <- msg
		}(conn)
	}
}