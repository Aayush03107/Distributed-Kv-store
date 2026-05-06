package main
import(
	"fmt";
	"net";
	"flag";
	"os";
	"bufio";
	"encoding/binary";
    "encoding/json";
	"io";
	"slices";
	"cmp";
	"strconv";
	"bytes";
	"strings";
	"sync"
);

var mp map[string]string;
var seen map[string]bool;

var counter int;
var mu sync.Mutex;
var currentTerm int;
var fileName string;

type Message struct{
	Id string;
	Resp []byte;
	Port string;
	Counter int;
};


func Parse(s []byte) ([]byte, error) {
	reader := bufio.NewReader(bytes.NewReader(s));
	line, err := reader.ReadBytes('\n');
	if err != nil {
		return nil, err;
	}
	if(line[0] == '+'){
		return bytes.TrimSpace(line[1:]), nil
	}
	if(line[0] != '*'){
		return nil, fmt.Errorf("expected '+', got %s", line[0]);
	};
	countStr := strings.TrimSpace(string(line[1:]))
	count, err := strconv.Atoi(countStr)
	if err != nil {
			return nil, err
	}
	var res []byte;
	for i := 0 ; i < int(count) ; i++{
		lenline,err := reader.ReadBytes('\n');
		if(lenline[0] != '$'){
			return nil, fmt.Errorf("expected '$', got %s", lenline[0]);
		}
		lenStr := strings.TrimSpace(string(lenline[1:]))
		li, err := strconv.Atoi(lenStr)
		if err != nil {
					return nil, err
		}
		temp:= make([]byte,li+2);
		_,err = io.ReadFull(reader, temp);
		if(err != nil){
			return nil,err;
		}
		res = append(res, temp[:li]...);
		res = append(res, ' ')
	}
	return bytes.TrimSpace(res),nil;
}

func serialse(msg Message) []byte{
	//fmt.Println(msg.Resp);
	data,err := json.Marshal(msg);
	if(err != nil){
		panic(err);
	}
	//fmt.Println(reflect.TypeOf(data));
	return data;
}

func Commit(msg Message,port string) (bool,error){
	fileName = "log"+port;
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644);
	if(err != nil){
		return false,err;
	};
	defer fd.Close();
	mesg := serialse(msg);
	w := bufio.NewWriter(fd);
	w.Write(mesg);
	w.WriteString("\n");
	w.Flush();
	fd.Sync();
	return true,nil;
};

func deserialise(msg []byte) Message{
	var mesg Message;
	reader := bufio.NewReader(bytes.NewReader(msg));
	message,err := reader.ReadBytes('\n');
	json.Unmarshal(message,&mesg);
	if(err != nil && err != io.EOF){
		panic(err);
	}
	return mesg;
}

func RecoveryHelper(text []byte){
	if (len(text) == 0){
		return ;
	}
	cmd := string(text[0]);
	if (cmd == "SET" && len(text) >= 3) {
		key := string(text[1])
		val := string(text[2])
		mu.Lock();
		mp[key] = val
		mu.Unlock();
	}
	if (cmd == "DEL" && len(text) >= 2) {
		key := string(text[1])
		mu.Lock();
		delete(mp, key)
		mu.Unlock();
	}
}

func Recover(port string) (int,error){
	fd,err := os.Open("log"+port);
	if(err != nil){
		return 1,err;
	}
	defer fd.Close();
	
	reader := bufio.NewReader(fd);
	for{
		msg,err := reader.ReadBytes('\n');
		mesg := deserialise(msg);
		if(err != nil){
			if(err == io.EOF){
				return 0,nil;
			}
			return 1,nil;
		};
		instr,err := Parse(mesg.Resp);
		counter = max(counter,mesg.Counter);

		RecoveryHelper(instr);
	};
	return 0,nil;
}
func handleFormat(input []byte) []byte {
	str := strings.TrimSpace(string(input))

	if len(str) > 0 && (str[0] == '*' || str[0] == '+' || str[0] == '$') {
		return input
	}
	text := strings.Fields(str)
	if len(text) == 0 {
		return nil
	}
	cmd := text[0]
	totlen := len(text)
	if cmd == "OK" || cmd == "Deleted" || cmd == "Does" {
		return []byte("+" + str + "\r\n")
	}
	key := text[1]
	ksize := len(key)
	if cmd == "SET" {
		val := text[2:]
		value := strings.Join(val, " ");
		valsize := len(value)
		final := "*" + strconv.Itoa(totlen) + "\r\n" + "$" + strconv.Itoa(len(cmd)) + "\r\n" + cmd + "\r\n" + "$" + strconv.Itoa(ksize) + "\r\n" + key + "\r\n" + "$" + strconv.Itoa(valsize) + "\r\n" + value + "\r\n"
		return []byte(final)
	}else if cmd == "GET" {
		final := "*" + strconv.Itoa(totlen) + "\r\n" + "$" + strconv.Itoa(len(cmd)) + "\r\n" + cmd + "\r\n" + "$" + strconv.Itoa(ksize) + "\r\n" + key + "\r\n"
		return []byte(final)
	}else if cmd == "DEL" {
		final := "*" + strconv.Itoa(totlen) + "\r\n" + "$" + strconv.Itoa(len(cmd)) + "\r\n" + cmd + "\r\n" + "$" + strconv.Itoa(ksize) + "\r\n" + key + "\r\n"
		return []byte(final)
	}
	return nil;
}
func Decode(conn net.Conn) (Message, error) {
	var msg Message;
	var length int32;
	err := binary.Read(conn, binary.BigEndian, &length);
	if err != nil {
		return msg, err
	}
	buf := make([]byte,length);
	_,err = io.ReadFull(conn,buf);
	if err != nil {
		if err == io.EOF {
			return msg, nil
		}
		return msg, err
	}
	err = json.Unmarshal(buf,&msg);
	return msg, nil
}

func SendMessage(conn net.Conn, msg Message) {
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

func createAck(resp []byte) []byte{
	final := "+" + string(resp) + "\r\n"
	return []byte(final)
};

func sendAck(msg Message, conn net.Conn, resp string) {
	ack := createAck([]byte(resp));
	SendMessage(conn, Message{Id: msg.Id, Resp: ack, Counter: msg.Counter, Port: msg.Port});
}

func handleConnection(conn net.Conn, mp map[string]string, port string){
	defer conn.Close();
	var rmessages []Message;
	for{
		counter++;
		msg,err := Decode(conn);
		if err != nil {
			if err == io.EOF {
				return
			}
			panic(err)
		}
		msg.Counter = max(msg.Counter,counter);
		rmessages = append(rmessages,msg);
		counterComp := func(m1, m2 Message ) int{
			return cmp.Compare(m1.Counter,m2.Counter);
		}
		slices.SortFunc(rmessages,counterComp);
		for _,Msg := range rmessages{
			res := Msg.Resp;
			id := Msg.Id;
			//recport := Msg.Port;
			if(seen[id]) {
				sendAck(Msg, conn, "OK");
				return
			}
			mu.Lock();
			seen[id] = true;
			mu.Unlock();
			resp,err := Parse(res);
			fmt.Println(string(resp));
			if err != nil {
				if err == io.EOF {
					return
				}
			}
			parts := strings.Fields(string(resp))
			if len(parts) == 0 {
				continue
			}

			cmd := parts[0]
			if cmd != "SET" && cmd != "GET" && cmd != "DEL" {
				fmt.Println("ACK:", string(resp));
				return;
			}
			fmt.Println(string(resp));
			mu.Lock();
			_,er := Commit(msg,port);
			mu.Unlock();
			rmessages = rmessages[1:];
			if(er != nil){
				panic(er);
			}
			if(er == nil){
				key := parts[1]
				val := ""
				if len(parts) > 2 {
					val = parts[2]
				}
				mesg := Message{
					Id:   id,
					Resp: resp,
					Port: port,
					Counter : counter,
				}
				if(cmd == "SET"){
					mu.Lock();
					mp[key] = val;
					mu.Unlock();
					sendAck(mesg,conn,"OK");
				}else if(cmd == "GET"){
					counter++;
					mu.Lock();
					val := mp[key];
					mu.Unlock();
					sendAck(mesg,conn,val);
				}else if(cmd == "DEL"){
					mu.Lock();
					_,ok := mp[key];
					mu.Unlock();
					if (ok){
						mu.Lock();
						delete(mp,key);
						mu.Unlock();
					    sendAck(mesg,conn,"deleted");
					}else{
						sendAck(mesg,conn, "no key exists");
					}
				}else{
					conn.Write([]byte("Commit Error\n"));
				}
			 }
		}
		
	}
}

func main(){
	port := flag.String("port","5100","port to run server");
	flag.Parse();
	ln,err := net.Listen("tcp", ":"+ *port);
	if(err != nil){
		panic(err);
	};
	mp = make(map[string]string);
	seen = make(map[string]bool);
	counter = 0;
	currentTerm = 0;
	code,err := Recover(*port);
	if(err != nil){
		if(code ==1){
		}
	};
	for{
		conn,err  := ln.Accept();
		if(err != nil){
			panic(err);
		}
		go handleConnection(conn,mp,*port);
	}
}