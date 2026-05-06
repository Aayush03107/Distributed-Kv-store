package main
import(
	"fmt";
	"net";
	//"bufio";
	//"flag"
	//"os";
	"encoding/binary";
    "encoding/json";
    "github.com/google/uuid"
    "github.com/gin-contrib/cors"
	"strings";
	"strconv";
	//"bytes";
	"io";
	"github.com/gin-gonic/gin"
)

type Message struct{
	Id string;
	Resp []byte;
	Port string;
	Counter int;
};
type Envelope struct {
	Type    string
	LbId string
	Message json.RawMessage
}
type Request struct {
	Cmd string;
	Key string;
	Val string;
}
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
func handleFormat(text []string) []byte {
	cmd := text[0];
	totlen := len(text);
	key := text[1];
	ksize := len(key);
	if cmd == "SET" {
		val := text[2:]
		value := strings.Join(val, " ");
		valsize := len(value)
		final := "*" + strconv.Itoa(totlen-1) + "\r\n" + "$" + strconv.Itoa(len(cmd)) + "\r\n" + cmd + "\r\n" + "$" + strconv.Itoa(ksize) + "\r\n" + key + "\r\n" + "$" + strconv.Itoa(valsize) + "\r\n" + value + "\r\n"
		return []byte(final)
	}else if cmd == "GET" {
		final := "*" + strconv.Itoa(totlen-1) + "\r\n" + "$" + strconv.Itoa(len(cmd)) + "\r\n" + cmd + "\r\n" + "$" + strconv.Itoa(ksize) + "\r\n" + key + "\r\n"
		return []byte(final)
	}else if cmd == "DEL" {
		final := "*" + strconv.Itoa(totlen-1) + "\r\n" + "$" + strconv.Itoa(len(cmd)) + "\r\n" + cmd + "\r\n" + "$" + strconv.Itoa(ksize) + "\r\n" + key + "\r\n"
		return []byte(final)
	}
	return nil;
}
func ParseOutput(s []byte)  []string {
	fmt.Println(string(s));
	var ans []string;
	for _,v := range s{
		ans = append(ans,string(v));
	}
	return ans;
}
func SendMessage( msg Envelope,conn net.Conn) {
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
func main(){
	targetNodePort := "lb:7001";
	
	clientListener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	defer clientListener.Close()
	_, clientPort, err := net.SplitHostPort(clientListener.Addr().String())
	if err != nil {
		panic(err)
	}
	fmt.Printf("Client started. Listening for Leader replies on port: %s\n", clientPort);
	counter := 0
	// reader := bufio.NewReader(os.Stdin);
	router := gin.Default();
	router.Use(cors.Default())
	router.POST("/", func(c *gin.Context) {
		var req Request
		if err := c.BindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}					
		var text string
		text += req.Cmd + "\r\n\t";
		if req.Key != "" {
			text += req.Key + "\r\n\t"
		}
		if req.Val != "" {
			text += req.Val + "\r\n\t"
		}
		words := strings.Split(text, "\r\n\t")
		fmt.Println(words);
		fmt.Println(words[1]);
		fmt.Println(len(words[1]));
		// for i, word := range words{
		// 	words[i] = word + "\r\n\t"
		// }
		if len(words) == 0 {
			c.JSON(400, gin.H{"error": "empty request"})
			return
		}
		counter++
		resp := handleFormat(words)
		fmt.Println(string(resp));
		op, _:= Parse(resp);
		for _,u:= range op{
			fmt.Println(string(u));
		}
		msg := Message{
			Id:      (uuid.New()).String(),
			Resp:    resp,
			Port:    "client:"+clientPort, 
			Counter: counter,
		}
		mesg, err := json.Marshal(msg)
		if err != nil {
			panic(err)
		}
		msgbyte := Envelope{Type: "ClientCommand", LbId: targetNodePort, Message: mesg}
		conn, err := net.Dial("tcp",targetNodePort)
		if err != nil {
			fmt.Println("Error connecting to node:", err)
			c.JSON(500, gin.H{
				"message": "Error connecting to node",
			});
			return;
		}
		SendMessage(msgbyte, conn)
		conn.Close()
		respConn, err := clientListener.Accept()
		if err != nil {
			fmt.Println("Error accepting reply from leader:", err)
			c.JSON(500, gin.H{
				"message": "1 11 Error accepting reply from leader",
			});
			return;
		}
		buf := make([]byte, 1024)
		n, err := respConn.Read(buf)
		if err != nil && err != io.EOF {
			fmt.Println("Error reading from leader:", err)
			respConn.Close()
			c.JSON(500, gin.H{
				"message": "Error reading from leader",
			});
			return;
		}
		res := ParseOutput(buf[1:n])
		fmt.Println("hit res ", res )
		if err != nil {
			fmt.Println(err);
			c.JSON(500, gin.H{
				"message": "Parse error",
			})
		} else {
			c.JSON(200, gin.H{
				"Response": res,
			})
		}
		respConn.Close()
	});
	router.Run(":7002");
	// for {
	// 	fmt.Print(">> ")
	// 	text, _ := reader.ReadString('\n');
	// 	words := strings.Fields(text)
	// 	fmt.Println(words);
	// 	if len(words) == 0 {
	// 		continue
	// 	}
	// 	counter++
	// 	resp := handleFormat(words)
	// 	msg := Message{
	// 		Id:      (uuid.New()).String(),
	// 		Resp:    resp,
	// 		Port:    clientPort, 
	// 		Counter: counter,
	// 	}
	// 	mesg, err := json.Marshal(msg)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	msgbyte := Envelope{Type: "ClientCommand", ShardId: *targetNodePort, Message: mesg}
	// 	conn, err := net.Dial("tcp", ":"+*targetNodePort)
	// 	if err != nil {
	// 		fmt.Println("Error connecting to node:", err)
	// 		continue
	// 	}
	// 	SendMessage(msgbyte, conn)
	// 	conn.Close()
	// 	respConn, err := clientListener.Accept()
	// 	if err != nil {
	// 		fmt.Println("Error accepting reply from leader:", err)
	// 		continue
	// 	}
	// 	buf := make([]byte, 1024)
	// 	n, err := respConn.Read(buf)
	// 	if err != nil && err != io.EOF {
	// 		fmt.Println("Error reading from leader:", err)
	// 		respConn.Close()
	// 		continue
	// 	}
	// 	res, err := Parse(buf[:n])
	// 	if err != nil {
	// 		fmt.Println("Parse error:", err)
	// 	} else {
	// 		fmt.Println(string(res))
	// 	}
	// 	respConn.Close()
	// }
}