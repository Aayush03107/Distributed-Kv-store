package main
import (
	"fmt"
	"bufio"
	"bytes"
)

func createAck(resp []byte) []byte{
	lt := len(resp);
	buff := make([]byte,4+lt);
	buff[0] = byte('+')
	buff[1] = byte('$');
	buff[2] = byte(lt);
	buff = append(buff, resp...);	
	buff = append(buff, '\n');
	return buff;
};

func handleAck(s []byte) []byte {
	fmt.Println(s);
	if(s[0] != '$') {
		return nil;
	}
	lt := s[1];
	buff := make([]byte, lt+2);
	buff = append(buff, s[2:]...);
	fmt.Println(string(buff));
	return buff;
}
func Parse(s []byte) ([]byte, error) {
	reader := bufio.NewReader(bytes.NewReader(s));
	line, err := reader.ReadBytes('\n');
	if err != nil {
		return nil, err;
	}
	if(line[0] == '+'){
		fmt.Println(line[1:]);
		buff := line[1:];
		res := handleAck(buff);
		return res, nil;
	}
	if(line[0] != '*'){
		return nil, fmt.Errorf("expected '+', got %s", line[0]);
	};
	
	count := line[1];
	var res []byte;
	for i := 0 ; i < int(count) ; i++{
		lenline,err := reader.ReadBytes('\n');
		if(lenline[0] != '$'){
			return nil,fmt.Errorf("expected '$', got %s", lenline);
		}
		li := lenline[1];
		temp:= make([]byte,li+2);
		_,err = reader.Read(temp);
		if(err != nil){
			return nil,err;
		}
		res = append(res, temp[:li]...);
	}
	return res,nil;
}

func main() {

	fmt.Println(string(buff));
	res, err := Parse(buff);
	if err != nil {
		fmt.Println(err);
		return;
	}
	fmt.Println(string(res));
}