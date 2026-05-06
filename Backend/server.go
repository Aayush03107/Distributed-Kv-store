package main

import (
	"fmt";
	"net/http";
	"encoding/json"
)

func home(w http.ResponseWriter, r *http.Request){
	fmt.Fprintf(w,"Hello from myend")
}
func users(w http.ResponseWriter, r *http.Request){
	users := []string{"Aayush","Sharma","hi"};
	w.Header().Set("Content-Type","application/json");
	json.NewEncoder(w).Encode(users);
}
type User struct{
	Name string;
	Age int;
}
func createUser(w http.ResponseWriter, r *http.Request){
	var user User;
	json.NewDecoder(r.Body).Decode(&user);
	w.Header().Set("Content-Type","application/json");
	json.NewEncoder(w).Encode(user);
}

func main(){
	http.HandleFunc("/",home);
	http.HandleFunc("/users", users)
	http.HandleFunc("/create-user",createUser);
	fmt.Println("Server listening on port 8080");
	
	http.ListenAndServe(":8080",nil);
}