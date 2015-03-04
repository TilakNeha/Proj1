// JSONRPCServer
// Author : Neha Tilak

package main

import (
        "fmt"
        "net/rpc"
        "net/rpc/jsonrpc"
        "os"
        "net"
        "io/ioutil"
        "strings"
        "encoding/json"
)


// Structs and type definitions
type JsonRpc int

type KeyRel struct {
        Key, Relation string
}

// Global Variables
var m map[KeyRel]map[string]interface{}
var n  map[string]int
var filename string

// Functions

// This function inserts new triplet in the DICT3.
// Returns true/ false depending on the whether the
// operation was successful/ unsuccessful.

func insert(k string, r string, v string) bool{
     kr := KeyRel{k,r}
     _,ok := m[kr]
     if ok == false {
	      bt := []byte (v)
	      var newjson map[string]interface{}
	      if err := json.Unmarshal(bt, &newjson); err != nil {
	           panic(err)
	      }    
	      m[kr] = newjson
	      return true;
     }
     return false;
}

// This function returns the json object corresponding
// to given key-relation pair.

func lookup(keyrel []string) []string {
     result := keyrel
     k := KeyRel{keyrel[0], keyrel[1]}
        if m[k] != nil {
           op,_ := json.Marshal(m[k])
           result = append(result,string(op))
        } else {
           result = append(result,"null")
        }
        
 
     return result
}

// This function adds or updates a triplet in the DICT3
// Does not return anything

func insertOrUpdate(k string, r string, v string) {
     kr := KeyRel{k,r}
     bt := []byte (v)
     var newjson map[string]interface{}
     if err := json.Unmarshal(bt, &newjson); err != nil {
          panic(err)
     }
     m[kr] = newjson     
}

// This function deletes a triplet from DICT3

func deleteo(keyrel []string) {
        k := KeyRel{keyrel[0], keyrel[1]}
	delete(m, k)
}

// This function returns a list of unique keys in the DICT3

func listKeys() []string {
     var keylist []string
     var tm map[string]int
     tm = make(map[string]int)
     for k := range m {
         _,ok := tm[k.Key]
         if ok == false {
             tm[k.Key] = 1;
             keylist = append(keylist,k.Key);
         }
     }
             
     return keylist;
}

// This function returns a list of key-relation pairs
// in DICT3

func listIDs() [][]string {
        var keyrel [][]string
        for k := range m {
            var temp []string;
            temp = append(temp, k.Key)
            temp = append(temp,k.Relation)
            keyrel = append(keyrel,temp)
        }
        return keyrel
}

// This function stores the contents of the map in the persistent
// storage container. The server program exits from RPCDo.

func shutdown() {
        os.Remove(filename);
        f,_ := os.Create(filename)
        for k := range m {
            f.WriteString(k.Key + "\n")
            f.WriteString(k.Relation + "\n")
            st,_ := json.Marshal(m[k])
            f.WriteString(string(st) + "\n")
        }
     //   os.Exit(0);
}

// This is the main RPC procedure available to the client.
// The function call appropriate methods depending on the
// contents of the request json message.

func (j *JsonRpc) RPCDo(remreq map[string]interface{}, remrep *map[string]interface{}) error {
	str := remreq["method"]
         
        switch str {
		case "lookup" :
                      barr,_ := json.Marshal(remreq["params"])
                      pstr := string(barr)
                      pstr = strings.Replace(pstr,"[","",-1)
                      pstr = strings.Replace(pstr,"]","",-1)
                      pstr = strings.Replace(pstr,"\"","",-1)
                      parr := strings.Split(pstr,",")
                      q := []string{strings.Trim(parr[0]," "), strings.Trim(parr[1]," ")}
                      ans := lookup(q);
                      newm := map[string]interface{}{"result" : ans, "error" : "null"}
                      *remrep = newm
                      return nil
                case "insert" , "insertOrUpdate":
                      barr,_ := json.Marshal(remreq["params"])
                      pstr := string(barr)
                      pstr = strings.Replace(pstr,"[","",-1)
                      pstr = strings.Replace(pstr,"]","",-1)
                      var first string
                      var i int
                      for i = 0; string(pstr[i]) != ","; i= i+1 {
                           if string(pstr[i]) != "\"" {
                              first = first + string(pstr[i])
                           }
                      }
                      first = strings.Trim(first," ")
                      i = i + 1;
                      var second string
                      for i = i; string(pstr[i]) != ","; i= i+1 {
                           if string(pstr[i]) != "\"" {
                              second = second + string(pstr[i])
                           }
                      }
                      second = strings.Trim(second," " )
                      i = i+ 1;
                      var third string
                      for i = i; i < len(pstr); i= i+1 {
                           third = third + string(pstr[i])
                      }
                      third = strings.Trim(third, " ")
                      
                      var newm map[string]interface{}
                      if str == "insert" {
                      ans := insert(first,second,third)
                      newm = map[string]interface{}{"result" : ans, "error" : "null"}
                      } else {
                      insertOrUpdate(first,second,third)
                      newm = map[string]interface{}{"error" : "null"}
                      }
                      *remrep = newm
                      return nil
                case "delete":
                      barr,_ := json.Marshal(remreq["params"])
                      pstr := string(barr)
                      pstr = strings.Replace(pstr,"[","",-1)
                      pstr = strings.Replace(pstr,"]","",-1)
                      pstr = strings.Replace(pstr,"\"","",-1)
                      parr := strings.Split(pstr,",")
                      q := []string{strings.Trim(parr[0]," "), strings.Trim(parr[1]," ")}

                      deleteo(q)
                      newm := map[string]interface{}{"error" : "null"}
                      *remrep = newm
                      return nil
                case "listKeys":
                      ans := listKeys();
                      newm := map[string]interface{}{"result" : ans, "error" : "null"}
                      *remrep = newm;
                      
                case "listIDs":
                      ans := listIDs();
                      newm := map[string]interface{}{"result" : ans, "error" : "null"}
                      *remrep = newm;
                case "shutdown":
                      newm := map[string]interface{} {"result" : "true"}
                      *remrep = newm;
                      defer os.Exit(0);
                      shutdown();
                      
                      return nil
                default :
                      newm := map[string]interface{}{"result" : "invalid"}
                      *remrep = newm
        }
        //remrep["result"] = "invalid";
        return nil                         
} 

// This function takes data from the persistent storage file and
// creates a map of triplets.

func MakeTriples(fn string) {
     m = make(map[KeyRel]map[string]interface{})
     data , err := ioutil.ReadFile(fn)
     if err != nil {
         panic(err)
     }
     s := string(data)
     sarr := strings.Split(s, "\n")
     length := len(sarr) - 1
     for i := 0; i <= length -3; i = i+ 3 {
           tempkr := KeyRel{sarr[i],sarr[i+1]}
           bt := []byte (sarr[i+2])
           var newjson map[string]interface{}
           if err := json.Unmarshal(bt, &newjson); err != nil {
                panic(err)
           }
           m[tempkr] = newjson

     }
     fmt.Println("Server is up !")
     fmt.Println("Initial collection in DICT3:")
     fmt.Println(m)
}

func main() {

        args := os.Args[1:]
        data , err := ioutil.ReadFile(args[0])
        if err != nil {
            panic(err)
        }
        s := string(data)
        bt := []byte (s)
        var Cjson map[string]interface{}
        if err := json.Unmarshal(bt, &Cjson); err != nil {
             panic(err)
        }

        Fjson := Cjson["persistentStorageContainer"].(map[string]interface{})
        filename = Fjson["file"].(string)
        MakeTriples(filename)
        proto := Cjson["protocol"].(string);
        port := Cjson["ipAddress"].(string) + ":" + Cjson["port"].(string);

        jr := new(JsonRpc)
        rpc.Register(jr)

        tcpAddr, err := net.ResolveTCPAddr(proto, port)
        checkError(err)

        listener, err := net.ListenTCP(proto, tcpAddr)
        checkError(err)

        /* This works:
        rpc.Accept(listener)
        */
        /* and so does this:
         */
        for {
                conn, err := listener.Accept()
                if err != nil {
                        continue
                }
                jsonrpc.ServeConn(conn)
        }

}

func checkError(err error) {
        if err != nil {
                fmt.Println("Fatal error ", err.Error())
                os.Exit(1)
        }
}
        
