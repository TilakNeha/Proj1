// JSONRpcCLient
// Author : Neha Tilak

package main

import (
        "net/rpc/jsonrpc"
        "fmt"
        "log"
        "encoding/json"
        "os"
        "bufio"
        "io/ioutil"
)

// type definitions and structures.
type Request struct {
        req map[string]interface{}
}

type Reply struct {
        res map[string]interface{}
}

// Client's main function.
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
             fmt.Println("From here")
             panic(err)
        }
        proto := Cjson["protocol"].(string);
        service := Cjson["ipAddress"].(string) + ":" + Cjson["port"].(string);

        client, err := jsonrpc.Dial(proto, service)
        if err != nil {
                log.Fatal("dialing:", err)
        }
  
        scanner := bufio.NewScanner(os.Stdin)
        var command string
        for i :=0; command != "exit"; i = i {
            fmt.Println("Enter request command:")
            scanner.Scan();
            command = scanner.Text();
            fmt.Println(command)
            if command != "exit" {
            barr := []byte(command)
            var commandjson map[string]interface{}
            if err = json.Unmarshal(barr,&commandjson); err != nil {
                   panic(err)
            }
            var respo map[string]interface{}
            err = client.Call("JsonRpc.RPCDo", commandjson, &respo)
            if commandjson["method"] != "shutdown" && err != nil {
                    log.Fatal("RPC Do error: ", err)
            }
            op,_ := json.Marshal(respo)       
            fmt.Println("Response:")
            fmt.Println(string(op))
            }
         }
}
