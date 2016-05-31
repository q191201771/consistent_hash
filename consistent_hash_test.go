package consistent_hash

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"
)

func TestError(t *testing.T) {
	ch := Default()
	_, err := ch.Get("aaa")
	if err != errNoNode {
		t.Fatalf("err: [%s] expected [%s]", err.Error(), errNoNode.Error())
	}
}

func TestBasic(t *testing.T) {
	ch := Default()
	ch.Add("node2")
	ch.Add("node3")
	ch.Add("node4")
	ch.Add("node5")

	keys := []string{
		"apple",
		"banana",
		"car",
		"dog",
		"eat",
		"fuck",
		"give",
		"hive",
		"ing",
		"jet",
		"ken",
		"long",
		"man",
		"no",
	}
	fmt.Println("-----First:")
	for _, v := range keys {
		node, _ := ch.Get(v)
		fmt.Println(v, ":", node)
	}
	fmt.Println("-----Second:")
	for _, v := range keys {
		node, _ := ch.Get(v)
		fmt.Println(v, ":", node)
	}
	ch.Add("node6")
	ch.Add("node1")
	fmt.Println("-----Third")
	for _, v := range keys {
		node, _ := ch.Get(v)
		fmt.Println(v, ":", node)
	}
	ch.Remove("node2")
	ch.Remove("node5")
	fmt.Println("-----Fourth")
	for _, v := range keys {
		node, _ := ch.Get(v)
		fmt.Println(v, ":", node)
	}
}

func TestHuge(t *testing.T) {
	var keys []string
	for i := 0; i < 10000; i++ {
		keys = append(keys, UUID())
	}

	nodeCount := make(map[string]int)
	ch := New(1000)
	ch.Add("ajsk1")
	ch.Add("12ejio1")
	ch.Add("2r02")
	ch.Add("nk")
	for _, key := range keys {
		node, _ := ch.Get(key)
		if _, exist := nodeCount[node]; !exist {
			nodeCount[node] = 0
		}
		nodeCount[node]++
	}
	fmt.Println("-----Huge First")
	for k, v := range nodeCount {
		fmt.Println(k, ":", v)
	}

	for k := range nodeCount {
		delete(nodeCount, k)
	}
	ch.Remove("2r02")
	ch.Remove("ajsk1")
	for _, key := range keys {
		node, _ := ch.Get(key)
		if _, exist := nodeCount[node]; !exist {
			nodeCount[node] = 0
		}
		nodeCount[node]++
	}
	fmt.Println("-----Huge Second")
	for k, v := range nodeCount {
		fmt.Println(k, ":", v)
	}

	for k := range nodeCount {
		delete(nodeCount, k)
	}
	ch.Add("2r")
	ch.Add("a1")
	for _, key := range keys {
		node, _ := ch.Get(key)
		if _, exist := nodeCount[node]; !exist {
			nodeCount[node] = 0
		}
		nodeCount[node]++
	}
	fmt.Println("-----Huge Third")
	for k, v := range nodeCount {
		fmt.Println(k, ":", v)
	}
}

func TestNodes(t *testing.T) {
	ch := Default()
	ch.Add("node1")
	ch.Add("node2")
	kv := ch.Nodes()
	if len(kv) != 2 {
		t.Fatalf("nodes [%v] expected len []", len(kv))
	}
	for _, node := range kv {
		if node != "node1" && node != "node2" {
			t.Fatal(kv)
		}
	}
}

func TestClear(t *testing.T) {
	ch := Default()
	ch.Add("node1")
	ch.Add("node2")
	ch.Clear()
	if len(ch.Nodes()) != 0 {
		t.Fatal(ch.Nodes())
	}
}

func UUID() string {
	out, _ := exec.Command("uuidgen").Output()
	return strings.TrimSuffix(strings.TrimSuffix(string(out), "\n"), "\r\n")
}
