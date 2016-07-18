package consistent

import "fmt"
import "reflect"
import "testing"

func TestInit(t *testing.T) {
	_ = NewConsistent()
	_ = NewConsistentWithN(200)
	_ = NewConsistentWithHash(130, func([]byte) uint64 {
		return 0
	})
}

func TestNodeOperation(t *testing.T) {
	c := NewConsistent()

	c.AddNodes([]string{"192.168.1.1", "192.168.1.2", "192.168.1.3", "192.168.1.4", "192.168.1.5"})

	if c.NodeNumber() != 5 {
		t.Errorf("Wrong NodeNumber(), exp: 2, got %v\n", c.NodeNumber())
	}

	c.RemoveNode("192.168.1.3")

	if c.NodeNumber() != 4 {
		t.Errorf("Wrong NodeNumber(), exp: 1, got %v\n", c.NodeNumber())
	}

	c.RemoveNodes([]string{"192.168.1.3", "192.168.1.2", "192.168.1.5"})

	if c.NodeNumber() != 2 {
		t.Errorf("Wrong NodeNumber(), exp: 1, got %v\n", c.NodeNumber())
	}

	testHasNode := []struct {
		Addr string
		Exp  bool
		Msg  string
	}{
		{"192.168.1.1", true, "Can't found 192.168.1.1"},
		{"192.168.1.2", false, "Found 192.168.1.2"},
		{"192.168.1.3", false, "Found 192.168.1.3"},
	}

	for _, v := range testHasNode {
		if c.HasNode(v.Addr) != v.Exp {
			t.Errorf("HasNode err: %v\n", v.Msg)
		}
	}

}

func TestConsistentHashing(t *testing.T) {
	c := NewConsistent()
	c.AddNodes([]string{"node1", "node2", "node3", "node4", "node5"})

	testGetNode := []struct {
		Key string
		Exp string
		Msg string
	}{
		{"Abc", "node1", "Wrong mapping Abc -> node1"},
		{"xxx", "node1", "Wrong mapping xxx -> node2"},
		{"1111234567", "node5", "Wrong mapping 1111234567 -> node5"},
		{"okbnqeobla;d", "node2", "Wrong mapping okbnqeobla;d -> node2"},
	}

	for _, v := range testGetNode {
		if node, err := c.GetNode(v.Key); err != nil || node != v.Exp {
			t.Errorf("GetNode err: %v, exp: %v, got: %v\n", v.Msg, v.Exp, node)
		}
	}

	testGetNnode := []struct {
		Key string
		N   int
		Exp []string
		Err error
		Msg string
	}{
		{"Abc", 1, []string{"node1"}, nil, "Get 1 Wrong mapping Abc -> node1"},
		{"xxx", 2, []string{"node1", "node2"}, nil, "Get 2 Wrong mapping xxx -> node1, 2"},
		{"okbnqeobla;d", 6, []string{}, consistentError{Msg: "Query N is greater than total nodes"},
			"Get N greater than total node is invalid"},
	}

	for _, v := range testGetNnode {
		if node, err := c.GetNNode(v.Key, v.N); err != v.Err || !reflect.DeepEqual(node, v.Exp) {
			t.Errorf("GetNNode err: %v, exp: %v, got: %v\n", v.Msg, v.Exp, node)
		}
	}

	c.RemoveNodes([]string{"node1", "node2", "node3", "node4", "node5"})

	testEmpty := []struct {
		Key string
		Exp error
		Msg string
	}{
		{"okbnqeobla;d", consistentError{Msg: "Empty! No nodes."}, "Empty! No nodes."},
	}

	for _, v := range testEmpty {
		if _, err := c.GetNode(v.Key); err != v.Exp {
			t.Errorf("GetNode err: %v, exp: %v, got: %v\n", v.Msg, v.Exp, err)
		}
	}

}

// AddNodes and RemoveNodes is positive to the list of nodes, so we skip testing these methods
func BenchmarkAddAndRemove(b *testing.B) {
	b.ReportAllocs()
	c := NewConsistent()
	node := "Node"
	for i := 0; i < b.N; i++ {
		c.AddNode(node)
		c.RemoveNode(node)
	}
}

func BenchmarkGetNNode(b *testing.B) {
	b.ReportAllocs()
	c := NewConsistent()
	c.AddNodes([]string{"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9", "n10", "n11", "n12"})
	for i := 0; i < b.N; i++ {
		c.GetNNode(fmt.Sprintf("%v", i), 5)
	}
}
