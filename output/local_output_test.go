package output

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestLocalOutput_Put(t *testing.T) {
	_ = os.RemoveAll("/tmp/logtubed-local-test")

	var o *LocalOutput
	var err error
	if o, err = NewLocalOutput(LocalOutputOptions{Enabled: true, Dir: "/tmp/logtubed-local-test"}); err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if err = o.PutOperation(&testOperation{
		index: "debug-test-test-2019-01-02",
		body:  []byte(`{"crid":"","env":"test","hostname":"","message":"hello, world","project":"test","timestamp":"2019-01-02T03:04:05.000000006+08:00","topic":"debug","via":""}`),
	}); err != nil {
		t.Fatal(err)
	}

	var buf []byte
	if buf, err = ioutil.ReadFile("/tmp/logtubed-local-test/debug-test-test-2019-01-02.log"); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "[2019-01-02T03:04:05.000000006+08:00] () [] hello, world \r\n" {
		t.Fatal("not equal", "\""+string(buf)+"\"")
	}
}
