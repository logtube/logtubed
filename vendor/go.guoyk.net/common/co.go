package common

type Co interface {
	Take()
	Return()
}

type co struct {
	tokens chan interface{}
}

func (c *co) Take() {
	<-c.tokens
}

func (c *co) Return() {
	c.tokens <- nil
}

type nopCo struct{}

func (c *nopCo) Take() {}

func (c *nopCo) Return() {}

func NewCo(limit int) Co {
	if limit < 1 {
		return &nopCo{}
	} else {
		tokens := make(chan interface{}, limit)
		for i := 0; i < limit; i++ {
			tokens <- nil
		}
		return &co{tokens: tokens}
	}
}
