package wire

import "simulator/contract"

// Ingress routes the one edge inbound channel to the P per-shard inbound
// channels by hash(Instrument) % numShards (EXCHANGE_10 §5). Pure router:
// it does not validate, admit, or inspect Kind. One goroutine will drive
// its run loop in Stage 2; Stage 1 constructs the struct but does not
// start it.
type Ingress struct {
	inbound   <-chan contract.InboundMessage
	shardIn   []chan<- contract.InboundMessage
	numShards int
}

func newIngress(
	inbound <-chan contract.InboundMessage,
	shardIn []chan<- contract.InboundMessage,
) *Ingress {
	return &Ingress{
		inbound:   inbound,
		shardIn:   shardIn,
		numShards: len(shardIn),
	}
}
