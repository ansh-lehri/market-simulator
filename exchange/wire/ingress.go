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

// run reads the edge inbound channel, hashes each message's Instrument, and
// forwards to the owning shard's inbound (EXCHANGE_10 §5). Pure router: no
// validation, no Kind inspection — Submit and Cancel route identically. Runs
// as one long-lived goroutine for the session; the for-range loop terminates
// when the edge inbound channel is closed by its writer.
//
// On shutdown (edge inbound closed) it closes every shard inbound so each
// shard's run loop drains and exits. Ingress is the sole writer of the
// shardIn channels during its run, so closing them here is the correct
// party's responsibility.
func (ingress *Ingress) run() {
	for msg := range ingress.inbound {
		shardID := hashInstrument(msg.Instrument) % uint32(ingress.numShards)
		ingress.shardIn[shardID] <- msg
	}
	for _, shardChannel := range ingress.shardIn {
		close(shardChannel)
	}
}
