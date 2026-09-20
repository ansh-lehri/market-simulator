package account

import (
	"errors"
	"reflect"
	"testing"
)

// -----------------------------------------------------------------------------
// MoveCash — the 5 legal edges, each across 3 amount regimes.
// -----------------------------------------------------------------------------

func TestMoveCash_LegalEdges(t *testing.T) {
	tests := []struct {
		name          string
		start         Cash
		from, to      Bucket
		amount        int64
		wantAfter     Cash
	}{
		// Available → Reserved (reserve/buy)
		{"reserve buy: partial", Cash{Available: 500, Reserved: 100}, Available, Reserved, 200, Cash{Available: 300, Reserved: 300}},
		{"reserve buy: drain source", Cash{Available: 500, Reserved: 100}, Available, Reserved, 500, Cash{Available: 0, Reserved: 600}},
		// Reserved → Settled (buy convert)
		{"buy convert: partial", Cash{Reserved: 400, Settled: 50}, Reserved, Settled, 100, Cash{Reserved: 300, Settled: 150}},
		{"buy convert: drain source", Cash{Reserved: 400, Settled: 50}, Reserved, Settled, 400, Cash{Reserved: 0, Settled: 450}},
		// Reserved → Available (release / release_excess)
		{"release: partial", Cash{Reserved: 300, Available: 100}, Reserved, Available, 50, Cash{Reserved: 250, Available: 150}},
		{"release: drain source", Cash{Reserved: 300, Available: 100}, Reserved, Available, 300, Cash{Reserved: 0, Available: 400}},
		// Available → Bank (bank-refill)
		{"bank-refill: partial", Cash{Available: 1000, Bank: 200}, Available, Bank, 400, Cash{Available: 600, Bank: 600}},
		{"bank-refill: drain source", Cash{Available: 1000, Bank: 200}, Available, Bank, 1000, Cash{Available: 0, Bank: 1200}},
		// Bank → Available (bank-pull)
		{"bank-pull: partial", Cash{Bank: 800, Available: 50}, Bank, Available, 300, Cash{Bank: 500, Available: 350}},
		{"bank-pull: drain source", Cash{Bank: 800, Available: 50}, Bank, Available, 800, Cash{Bank: 0, Available: 850}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			cash := testCase.start
			sumBefore := cash.Available + cash.Reserved + cash.Settled + cash.Bank
			if err := cash.MoveCash(testCase.from, testCase.to, testCase.amount); err != nil {
				t.Fatalf("MoveCash returned %v, want nil", err)
			}
			if !reflect.DeepEqual(cash, testCase.wantAfter) {
				t.Errorf("cash after = %+v, want %+v", cash, testCase.wantAfter)
			}
			sumAfter := cash.Available + cash.Reserved + cash.Settled + cash.Bank
			if sumBefore != sumAfter {
				t.Errorf("§0.6 conservation broken: sumBefore=%d sumAfter=%d", sumBefore, sumAfter)
			}
		})
	}
}

// Insufficient: amount > *source on every legal edge fires ErrInsufficient,
// state untouched. Covers the "no negative balance by construction" contract
// for transfers.
func TestMoveCash_InsufficientOnEveryLegalEdge(t *testing.T) {
	tests := []struct {
		name     string
		start    Cash
		from, to Bucket
		amount   int64
		wantHave int64
	}{
		{"Available→Reserved overshoot", Cash{Available: 100, Reserved: 0}, Available, Reserved, 200, 100},
		{"Reserved→Settled overshoot", Cash{Reserved: 50, Settled: 0}, Reserved, Settled, 51, 50},
		{"Reserved→Available overshoot", Cash{Reserved: 5, Available: 0}, Reserved, Available, 6, 5},
		{"Available→Bank overshoot", Cash{Available: 10, Bank: 100}, Available, Bank, 11, 10},
		{"Bank→Available overshoot", Cash{Bank: 0, Available: 100}, Bank, Available, 1, 0},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			cash := testCase.start
			err := cash.MoveCash(testCase.from, testCase.to, testCase.amount)
			if !errors.Is(err, ErrInsufficientFunds) {
				t.Errorf("errors.Is(err, ErrInsufficientFunds) = false, err=%v", err)
			}
			var detail ErrInsufficient
			if !errors.As(err, &detail) {
				t.Fatalf("errors.As failed to recover ErrInsufficient, err=%v", err)
			}
			if detail.Bucket != testCase.from || detail.Have != testCase.wantHave || detail.Want != testCase.amount {
				t.Errorf("detail = %+v, want {%s %d %d}", detail, testCase.from, testCase.wantHave, testCase.amount)
			}
			if !reflect.DeepEqual(cash, testCase.start) {
				t.Errorf("cash mutated on error: got %+v, want %+v", cash, testCase.start)
			}
		})
	}
}

// Illegal edges: spot-check the load-bearing structural rules (bank not
// reservable, can't un-settle a fill) plus every same-bucket no-op.
func TestMoveCash_IllegalEdges(t *testing.T) {
	tests := []struct {
		name     string
		from, to Bucket
	}{
		{"Settled→Reserved (un-settle nonsense)", Settled, Reserved},
		{"Bank→Reserved (bank not reservable)", Bank, Reserved},
		{"Settled→Available (no un-settling)", Settled, Available},
		{"Available→Settled (must go via Reserved)", Available, Settled},
		{"Bank→Settled", Bank, Settled},
		{"Reserved→Bank", Reserved, Bank},
		{"Settled→Bank", Settled, Bank},
		{"Bank→Settled", Bank, Settled},
		{"same-bucket Available", Available, Available},
		{"same-bucket Reserved", Reserved, Reserved},
		{"same-bucket Settled", Settled, Settled},
		{"same-bucket Bank", Bank, Bank},
	}
	// Rich start state — if any illegal edge slipped through, state would change
	// and the DeepEqual snapshot would catch it regardless of message shape.
	start := Cash{Available: 1000, Reserved: 500, Settled: 300, Bank: 200}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			cash := start
			err := cash.MoveCash(testCase.from, testCase.to, 10)
			if !errors.Is(err, ErrIllegalOperation) {
				t.Errorf("errors.Is(err, ErrIllegalOperation) = false, err=%v", err)
			}
			var detail ErrIllegalMove
			if !errors.As(err, &detail) {
				t.Fatalf("errors.As failed to recover ErrIllegalMove, err=%v", err)
			}
			if detail.From != testCase.from || detail.To != testCase.to {
				t.Errorf("detail = %+v, want {%s %s}", detail, testCase.from, testCase.to)
			}
			if !reflect.DeepEqual(cash, start) {
				t.Errorf("cash mutated on error: got %+v, want %+v", cash, start)
			}
		})
	}
}

func TestMoveCash_NonPositiveAmount(t *testing.T) {
	tests := []struct {
		name   string
		amount int64
	}{
		{"zero", 0},
		{"negative", -1},
		{"large negative", -1_000_000},
	}
	start := Cash{Available: 500, Reserved: 100, Settled: 50, Bank: 300}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			cash := start
			err := cash.MoveCash(Available, Reserved, testCase.amount)
			if !errors.Is(err, ErrNonPositiveAmount) {
				t.Errorf("errors.Is(err, ErrNonPositiveAmount) = false, err=%v", err)
			}
			if !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("errors.Is(err, ErrInvalidArgument) = false — category chain broken, err=%v", err)
			}
			if !reflect.DeepEqual(cash, start) {
				t.Errorf("cash mutated on error: got %+v, want %+v", cash, start)
			}
		})
	}
}

// Ordering matters: MoveCash validates amount BEFORE legality BEFORE
// sufficiency. Assert that ordering so future edits don't shuffle guards.
func TestMoveCash_ValidationOrder(t *testing.T) {
	start := Cash{Available: 5, Reserved: 0, Settled: 0, Bank: 0}

	// amount=0 on an ILLEGAL edge with INSUFFICIENT source → amount check wins.
	err := start.MoveCash(Settled, Reserved, 0)
	if !errors.Is(err, ErrNonPositiveAmount) {
		t.Errorf("expected amount check first, got %v", err)
	}

	// amount=100 on an ILLEGAL edge with INSUFFICIENT source → legality wins over sufficiency.
	err = start.MoveCash(Settled, Reserved, 100)
	if !errors.Is(err, ErrIllegalOperation) {
		t.Errorf("expected legality check before sufficiency, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// CreditCash
// -----------------------------------------------------------------------------

func TestCreditCash_LegalTarget(t *testing.T) {
	tests := []struct {
		name         string
		start        Cash
		amount       int64
		wantAvailable int64
	}{
		{"credit 1 into fresh account", Cash{}, 1, 1},
		{"credit large amount", Cash{Available: 100}, 999_999, 1_000_099},
		{"credit onto negative Available (Option B → still just adds)", Cash{Available: -500}, 800, 300},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			cash := testCase.start
			if err := cash.CreditCash(Available, testCase.amount); err != nil {
				t.Fatalf("CreditCash returned %v, want nil", err)
			}
			if cash.Available != testCase.wantAvailable {
				t.Errorf("Available = %d, want %d", cash.Available, testCase.wantAvailable)
			}
			// Other buckets must not shift.
			if cash.Reserved != testCase.start.Reserved || cash.Settled != testCase.start.Settled || cash.Bank != testCase.start.Bank {
				t.Errorf("non-target bucket mutated: got %+v, start %+v", cash, testCase.start)
			}
		})
	}
}

func TestCreditCash_IllegalTargets(t *testing.T) {
	start := Cash{Available: 100, Reserved: 100, Settled: 100, Bank: 100}
	for _, target := range []Bucket{Reserved, Settled, Bank} {
		t.Run(target.String(), func(t *testing.T) {
			cash := start
			err := cash.CreditCash(target, 50)
			if !errors.Is(err, ErrIllegalOperation) {
				t.Errorf("errors.Is(err, ErrIllegalOperation) = false, err=%v", err)
			}
			var detail ErrIllegalCreditTarget
			if !errors.As(err, &detail) {
				t.Fatalf("errors.As failed to recover ErrIllegalCreditTarget, err=%v", err)
			}
			if detail.To != target {
				t.Errorf("detail.To = %s, want %s", detail.To, target)
			}
			if !reflect.DeepEqual(cash, start) {
				t.Errorf("cash mutated on error: got %+v, want %+v", cash, start)
			}
		})
	}
}

func TestCreditCash_NonPositiveAmount(t *testing.T) {
	start := Cash{Available: 100}
	for _, amount := range []int64{0, -1, -1_000_000} {
		cash := start
		err := cash.CreditCash(Available, amount)
		if !errors.Is(err, ErrNonPositiveAmount) {
			t.Errorf("amount=%d: errors.Is(err, ErrNonPositiveAmount) = false, err=%v", amount, err)
		}
		if !reflect.DeepEqual(cash, start) {
			t.Errorf("amount=%d: cash mutated on error", amount)
		}
	}
}

// -----------------------------------------------------------------------------
// DebitCash — Option B: NO sufficiency gate. Every path where amount>0 and
// source==Available succeeds, even if the result drives Available negative.
// -----------------------------------------------------------------------------

func TestDebitCash_OptionB_AlwaysAppliesOnAvailable(t *testing.T) {
	tests := []struct {
		name         string
		start        Cash
		amount       int64
		wantAvailable int64
	}{
		{"amount < Available (stays positive)", Cash{Available: 500}, 100, 400},
		{"amount == Available (lands on 0)", Cash{Available: 500}, 500, 0},
		{"amount > Available from positive (drives negative)", Cash{Available: 100}, 250, -150},
		{"amount from zero (drives negative)", Cash{Available: 0}, 50, -50},
		{"amount from already-negative (deeper negative)", Cash{Available: -100}, 30, -130},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			cash := testCase.start
			if err := cash.DebitCash(Available, testCase.amount); err != nil {
				t.Fatalf("DebitCash returned %v — Option B forbids ANY error on this path", err)
			}
			if cash.Available != testCase.wantAvailable {
				t.Errorf("Available = %d, want %d", cash.Available, testCase.wantAvailable)
			}
			if cash.Reserved != testCase.start.Reserved || cash.Settled != testCase.start.Settled || cash.Bank != testCase.start.Bank {
				t.Errorf("non-target bucket mutated: got %+v, start %+v", cash, testCase.start)
			}
		})
	}
}

// Positive assertion: NO input on the legal path returns ErrInsufficientFunds.
// This locks Option B — a future edit adding a sufficiency guard would fail here.
func TestDebitCash_NeverReturnsInsufficient(t *testing.T) {
	for _, amount := range []int64{1, 100, 1_000_000_000} {
		for _, startAvailable := range []int64{-1000, 0, 100, 500, 999_999_999} {
			cash := Cash{Available: startAvailable}
			err := cash.DebitCash(Available, amount)
			if errors.Is(err, ErrInsufficientFunds) {
				t.Errorf("Option B violated: DebitCash(%d) with Available=%d returned ErrInsufficientFunds", amount, startAvailable)
			}
			if err != nil {
				t.Errorf("unexpected error DebitCash(%d) with Available=%d: %v", amount, startAvailable, err)
			}
		}
	}
}

func TestDebitCash_IllegalSources(t *testing.T) {
	start := Cash{Available: 100, Reserved: 100, Settled: 100, Bank: 100}
	for _, source := range []Bucket{Reserved, Settled, Bank} {
		t.Run(source.String(), func(t *testing.T) {
			cash := start
			err := cash.DebitCash(source, 50)
			if !errors.Is(err, ErrIllegalOperation) {
				t.Errorf("errors.Is(err, ErrIllegalOperation) = false, err=%v", err)
			}
			var detail ErrIllegalBurnSource
			if !errors.As(err, &detail) {
				t.Fatalf("errors.As failed to recover ErrIllegalBurnSource, err=%v", err)
			}
			if detail.From != source {
				t.Errorf("detail.From = %s, want %s", detail.From, source)
			}
			if !reflect.DeepEqual(cash, start) {
				t.Errorf("cash mutated on error: got %+v, want %+v", cash, start)
			}
		})
	}
}

func TestDebitCash_NonPositiveAmount(t *testing.T) {
	start := Cash{Available: 100}
	for _, amount := range []int64{0, -1, -1_000_000} {
		cash := start
		err := cash.DebitCash(Available, amount)
		if !errors.Is(err, ErrNonPositiveAmount) {
			t.Errorf("amount=%d: errors.Is(err, ErrNonPositiveAmount) = false, err=%v", amount, err)
		}
		if !reflect.DeepEqual(cash, start) {
			t.Errorf("amount=%d: cash mutated on error", amount)
		}
	}
}
