package book

import "simulator/contract"

// LevelSnapshot is a value-copy of one depth row for cross-package callers
// (the shard, which assembles the tape's DepthSnapshot from two TopN calls).
// Kept in this package so book does not import emit — the seam is a value type
// and the shard does the (identical-shape) conversion.
type LevelSnapshot struct {
	Price    contract.Ticks
	TotalQty contract.Qty
}

// TopN returns up to n depth rows for the given side, best price first
// (highest for Buy, lowest for Sell — the direction the underlying SideBook
// walks). An empty side returns an empty slice; a side with fewer than n
// levels returns all of them. n must be > 0.
//
// Only live resting quantity is exposed: TotalQty is Σ Remaining at the
// level, maintained by the book's mutators (EXCHANGE_11 invariant 2).
func (book *Book) TopN(side contract.Side, n int) []LevelSnapshot {
	if n <= 0 {
		panic("book.TopN: n must be > 0")
	}
	snapshots := make([]LevelSnapshot, 0, n)
	book.sideBook(side).Iterate(func(level *PriceLevel) bool {
		snapshots = append(snapshots, LevelSnapshot{
			Price:    level.Price,
			TotalQty: level.TotalQty,
		})
		return len(snapshots) < n
	})
	return snapshots
}
