package book

import (
	"math"
	"sort"
	"testing"

	"pgregory.net/rapid"

	"simulator/contract"
)

// --- RB structural checker -----------------------------------------------

// assertRBInvariants walks the tree and panics-through-testing.T on any of the
// four RB invariants + BST ordering being violated. Runs after every mutation
// in the rapid property test; also usable directly against internal state.
func assertRBInvariants(t *testing.T, tree *rbTreeSide) {
	t.Helper()

	if tree.sentinel.color != black {
		t.Fatal("sentinel must be black")
	}
	if tree.root.color != black {
		t.Fatal("root must be black (RB invariant 2)")
	}

	// Recursive walk returns the black-height of the subtree rooted at node.
	// A leaf slot (sentinel) has black-height 0 (or 1 counting itself; we use
	// "edges to a black nil" — pick one convention and stick to it).
	var walk func(node *rbNode, minKey, maxKey contract.Ticks) int
	walk = func(node *rbNode, minKey, maxKey contract.Ticks) int {
		if node == tree.sentinel {
			return 1 // count the black sentinel toward black-height
		}
		if node.price < minKey || node.price > maxKey {
			t.Fatalf("BST ordering violated at price %d (bounds [%d, %d])", node.price, minKey, maxKey)
		}
		if node.color == red {
			// RB invariant 4: red node must not have red children.
			if node.left.color == red || node.right.color == red {
				t.Fatalf("red node at price %d has a red child", node.price)
			}
		}
		leftBH := walk(node.left, minKey, node.price-1)
		rightBH := walk(node.right, node.price+1, maxKey)
		if leftBH != rightBH {
			t.Fatalf("black-height mismatch at price %d: left=%d right=%d", node.price, leftBH, rightBH)
		}
		if node.color == black {
			return leftBH + 1
		}
		return leftBH
	}
	walk(tree.root, math.MinInt64, math.MaxInt64)

	// Structural: parent pointers must be consistent.
	var verifyParents func(node *rbNode)
	verifyParents = func(node *rbNode) {
		if node == tree.sentinel {
			return
		}
		if node.left != tree.sentinel && node.left.parent != node {
			t.Fatalf("broken parent link: node %d has left child %d whose parent is not it", node.price, node.left.price)
		}
		if node.right != tree.sentinel && node.right.parent != node {
			t.Fatalf("broken parent link: node %d has right child %d whose parent is not it", node.price, node.right.price)
		}
		verifyParents(node.left)
		verifyParents(node.right)
	}
	verifyParents(tree.root)
}

// treeSize walks and counts non-sentinel nodes (independent of cached side.size,
// so it catches size-drift bugs).
func treeSize(tree *rbTreeSide) int {
	count := 0
	var walk func(node *rbNode)
	walk = func(node *rbNode) {
		if node == tree.sentinel {
			return
		}
		count++
		walk(node.left)
		walk(node.right)
	}
	walk(tree.root)
	return count
}

// --- targeted structural tests -------------------------------------------

func TestRBTree_MonotonicAscendingInsertsStayBalanced(t *testing.T) {
	tree := newRBTreeSide(Ascending)
	const insertCount = 512
	for price := contract.Ticks(1); price <= insertCount; price++ {
		tree.Insert(price)
		assertRBInvariants(t, tree)
	}
	if tree.size != insertCount {
		t.Fatalf("size = %d, want %d", tree.size, insertCount)
	}
	if got := treeSize(tree); got != insertCount {
		t.Fatalf("walked size = %d, want %d", got, insertCount)
	}
	// Best on an ascending tree over 1..N is 1.
	best, _ := tree.Best()
	if best.Price != 1 {
		t.Fatalf("Best = %d, want 1", best.Price)
	}
}

func TestRBTree_MonotonicDescendingInsertsStayBalanced(t *testing.T) {
	tree := newRBTreeSide(Descending)
	const insertCount = 512
	for price := contract.Ticks(insertCount); price >= 1; price-- {
		tree.Insert(price)
		assertRBInvariants(t, tree)
	}
	best, _ := tree.Best()
	if best.Price != contract.Ticks(insertCount) {
		t.Fatalf("Best = %d, want %d", best.Price, insertCount)
	}
}

func TestRBTree_InsertRemoveRoundTripEmptiesTree(t *testing.T) {
	tree := newRBTreeSide(Ascending)
	prices := []contract.Ticks{10021, 10018, 10022, 10019, 10020, 10025, 10017, 10023}
	for _, price := range prices {
		tree.Insert(price)
	}
	assertRBInvariants(t, tree)
	for _, price := range prices {
		tree.RemoveLevel(price)
		assertRBInvariants(t, tree)
	}
	if tree.size != 0 {
		t.Fatalf("size after removing all = %d, want 0", tree.size)
	}
	if tree.root != tree.sentinel {
		t.Fatal("root must be sentinel after removing all")
	}
	if _, ok := tree.Best(); ok {
		t.Fatal("Best on drained tree must be (_, false)")
	}
}

// --- rapid property test -------------------------------------------------

// TestRBTree_Property runs a random mix of Insert/RemoveLevel/LevelAt calls
// against both directions and verifies, after every op:
//
//   - RB structural invariants (colors, black-height, BST order, parent links).
//   - Iterate produces the reference set in sorted order per direction.
//   - Best matches the reference extreme.
//   - LevelAt hits iff price is in the reference set.
//   - Cached size matches walked size matches reference len.
//   - Insert at an existing price returns the same *PriceLevel pointer
//     (get-or-create contract).
func TestRBTree_Property(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		direction := Direction(rapid.IntRange(0, 1).Draw(rt, "direction"))
		tree := newRBTreeSide(direction)
		// Reference: price -> the *PriceLevel we saw for it (used to check that
		// Insert idempotence returns the SAME pointer, not just equal state).
		reference := map[contract.Ticks]*PriceLevel{}

		opCount := rapid.IntRange(0, 200).Draw(rt, "opCount")
		for step := 0; step < opCount; step++ {
			op := rapid.SampledFrom([]string{"insert", "remove", "lookup"}).Draw(rt, "op")
			price := contract.Ticks(rapid.IntRange(1, 20).Draw(rt, "price"))

			switch op {
			case "insert":
				gotLevel := tree.Insert(price)
				if existing, ok := reference[price]; ok {
					if gotLevel != existing {
						t.Fatalf("Insert on existing price %d returned a different pointer", price)
					}
				} else {
					if gotLevel == nil || gotLevel.Price != price {
						t.Fatalf("Insert(%d) returned bad level %v", price, gotLevel)
					}
					reference[price] = gotLevel
				}
			case "remove":
				tree.RemoveLevel(price)
				delete(reference, price)
			case "lookup":
				gotLevel, ok := tree.LevelAt(price)
				existing, want := reference[price]
				if ok != want {
					t.Fatalf("LevelAt(%d): ok=%v want=%v", price, ok, want)
				}
				if ok && gotLevel != existing {
					t.Fatalf("LevelAt(%d) pointer mismatch vs reference", price)
				}
			}

			assertRBInvariants(t, tree)

			if tree.size != len(reference) {
				t.Fatalf("cached size=%d, reference len=%d", tree.size, len(reference))
			}
			if walked := treeSize(tree); walked != len(reference) {
				t.Fatalf("walked size=%d, reference len=%d", walked, len(reference))
			}

			// Reference expected iteration order.
			expected := make([]contract.Ticks, 0, len(reference))
			for price := range reference {
				expected = append(expected, price)
			}
			if direction == Ascending {
				sort.Slice(expected, func(a, b int) bool { return expected[a] < expected[b] })
			} else {
				sort.Slice(expected, func(a, b int) bool { return expected[a] > expected[b] })
			}
			got := collectIterationOrder(tree)
			if !equalTicks(got, expected) {
				t.Fatalf("Iterate order mismatch: got %v, want %v", got, expected)
			}

			best, ok := tree.Best()
			if len(expected) == 0 {
				if ok {
					t.Fatalf("Best on empty must be (_, false); got price %d", best.Price)
				}
			} else {
				if !ok {
					t.Fatal("Best on non-empty must be present")
				}
				if best.Price != expected[0] {
					t.Fatalf("Best.Price = %d, want %d", best.Price, expected[0])
				}
			}
		}
	})
}
