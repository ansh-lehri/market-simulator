package book

import "simulator/contract"

// Red-black tree keyed by contract.Ticks, storing one PriceLevel per key.
// The tree itself is direction-agnostic (always sorted ascending on price);
// Direction only flips which extremum Best() reads and which end Iterate walks
// from. Keeping the comparator invariant makes the fix-up algebra identical to
// the CLRS reference and avoids per-side code duplication.
//
// A single sentinel node (always black) stands in for every NIL leaf slot and
// for the parent of the root. This matches CLRS 3rd ed. chapter 13 and keeps
// the fix-up branches free of nil checks.

type rbColor uint8

const (
	red rbColor = iota
	black
)

type rbNode struct {
	price  contract.Ticks
	level  *PriceLevel
	color  rbColor
	left   *rbNode
	right  *rbNode
	parent *rbNode
}

type rbTreeSide struct {
	sentinel  *rbNode
	root      *rbNode
	direction Direction
	size      int
}

func newRBTreeSide(direction Direction) *rbTreeSide {
	sentinel := &rbNode{color: black}
	return &rbTreeSide{
		sentinel:  sentinel,
		root:      sentinel,
		direction: direction,
	}
}

func (side *rbTreeSide) Best() (*PriceLevel, bool) {
	if side.root == side.sentinel {
		return nil, false
	}
	var node *rbNode
	if side.direction == Ascending {
		node = side.minSubtree(side.root)
	} else {
		node = side.maxSubtree(side.root)
	}
	return node.level, true
}

func (side *rbTreeSide) LevelAt(price contract.Ticks) (*PriceLevel, bool) {
	node := side.find(price)
	if node == side.sentinel {
		return nil, false
	}
	return node.level, true
}

func (side *rbTreeSide) Insert(price contract.Ticks) *PriceLevel {
	if existing := side.find(price); existing != side.sentinel {
		return existing.level
	}
	level := newLevel(price)
	node := &rbNode{
		price:  price,
		level:  level,
		color:  red,
		left:   side.sentinel,
		right:  side.sentinel,
		parent: side.sentinel,
	}
	side.bstInsert(node)
	side.insertFixup(node)
	side.size++
	return level
}

func (side *rbTreeSide) RemoveLevel(price contract.Ticks) {
	node := side.find(price)
	if node == side.sentinel {
		return
	}
	side.deleteNode(node)
	side.size--
}

func (side *rbTreeSide) Iterate(visit func(*PriceLevel) bool) {
	if side.direction == Ascending {
		side.walkAsc(side.root, visit)
	} else {
		side.walkDesc(side.root, visit)
	}
}

// --- BST-level helpers ---------------------------------------------------

func (side *rbTreeSide) find(price contract.Ticks) *rbNode {
	node := side.root
	for node != side.sentinel {
		switch {
		case price < node.price:
			node = node.left
		case price > node.price:
			node = node.right
		default:
			return node
		}
	}
	return side.sentinel
}

func (side *rbTreeSide) minSubtree(node *rbNode) *rbNode {
	for node.left != side.sentinel {
		node = node.left
	}
	return node
}

func (side *rbTreeSide) maxSubtree(node *rbNode) *rbNode {
	for node.right != side.sentinel {
		node = node.right
	}
	return node
}

func (side *rbTreeSide) walkAsc(node *rbNode, visit func(*PriceLevel) bool) bool {
	if node == side.sentinel {
		return true
	}
	if !side.walkAsc(node.left, visit) {
		return false
	}
	if !visit(node.level) {
		return false
	}
	return side.walkAsc(node.right, visit)
}

func (side *rbTreeSide) walkDesc(node *rbNode, visit func(*PriceLevel) bool) bool {
	if node == side.sentinel {
		return true
	}
	if !side.walkDesc(node.right, visit) {
		return false
	}
	if !visit(node.level) {
		return false
	}
	return side.walkDesc(node.left, visit)
}

func (side *rbTreeSide) bstInsert(node *rbNode) {
	parent := side.sentinel
	current := side.root
	for current != side.sentinel {
		parent = current
		if node.price < current.price {
			current = current.left
		} else {
			current = current.right
		}
	}
	node.parent = parent
	switch {
	case parent == side.sentinel:
		side.root = node
	case node.price < parent.price:
		parent.left = node
	default:
		parent.right = node
	}
}

func (side *rbTreeSide) rotateLeft(node *rbNode) {
	rightChild := node.right
	node.right = rightChild.left
	if rightChild.left != side.sentinel {
		rightChild.left.parent = node
	}
	rightChild.parent = node.parent
	switch {
	case node.parent == side.sentinel:
		side.root = rightChild
	case node == node.parent.left:
		node.parent.left = rightChild
	default:
		node.parent.right = rightChild
	}
	rightChild.left = node
	node.parent = rightChild
}

func (side *rbTreeSide) rotateRight(node *rbNode) {
	leftChild := node.left
	node.left = leftChild.right
	if leftChild.right != side.sentinel {
		leftChild.right.parent = node
	}
	leftChild.parent = node.parent
	switch {
	case node.parent == side.sentinel:
		side.root = leftChild
	case node == node.parent.right:
		node.parent.right = leftChild
	default:
		node.parent.left = leftChild
	}
	leftChild.right = node
	node.parent = leftChild
}

// --- RB fix-ups (CLRS 13.3, 13.4) ---------------------------------------

func (side *rbTreeSide) insertFixup(node *rbNode) {
	for node.parent.color == red {
		grandparent := node.parent.parent
		if node.parent == grandparent.left {
			uncle := grandparent.right
			if uncle.color == red {
				node.parent.color = black
				uncle.color = black
				grandparent.color = red
				node = grandparent
				continue
			}
			if node == node.parent.right {
				node = node.parent
				side.rotateLeft(node)
			}
			node.parent.color = black
			node.parent.parent.color = red
			side.rotateRight(node.parent.parent)
		} else {
			uncle := grandparent.left
			if uncle.color == red {
				node.parent.color = black
				uncle.color = black
				grandparent.color = red
				node = grandparent
				continue
			}
			if node == node.parent.left {
				node = node.parent
				side.rotateRight(node)
			}
			node.parent.color = black
			node.parent.parent.color = red
			side.rotateLeft(node.parent.parent)
		}
	}
	side.root.color = black
}

// transplant swaps replacement into target's parent slot. Used by delete.
func (side *rbTreeSide) transplant(target, replacement *rbNode) {
	switch {
	case target.parent == side.sentinel:
		side.root = replacement
	case target == target.parent.left:
		target.parent.left = replacement
	default:
		target.parent.right = replacement
	}
	replacement.parent = target.parent
}

func (side *rbTreeSide) deleteNode(target *rbNode) {
	spliced := target
	splicedOriginalColor := spliced.color
	var fixNode *rbNode
	switch {
	case target.left == side.sentinel:
		fixNode = target.right
		side.transplant(target, target.right)
	case target.right == side.sentinel:
		fixNode = target.left
		side.transplant(target, target.left)
	default:
		spliced = side.minSubtree(target.right)
		splicedOriginalColor = spliced.color
		fixNode = spliced.right
		if spliced.parent == target {
			fixNode.parent = spliced
		} else {
			side.transplant(spliced, spliced.right)
			spliced.right = target.right
			spliced.right.parent = spliced
		}
		side.transplant(target, spliced)
		spliced.left = target.left
		spliced.left.parent = spliced
		spliced.color = target.color
	}
	if splicedOriginalColor == black {
		side.deleteFixup(fixNode)
	}
}

func (side *rbTreeSide) deleteFixup(node *rbNode) {
	for node != side.root && node.color == black {
		if node == node.parent.left {
			sibling := node.parent.right
			if sibling.color == red {
				sibling.color = black
				node.parent.color = red
				side.rotateLeft(node.parent)
				sibling = node.parent.right
			}
			if sibling.left.color == black && sibling.right.color == black {
				sibling.color = red
				node = node.parent
				continue
			}
			if sibling.right.color == black {
				sibling.left.color = black
				sibling.color = red
				side.rotateRight(sibling)
				sibling = node.parent.right
			}
			sibling.color = node.parent.color
			node.parent.color = black
			sibling.right.color = black
			side.rotateLeft(node.parent)
			node = side.root
		} else {
			sibling := node.parent.left
			if sibling.color == red {
				sibling.color = black
				node.parent.color = red
				side.rotateRight(node.parent)
				sibling = node.parent.left
			}
			if sibling.right.color == black && sibling.left.color == black {
				sibling.color = red
				node = node.parent
				continue
			}
			if sibling.left.color == black {
				sibling.right.color = black
				sibling.color = red
				side.rotateLeft(sibling)
				sibling = node.parent.left
			}
			sibling.color = node.parent.color
			node.parent.color = black
			sibling.left.color = black
			side.rotateRight(node.parent)
			node = side.root
		}
	}
	node.color = black
}
