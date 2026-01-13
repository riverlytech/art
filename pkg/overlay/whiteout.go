package overlay

import (
	"path/filepath"
	"strings"
	"sync"
)

// WhiteoutCache is an in-memory trie for efficient whiteout lookups.
// It provides O(depth) ancestor checking instead of O(n) database queries.
type WhiteoutCache struct {
	mu   sync.RWMutex
	root *whiteoutNode
}

// whiteoutNode represents a node in the whiteout trie
type whiteoutNode struct {
	children   map[string]*whiteoutNode
	isWhiteout bool
}

// NewWhiteoutCache creates a new empty whiteout cache
func NewWhiteoutCache() *WhiteoutCache {
	return &WhiteoutCache{
		root: &whiteoutNode{
			children: make(map[string]*whiteoutNode),
		},
	}
}

// LoadFromPaths populates the cache from a list of whiteout paths
func (w *WhiteoutCache) LoadFromPaths(paths []string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Reset the trie
	w.root = &whiteoutNode{
		children: make(map[string]*whiteoutNode),
	}

	for _, path := range paths {
		w.insertLocked(path)
	}
}

// Insert adds a whiteout for the given path
func (w *WhiteoutCache) Insert(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.insertLocked(path)
}

// insertLocked adds a whiteout without locking (caller must hold lock)
func (w *WhiteoutCache) insertLocked(path string) {
	parts := splitPath(path)
	node := w.root

	for _, part := range parts {
		if node.children == nil {
			node.children = make(map[string]*whiteoutNode)
		}
		child, ok := node.children[part]
		if !ok {
			child = &whiteoutNode{
				children: make(map[string]*whiteoutNode),
			}
			node.children[part] = child
		}
		node = child
	}

	node.isWhiteout = true
}

// Remove removes a whiteout for the given path
func (w *WhiteoutCache) Remove(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	parts := splitPath(path)
	if len(parts) == 0 {
		return
	}

	// Find the node and its parent
	var parents []*whiteoutNode
	var partNames []string
	node := w.root

	for _, part := range parts {
		if node.children == nil {
			return // Path doesn't exist in trie
		}
		child, ok := node.children[part]
		if !ok {
			return // Path doesn't exist in trie
		}
		parents = append(parents, node)
		partNames = append(partNames, part)
		node = child
	}

	// Clear the whiteout flag
	node.isWhiteout = false

	// Clean up empty nodes from bottom to top
	for i := len(parents) - 1; i >= 0; i-- {
		parent := parents[i]
		name := partNames[i]
		child := parent.children[name]

		// If the child has no children and is not a whiteout, remove it
		if len(child.children) == 0 && !child.isWhiteout {
			delete(parent.children, name)
		} else {
			break // Stop cleanup if this node is still needed
		}
	}
}

// HasExactWhiteout checks if there's a whiteout for the exact path
func (w *WhiteoutCache) HasExactWhiteout(path string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	parts := splitPath(path)
	node := w.root

	for _, part := range parts {
		if node.children == nil {
			return false
		}
		child, ok := node.children[part]
		if !ok {
			return false
		}
		node = child
	}

	return node.isWhiteout
}

// HasWhiteoutAncestor checks if the path or any of its ancestors is whited out.
// Returns true if any ancestor (including the path itself) has a whiteout.
func (w *WhiteoutCache) HasWhiteoutAncestor(path string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	parts := splitPath(path)
	node := w.root

	for _, part := range parts {
		if node.children == nil {
			return false
		}
		child, ok := node.children[part]
		if !ok {
			return false
		}
		node = child
		// Check if this ancestor is a whiteout
		if node.isWhiteout {
			return true
		}
	}

	return false
}

// GetChildWhiteouts returns the names of direct children that are whited out
// for the given directory path
func (w *WhiteoutCache) GetChildWhiteouts(dirPath string) []string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	parts := splitPath(dirPath)
	node := w.root

	// Navigate to the directory node
	for _, part := range parts {
		if node.children == nil {
			return nil
		}
		child, ok := node.children[part]
		if !ok {
			return nil
		}
		node = child
	}

	// Collect children that are whiteouts
	var names []string
	for name, child := range node.children {
		if child.isWhiteout {
			names = append(names, name)
		}
	}

	return names
}

// GetAllWhiteouts returns all whiteout paths (for debugging/testing)
func (w *WhiteoutCache) GetAllWhiteouts() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var paths []string
	w.collectWhiteouts(w.root, "", &paths)
	return paths
}

// collectWhiteouts recursively collects all whiteout paths
func (w *WhiteoutCache) collectWhiteouts(node *whiteoutNode, prefix string, paths *[]string) {
	if node.isWhiteout && prefix != "" {
		*paths = append(*paths, prefix)
	}
	for name, child := range node.children {
		childPath := prefix + "/" + name
		w.collectWhiteouts(child, childPath, paths)
	}
}

// Clear removes all whiteouts from the cache
func (w *WhiteoutCache) Clear() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.root = &whiteoutNode{
		children: make(map[string]*whiteoutNode),
	}
}

// splitPath splits a path into components, handling edge cases
func splitPath(path string) []string {
	// Normalize: ensure leading /, remove trailing /
	path = filepath.Clean("/" + path)
	if path == "/" {
		return nil
	}

	// Remove leading / and split
	path = strings.TrimPrefix(path, "/")
	parts := strings.Split(path, "/")

	// Filter out empty parts
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" && p != "." {
			result = append(result, p)
		}
	}
	return result
}

// joinPath joins path components back into a path string
func joinPath(parts []string) string {
	if len(parts) == 0 {
		return "/"
	}
	return "/" + strings.Join(parts, "/")
}
