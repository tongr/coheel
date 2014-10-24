package de.uni_potsdam.hpi.coheel.datastructures

import it.unimi.dsi.fastutil.ints.Int2ReferenceMap

case class ContainsResult(asEntry: Boolean, asIntermediateNode: Boolean)

case class Trie() {

	val rootNode = TrieNode()

	def add(tokens: Seq[String]): Unit = {
		if (tokens.isEmpty)
			throw new RuntimeException("Cannot add empty tokens.")
		rootNode.add(tokens)
	}
	def add(tokenString: String): Unit = add(tokenString.split(' '))

	def contains(tokens: Seq[String]): ContainsResult = {
		if (tokens.isEmpty)
			throw new RuntimeException("Cannot add empty tokens.")
		rootNode.contains(tokens)
	}
	def contains(tokenString: String): ContainsResult = contains(tokenString.split(' '))

	/**
	 * Returns all elements of the trie, starting from a certain offset and going as far as necessary.
	 * @param arr The array to search in.
	 * @param startIndex And the start index.
	 * @return A list of the trie elements matching to the array starting from the start index.
	 */
	def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]] = {
		rootNode.slidingContains(arr, startIndex)
	}

	/**
	 * Same as slidingContains(Array[String], startIndex: Int), but works in arbitrary types.
	 * Needs a conversion function form the type to a string.
	 */
	def slidingContains[T](arr: Array[T], toString: T => String, startIndex: Int): Seq[Seq[T]] = {
		rootNode.slidingContains(arr, toString, startIndex)
	}
}

case class TrieNode() {

	var isEntry = false

	var children: Int2ReferenceMap[TrieNode] = _
//	var children: TIntObjectHashMap[TrieNode] = _

	def add(tokens: Seq[String]): Unit = {
		if (children == null)
//			children = new TIntObjectHashMap[TrieNode]()
			children = new it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap[TrieNode]()
		if (tokens.tail.isEmpty) {
			children.get(tokens.head.hashCode) match {
				case null =>
					val newNode = TrieNode()
					newNode.isEntry = true
					children.put(tokens.head.hashCode, newNode)
				case trieNode => trieNode.isEntry = true
			}
		}
		else {
			children.get(tokens.head.hashCode) match {
				case null =>
					val newNode = TrieNode()
					newNode.add(tokens.tail)
					children.put(tokens.head.hashCode, newNode)
				case trieNode =>
					trieNode.add(tokens.tail)
			}
		}
	}

	def contains(tokens: Seq[String]): ContainsResult = {
		// We found the correct node, now check if it is an entry
		if (tokens.isEmpty)
			ContainsResult(isEntry, true)
		// We reached an early end in the tree (no child node, even though we have more tokens to process)
		else if (children == null)
			ContainsResult(false, false)
		else {
			children.get(tokens.head.hashCode) match {
				case null => ContainsResult(false, false)
				case trieNode => trieNode.contains(tokens.tail)
			}
		}
	}

	def slidingContains[T](arr: Array[T], toString: T => String, startIndex: Int): Seq[Seq[T]] = {
		var result = List[Seq[T]]()
		// vector: immutable list structure with fast append
		var currentCheck = Vector[T](arr(startIndex))
		var containsResult = this.contains(currentCheck.map(toString))

		var i = 1
		// for each word, go so far until it is no intermediate node anymore
		while (containsResult.asIntermediateNode) {
			// if it is a entry, add to to result list
			if (containsResult.asEntry)
				result ::= currentCheck
			// expand current window, if possible
			if (startIndex + i < arr.size) {
				// append element to the end of the vector
				currentCheck :+= arr(startIndex + i)
				containsResult = this.contains(currentCheck.map(toString))
				i += 1
			} else {
				// if we reached the end of the text, we need to break manually
				containsResult = ContainsResult(false, false)
			}
		}
		result
	}

	def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]] = {
		slidingContains[String](arr, { s => s }, startIndex)
	}
}
