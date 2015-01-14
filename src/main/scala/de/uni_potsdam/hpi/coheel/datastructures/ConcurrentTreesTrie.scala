package de.uni_potsdam.hpi.coheel.datastructures

class ConcurrentTreesTrie extends TrieLike {

	val rt = new ConcurrentInvertedRadixTree[VoidValue](new DefaultCharArrayNodeFactory)

	def getKeysContainedIn(document: String): Iterator[String] = {
		rt.getKeysContainedIn(document).iterator().asScala.map(_.toString.trim)
	}
	override def add(tokens: Seq[String]): Unit = {
		rt.put(tokens.mkString(" ") + " ", VoidValue.SINGLETON)
	}

	override def slidingContains(arr: Array[String], startIndex: Int): Seq[Seq[String]] = {
		???
	}

	override def slidingContains[T](arr: Array[T], toString: (T) => String, startIndex: Int): Seq[Seq[T]] = {
		???
	}

	override def contains(tokens: Seq[String]): ContainsResult = {
		val tokenString = tokens.mkString(" ") + " "
		val asEntry = rt.getValueForExactKey(tokenString) != null
		val asIntermediaNode = rt.getKeysStartingWith(tokenString).iterator().hasNext
		ContainsResult(asEntry, asIntermediaNode)
	}
}
