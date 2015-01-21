package de.uni_potsdam.hpi.coheel.datastructures

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class TrieTest extends FunSuite {

	List(classOf[HashTrie], classOf[ConcurrentTreesTrie], classOf[PatriciaTrieWrapper]).foreach { trie =>
		buildTests(trie)
	}

	def buildTests[T <: Trie](trieClass: Class[T]): Unit = {
		def newTrie(): Trie = {
			trieClass.newInstance()
		}
		val name = trieClass.getSimpleName

		test(s"single word queries work for $name") {
			val trie = newTrie()
			trie.add("angela")
			assert(trie.contains("angela").asEntry)
		}

		test(s"multiple word queries work for $name") {
			val trie = newTrie()
			trie.add("angela merkel")
			assert(trie.contains("angela merkel").asEntry)
		}

		test(s"distinction between contains-asEntry and contains-asIntermediateNode for $name") {
			val trie = newTrie()
			trie.add("angela dorothea merkel")

			assert(trie.contains("angela").asIntermediateNode)
			assert(trie.contains("angela dorothea").asIntermediateNode)
			assert(trie.contains("angela dorothea merkel").asIntermediateNode)

			assert(!trie.contains("angela").asEntry)
			assert(!trie.contains("angela dorothea").asEntry)
			assert(trie.contains("angela dorothea merkel").asEntry)
		}

		test(s"only actually added words are considered for $name") {
			val trie = newTrie()
			trie.add("angela merkel")
			assert(!trie.contains("angela").asEntry)
		}

		test(s"multiple adds do not cause harm for $name") {
			val trie = newTrie()
			trie.add("angela merkel")
			trie.add("angela merkel")
			trie.add("angela merkel")
			assert(!trie.contains("angela").asEntry)
			assert(trie.contains("angela merkel").asEntry)
		}

		test(s"findAllIn finds all occurrences for $name") {
			val trie = newTrie()
			trie.add("angela merkel")
			trie.add("angela merkel is german")
			trie.add("angela")
			trie.add("merkel")

			val testSentence = "angela merkel is german"
			val result1 = trie.findAllIn(testSentence).toList
			val expected1 = Seq("angela", "angela merkel", "angela merkel is german", "merkel")
			expected1.foreach { expected =>
				assert(result1.contains(expected))
			}
		}

		//	test("sliding contains works as expected for arbitrary type") {
		//		case class Foo(a: Int, b: Double)
		//
		//		val trie = newTrie()
		//		trie.add("1 2")
		//		trie.add("1 2 3")
		//
		//		val testSentence = Array(Foo(1, 1.0), Foo(2, 2.0))
		//		val result = trie.slidingContains[Foo](testSentence, { f => f.a.toString }, 0)
		//		assert (result.size === 1)
		//		assert (result(0)(0).a === 1 && result(0)(1).a === 2)
		//	}

		test(s"branching works at every level for $name") {
			val trie = newTrie()
			trie.add("angela dorothea merkel")
			trie.add("angela dorothea kanzler")
			assert(!trie.contains("dorothea").asEntry)
			assert(!trie.contains("angela dorothea").asEntry)
			assert(trie.contains("angela dorothea merkel").asEntry)
		}

		test(s"can go many levels deep for $name") {
			val trie = newTrie()
			trie.add("ab cd ef gh ij kl")
			assert(trie.contains("ab cd ef gh ij kl").asEntry)
		}
	}
}
