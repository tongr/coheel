package de.uni_potsdam.hpi.coheel.wiki

import java.io.{Reader, StringReader, BufferedReader}
import javax.xml.stream.{XMLStreamConstants, XMLInputFactory}
import de.uni_potsdam.hpi.coheel.programs.CoheelLogger
import de.uni_potsdam.hpi.coheel.programs.DataClasses._
import de.uni_potsdam.hpi.coheel.util.Timer
import org.apache.commons.lang3.StringEscapeUtils

import scala.collection.mutable

case class RawWikiPage(pageTitle: String,
                       ns: Int,
                       redirect: String,
                       source: String) {

	import CoheelLogger._
	lazy val isDisambiguation = {
		val isDisambiguationFromTitle = pageTitle.contains("(disambiguation)")
		if (isDisambiguationFromTitle)
			true
		else {
			Timer.start("DISAMBIGUATION CHECK")
			// in case of performance problems:
			// disambiguation links are always (as seen so far) at the end of the text
			// maybe this could be used to not scan the entire text
			val disambiguationRegex = """(?ui)\{\{disambiguation.*?\}\}""".r
			val matches = disambiguationRegex.findAllIn(source)
				// check whether the regex sometimes accidentially matches too much text
				.map { s =>
				if (s.length > 200)
					log.warn(s"Disambiguation regex returns long result: $s.")
				s
			}
				.map(_.toLowerCase)
				.filter { s =>
					!s.contains("disambiguation needed")
				}
			Timer.end("DISAMBIGUATION CHECK")
			matches.nonEmpty
		}
	}
}
/**
 * Captures the important aspects of a WikiPage for our use case, while still
 * maintaning connection (via inheritance) to the DBpedia extraction framework.
 * <br />
 * <strong>Definition redirect</strong>:<br />
 * A page is seen as a redirect, if it contains the &lt;redirect&gt; tag inside the &lt;page&gt; tag in the wiki
 * dump.
 * <br />
 * <strong>Definition disambiguation</strong>:<br />
 * A page is seen as a disambiguation if one of the following conditions is true:
 * <ul>
 *     <li> The title contains "(disambiguation)"
 *     <li> The text contains a disambiguation template (not all disambiguation pages contain "(disambiguation)" in the
 *          title, e.g. http://en.wikipedia.org/wiki/Alien
 * <strong>Definition list</strong>:<br />
 * A page is seen as a list if it's title starts with "List of" or "Lists of".
 *
 * @param pageTitle The title of the page as a string.
 * @param ns The namespace of the wiki page.
 * @param redirect The title of the page this page is redirecting to or null, if it is not a redirect.
 * @param plainTextStemmed This page's plain text content as array of tokens (stemmed).
 * @param plainTextUnstemmed This page's plain text content as array of tokens (unstemmed).
 */
case class WikiPage(pageTitle: String,
                    ns: Int,
                    redirect: String,
                    plainTextStemmed: Array[String],
                    plainTextUnstemmed: Array[String],
                    links: Array[Link],
                    isDisambiguation: Boolean) {

	val isRedirect: Boolean = this.redirect != RawWikiPage.NO_REDIRECT

	lazy val isList = pageTitle.startsWith("List of") || pageTitle.startsWith("Lists of")


	def isNormalPage: Boolean = {
		!isDisambiguation && !isRedirect && !isList
	}
}

/**
 * Compared to the normal wiki page, this also stores the position of the links and the part of speech tags.
 */
case class FullInfoWikiPage(pageTitle: String,
                    ns: Int, redirect: String,
                    plainText: mutable.ArrayBuffer[String],
                    tags: mutable.ArrayBuffer[String],
                    links: mutable.Map[Int, Link],
                    isDisambiguation: Boolean) {

	lazy val isList = pageTitle.startsWith("List of") || pageTitle.startsWith("Lists of")
}

object RawWikiPage {

	val NO_REDIRECT = ""
	/**
	 * Builds a wiki page from the given title and wiki markup source.
	 */
	def fromSource(pageTitle: String, source: String): RawWikiPage = {
		RawWikiPage(pageTitle, 0, "", source)
	}
}

class WikiPageReader {
	val factory = XMLInputFactory.newInstance()
	def xmlToWikiPages(s: String): Iterator[RawWikiPage] = {
		val reader = new BufferedReader(new StringReader(s))
		xmlToWikiPages(reader)
	}

	private var readCounter = 1

	def xmlToWikiPages(reader: Reader, pageFilter: String => Boolean = _ => true): Iterator[RawWikiPage] = {
		new Iterator[RawWikiPage] {
			var alreadyRead = false
			var hasMorePages = true

			val streamReader = factory.createXMLStreamReader(reader)

			// Values for the current page
			var pageTitle: String = _
			var ns: Int = _
			var redirectTitle: String = _
			var text: String = _

			readNextPage()

			def readNextPage(): Unit = {
				Timer.start("XML")
				redirectTitle = RawWikiPage.NO_REDIRECT
				var foundNextPage = false
				var pagePassedFilter = true

				while (!foundNextPage && streamReader.hasNext) {
					streamReader.next
					if (streamReader.getEventType == XMLStreamConstants.START_ELEMENT) {
						streamReader.getLocalName match {
							case "text" if pagePassedFilter => text = streamReader.getElementText
							case "ns" if pagePassedFilter => ns = streamReader.getElementText.toInt
							case "title" =>
								pageTitle = StringEscapeUtils.unescapeXml(streamReader.getElementText)
								// check whether the found page passes the page filter
								// if not, we will search for the next page and test again here
								pagePassedFilter = pageFilter(pageTitle)
							case "redirect" if pagePassedFilter => redirectTitle = StringEscapeUtils.unescapeXml(streamReader.getAttributeValue(null, "title"))
							case "page" if pagePassedFilter => foundNextPage = true
							case _ =>
						}
					}
				}
				hasMorePages = streamReader.hasNext
				if (!hasMorePages)
					reader.close()
				Timer.end("XML")
			}

			def hasNext = hasMorePages
			def next(): RawWikiPage = {
				if (!alreadyRead) {
					alreadyRead = true
//					log.info(f"Reading $readCounter%4s. wiki file on this node.")
					readCounter += 1
				}
				readNextPage()
				RawWikiPage(
					pageTitle,
					ns,
					redirectTitle,
					text
				)
			}
		}
	}
}
