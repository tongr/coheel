package de.uni_potsdam.hpi.coheel.wiki

import java.io.{StringReader, BufferedReader}
import javax.xml.stream.{XMLStreamConstants, XMLInputFactory}
import org.apache.commons.lang3.StringEscapeUtils


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
 * A page is seen as a list if it belongs to a category which starts with List.
 * @param pageTitle The title of the page as a string.
 * @param ns The namespace of the wiki page.
 * @param redirectTitle The title of the page this page is redirecting to or "", if it is not a redirect.
 * @param source This page's content.
 */
case class WikiPage(pageTitle: String, ns: Int, redirectTitle: String, source: String) {
	val redirect        = if (redirectTitle == "") null else redirectTitle


	def isRedirect: Boolean = this.redirect != null
	def isDisambiguation: Boolean = {
		val isDisambiguationFromTitle = pageTitle.contains("(disambiguation)")
		if (isDisambiguationFromTitle)
			return true

		// in case of performance problems:
		// disambiguation links are always (as seen so far) at the end of the text
		// maybe this could be used to not scan the whole text
		val disambiguationRegex = """(?ui)\{\{disambiguation.*?\}\}""".r
		val matches = disambiguationRegex.findAllIn(source)
			// check whether the regex sometimes accidentially matches too much text
			.map { s =>
				if (s.length > 200)
					throw new RuntimeException(s"Disambiguation regex went wrong on $s.")
				s
			}
			.map(_.toLowerCase)
			.filter { s =>
				!s.contains("disambiguation needed")
			}
		matches.nonEmpty
	}

	def isList: Boolean = {
		source.contains("[[Category:List")
	}
}

object WikiPageReader {

	lazy val factory = XMLInputFactory.newInstance()
	var i = 0
	def xmlToWikiPages(xml: String): Iterator[WikiPage] = {
		new Iterator[WikiPage] {
			var hasMorePages = true

			// XML related
			val reader = new BufferedReader(new StringReader(xml))
			val streamReader = factory.createXMLStreamReader(reader)

			// Values for the current page
			var pageTitle: String = _
			var ns: Int = _
			var redirectTitle: String = _
			var text: String = _

			readNextPage()

			def readNextPage(): Unit = {
				redirectTitle = ""
				var foundNextPage = false

				while (!foundNextPage && streamReader.hasNext) {
					streamReader.next
					if (streamReader.getEventType == XMLStreamConstants.START_ELEMENT) {
						streamReader.getLocalName match {
							case "text" => text = streamReader.getElementText
							case "ns" => ns = streamReader.getElementText.toInt
							case "title" => pageTitle = StringEscapeUtils.unescapeXml(streamReader.getElementText)
							case "redirect" => redirectTitle = StringEscapeUtils.unescapeXml(streamReader.getAttributeValue(null, "title"))
							case "page" => foundNextPage = true
							case _ =>
						}
					}
				}
				hasMorePages = streamReader.hasNext
			}


			def hasNext = hasMorePages
			def next(): WikiPage = {
				readNextPage()
				new WikiPage(
					pageTitle = pageTitle,
					ns = ns,
					redirectTitle = redirectTitle,
					source = text
				)
			}

		}
	}

}
