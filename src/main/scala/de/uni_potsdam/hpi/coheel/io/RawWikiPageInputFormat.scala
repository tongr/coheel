package de.uni_potsdam.hpi.coheel.io

import de.uni_potsdam.hpi.coheel.wiki.RawWikiPage
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.Seekable
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import java.io.DataInputStream
import java.io.IOException
import java.nio.charset.CharacterCodingException

/**
  * These classes are heavily inspired by the work of Jimmy Lin:
  * "Cloud9: A MapReduce Library for Hadoop"  http://cloud9lib.org/ licenced under
  * Apache License, Version 2.0 http://www.apache.org/licenses/LICENSE-2.0.
  * The original code can be found at  https://github.com/lintool/Cloud9.
  *
  * The class is meant to parse a Wikipedia XML dump
  *
  * @author Jimmy Lin
  * @author tongr
  * @see <a href="http://cloud9lib.org/">http://cloud9lib.org/</a>
  * @see <a href="https://github.com/lintool/Cloud9">https://github.com/lintool/Cloud9</a>
  */
object RawWikiPageInputFormat {
	/**
	  * Start delimiter of the page, which is &lt;<code>page</code>&gt;.
	  */
	private val XML_START_TAG: String = "<page>"
	/**
	  * End delimiter of the page, which is &lt;<code>/page</code>&gt;.
	  */
	private val XML_END_TAG: String = "</page>"
	/**
	  * Start delimiter of the title, which is &lt;<code>title</code>&gt;.
	  */
	private val XML_START_TAG_TITLE: String = "<title>"
	/**
	  * End delimiter of the title, which is &lt;<code>/title</code>&gt;.
	  */
	private val XML_END_TAG_TITLE: String = "</title>"
	/**
	  * Start delimiter of the namespace, which is &lt;<code>ns</code>&gt;.
	  */
	private val XML_START_TAG_NAMESPACE: String = "<ns>"
	/**
	  * End delimiter of the namespace, which is &lt;<code>/ns</code>&gt;.
	  */
	private val XML_END_TAG_NAMESPACE: String = "</ns>"
	/**
	  * Start delimiter of the text, which is &lt;<code>text xml:space=\"preserve\"</code>&gt;.
	  */
	private val XML_START_TAG_TEXT: String = "<text xml:space=\"preserve\">"
	/**
	  * End delimiter of the text, which is &lt;<code>/text</code>&gt;.
	  */
	private val XML_END_TAG_TEXT: String = "</text>"
	/**
	  * Start delimiter of the text, which is &lt;<code>edirect title\"</code>.
	  */
	private val XML_START_TAG_REDIRECT: String = "<redirect title=\""
	/**
	  * End delimiter of the text, which is <code>\" /</code>&gt;.
	  */
	private val XML_END_TAG_REDIRECT: String = "\" />"

	private class WikiRecordReader extends RecordReader[LongWritable, RawWikiPage] {
		private var startTag: Array[Byte] = null
		private var endTag: Array[Byte] = null
		private var start: Long = 0L
		private var end: Long = 0L
		private var pos: Long = 0L
		private var fsin: DataInputStream = null
		private var buffer: DataOutputBuffer = new DataOutputBuffer
		private final val key: LongWritable = new LongWritable
		private var value: RawWikiPage = null

		@throws(classOf[IOException])
		@throws(classOf[InterruptedException])
		def initialize(input: InputSplit, context: TaskAttemptContext) {
			startTag = XML_START_TAG.getBytes("utf-8")
			endTag = XML_END_TAG.getBytes("utf-8")
			val split: FileSplit = input.asInstanceOf[FileSplit]
			start = split.getStart
			end = start + split.getLength
			pos = start
			val file: Path = split.getPath
			val fs: FileSystem = file.getFileSystem(context.getConfiguration)
			val fileIn: FSDataInputStream = fs.open(file)
			fileIn.seek(start)
			fsin = fileIn
		}

		@throws(classOf[IOException])
		@throws(classOf[InterruptedException])
		def nextKeyValue: Boolean = {
			if (pos < end) {
				if (readUntilMatch(startTag, false)) {
					try {
						buffer.write(startTag)
						if (readUntilMatch(endTag, true)) {
							key.set(pos - startTag.length)
							parseRawWikiPage
							return true
						}
					} finally {
						assert(!(fsin.isInstanceOf[Seekable]) || pos == (fsin.asInstanceOf[Seekable]).getPos, "bytes consumed error!")
						buffer.reset
					}
				}
			}
			return false
		}

		@throws(classOf[IOException])
		@throws(classOf[InterruptedException])
		def getCurrentKey: LongWritable = {
			return key
		}

		@throws(classOf[IOException])
		@throws(classOf[InterruptedException])
		def getCurrentValue: RawWikiPage = {
			return value
		}

		@throws(classOf[IOException])
		def close {
			fsin.close
		}

		@throws(classOf[IOException])
		def getProgress: Float = {
			return ((pos - start).toFloat) / ((end - start).toFloat)
		}

		@throws(classOf[IOException])
		private def readUntilMatch(`match`: Array[Byte], withinBlock: Boolean): Boolean = {
			var i: Int = 0
			while (true) {
				val b: Int = fsin.read
				pos += 1
				if (b == -1) return false
				if (withinBlock) buffer.write(b)
				if (b == `match`(i)) {
					i += 1
					if (i >= `match`.length) return true
				}
				else i = 0
				if (!withinBlock && i == 0 && pos >= end) return false
			}
			// one should not end up here
			throw new IllegalStateException()
		}

		@throws(classOf[CharacterCodingException])
		private def parseRawWikiPage {
			val pageXml: String = Text.decode(buffer.getData, 0, buffer.getLength)
			val pageTitle: String = escapedContent(XML_START_TAG_TITLE, XML_END_TAG_TITLE, pageXml)
			val ns: Int = intContent0(XML_START_TAG_NAMESPACE, XML_END_TAG_NAMESPACE, pageXml)
			val redirectTitle: String = escapedContent(XML_START_TAG_REDIRECT, XML_END_TAG_REDIRECT, pageXml)
			val source: String = escapedContent(XML_START_TAG_TEXT, XML_END_TAG_TEXT, pageXml)
			value = new RawWikiPage(pageTitle, ns, redirectTitle, source)
		}

		private def escapedContent(startTag: String, endTag: String, xml: String): String = {
			return StringEscapeUtils.unescapeXml(getContent(startTag, endTag, xml, ""))
		}

		private def intContent0(startTag: String, endTag: String, xml: String): Int = {
			return getContent(startTag, endTag, xml, "0").toInt
		}

		private def getContent(startTag: String, endTag: String, xml: String, defaultVal: String): String = {
			val start: Int = xml.indexOf(startTag)
			if (start < 0) {
				return defaultVal
			}
			val end: Int = xml.indexOf(endTag, start)
			return xml.substring(start + startTag.length, end)
		}
	}

}

class RawWikiPageInputFormat extends FileInputFormat[LongWritable, RawWikiPage] {
	@throws(classOf[IOException])
	@throws(classOf[InterruptedException])
	def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, RawWikiPage] = {
		return new RawWikiPageInputFormat.WikiRecordReader
	}
}
