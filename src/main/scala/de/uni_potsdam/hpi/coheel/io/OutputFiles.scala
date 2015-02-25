package de.uni_potsdam.hpi.coheel.io

import de.uni_potsdam.hpi.coheel.FlinkProgramRunner
import org.apache.flink.api.java.operators.DataSink
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import scala.language.implicitConversions

object OutputFiles {
	lazy val currentPath = FlinkProgramRunner.config.getString("output_files_dir")
	lazy val location = FlinkProgramRunner.config.getString("type")

	lazy val surfaceProbsPath            = s"$location://${currentPath}surface-probs.wiki"
	lazy val contextLinkProbsPath        = s"$location://${currentPath}context-link-probs.wiki"
	lazy val languageModelProbsPath      = s"$location://${currentPath}language-model-probs.wiki"
	lazy val documentWordCountsPath      = s"$location://${currentPath}document-word-counts.wiki"
	lazy val redirectPath                = s"$location://${currentPath}redirects.wiki"
	lazy val resolvedRedirectsPath       = s"$location://${currentPath}resolved-redirects.wiki"
	lazy val wikiPagesPath               = s"$location://${currentPath}wiki-pages.wiki"
	lazy val plainTextsPath              = s"$location://${currentPath}plain-texts.wiki"
	lazy val surfaceDocumentCountsPath   = s"$location://${currentPath}surface-document-counts.wiki"
	lazy val entireTextSurfacesPath      = s"$location://${currentPath}entire-text-surfaces.wiki"
	lazy val surfaceLinkProbsPath        = s"$location://${currentPath}surface-link-probs.wiki"
	lazy val surfaceEvaluationPath       = s"$location://${currentPath}surface-evaluation.wiki"

	lazy val classificationPath          = s"$location://${currentPath}classification.wiki"

	implicit def toOutputFiles(dataSet: DataSet[_]): OutputFiles = {
		new OutputFiles(dataSet)
	}

	val LINE_DELIMITER = "\n"
	val ROW_DELIMITER  = '\t'
}

class OutputFiles(dataSet: DataSet[_]) {

	def writeAsTsv(path: String): DataSink[_] = {
		dataSet.writeAsCsv(path, OutputFiles.LINE_DELIMITER, OutputFiles.ROW_DELIMITER.toString, FileSystem.WriteMode.OVERWRITE)
	}
}
