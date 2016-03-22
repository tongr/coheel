package de.uni_potsdam.hpi.coheel.ml

import java.util

import de.uni_potsdam.hpi.coheel.programs.DataClasses.{EntityTypes, ClassificationInfo, FeatureLine}
import weka.classifiers.Classifier
import weka.classifiers.meta.FilteredClassifier
import weka.core._
import weka.filters.unsupervised.attribute.Remove
import scala.collection.immutable.Set


object CoheelClassifier {

	val NUMBER_OF_FEATURES = 16 + EntityTypes.values.size // excluding class attribute
	val POSITIVE_CLASS = 1.0

	val POS_TAG_GROUPS = Array(
		List("NN", "NNS"),
		List("NNP", "NNPS"),
		List("JJ", "JJR", "JJS"),
		List("VB", "VBD", "VBG", "VBN", "VBP", "VBZ"),
		List("CD"),
		List("SYM"),
		List("WDT", "WP", "WP$", "WRB")
	)

	val FEATURE_DEFINITION = {
		val booleanAttrValues = util.Arrays.asList("false","true")
		val attrs = new util.ArrayList[Attribute](NUMBER_OF_FEATURES + 1)
		// basic features
		attrs.add(new Attribute("prom"))
		attrs.add(new Attribute("promRank"))
		attrs.add(new Attribute("promDeltaTop"))
		attrs.add(new Attribute("promDeltaSucc"))
		attrs.add(new Attribute("context"))
		attrs.add(new Attribute("contextRank"))
		attrs.add(new Attribute("contextDeltaTop"))
		attrs.add(new Attribute("contextDeltaSucc"))
		attrs.add(new Attribute("surfaceLinkProb"))
		// entity types
		EntityTypes.values.foreach(t => attrs.add(new Attribute(t.toString, booleanAttrValues)))
		// pos tags
		attrs.add(new Attribute("NN", booleanAttrValues))
		attrs.add(new Attribute("NNP", booleanAttrValues))
		attrs.add(new Attribute("JJ", booleanAttrValues))
		attrs.add(new Attribute("VB", booleanAttrValues))
		attrs.add(new Attribute("CD", booleanAttrValues))
		attrs.add(new Attribute("SYM", booleanAttrValues))
		attrs.add(new Attribute("W", booleanAttrValues))

		val classAttrValues = new util.ArrayList[String](2)
		classAttrValues.add("0.0")
		classAttrValues.add("1.0")
		val classAttr = new Attribute("class", classAttrValues)
		attrs.add(classAttr)
		attrs
	}

	def newInstance(classifier: Classifier): CoheelClassifier = {
		//new CoheelClassifier(classifier)
		// alternative (filtered):
		new CoheelClassifier(classifier, Set[String]("context","contextRank","contextDeltaTop","contextDeltaSucc","NN", "NNP", "JJ", "VB", "CD", "SYM", "W"))
	}
}

@SerialVersionUID(-3360509244299376345L)
class CoheelClassifier private(classifier: Classifier) extends Serializable {
	/**
	 * attribute filtered CoheelClassifier instance
	 * @param classifier the classifier configuration to be used (i.e., fitted)
	 * @param blacklist the attribute blacklist to be applied
	 * @return the filtered instance
	 */
	def this(classifier: Classifier, blacklist: Set[String]) = this({
		// find attributes to be removed
		import CoheelClassifier.FEATURE_DEFINITION
		val removeBuff = (0 until FEATURE_DEFINITION.size())
			  .filter{ i=> blacklist.contains(FEATURE_DEFINITION.get(i).name)}
			  .toArray

		// create a remove filtered classifier
		val attrRemover: Remove = new Remove()
		attrRemover.setAttributeIndicesArray(removeBuff)
		//debug: println(s"using attributes: ${attrRemover.getAttributeIndices}")
		val fc = new FilteredClassifier()
		fc.setFilter(attrRemover)
		fc.setClassifier(classifier)
		fc
	})

	val instances = new Instances("Classification", CoheelClassifier.FEATURE_DEFINITION, 1)
	instances.setClassIndex(CoheelClassifier.NUMBER_OF_FEATURES)
	val positiveClass = CoheelClassifier.POSITIVE_CLASS

	/**
	 * Classifies a given group of instances, which result from the same link/trie hit in the original text.
	 * Only if exactly one true prediction is given, the function returns a result.
	 *
	 * @param featureLine The features of all possible links.
	 * @return The predicted link or None, if no link is predicted.
	 */
	def classifyResultsWithSeedLogic(featureLine: Seq[FeatureLine[ClassificationInfo]]): scala.Option[FeatureLine[ClassificationInfo]] = {
		asSeed( classify( featureLine ) )
	}

	/**
	  * Classifies a given group of instances, which result from the same link/trie hit in the original text, using candidate logic.
	  */
	def classifyResultsWithCandidateLogic(featureLine: Seq[FeatureLine[ClassificationInfo]]): List[FeatureLine[ClassificationInfo]] = classify(featureLine)

	/**
	  * transforms a classification result of one group of instance into a valid seed classification
	  * @param predictions the predictions beeing made
	  * @return the seed or None
    */
	def asSeed(predictions: List[FeatureLine[ClassificationInfo]]): scala.Option[FeatureLine[ClassificationInfo]] = {
		if (predictions.size == 1)
			predictions.headOption
		else
			None
	}

	/**
	  * Classifies a given group of instances, which result from the same link/trie hit in the original text.
	  */
	def classify(featureLine: Seq[FeatureLine[ClassificationInfo]]): List[FeatureLine[ClassificationInfo]] = {
		var positivePredictions = List[FeatureLine[ClassificationInfo]]()
		featureLine.foreach { featureLine =>
			assert(featureLine.features.size == instances.numAttributes() || featureLine.features.size + 1 == instances.numAttributes() )
			val instance = buildInstance(featureLine)
			instance.setDataset(instances)
			if (classifier.classifyInstance(instance) == positiveClass) {
				positivePredictions ::= featureLine
			}
		}
		positivePredictions
	}

	protected def buildInstance(featureLine: FeatureLine[ClassificationInfo]): Instance = {
		val attValues = featureLine.features.toArray
		//val instance = new DenseInstance(1.0, attValues)
		// should use less memory (due to very sparse POS- and EntityType-attributes)
		val instance = new SparseInstance(1.0, attValues)
		instance
	}

	def fit(data: Instances): Unit = {
		assert(data.numAttributes() == instances.numAttributes(), s"expected attributes ${(0 until instances.numAttributes()).map(instances.attribute(_))}, actual attributes ${(0 until data.numAttributes()).map(data.attribute(_))}")
		// TODO check attribute names as well?!
		classifier.buildClassifier(data)
	}
}
