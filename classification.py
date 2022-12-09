import itertools

import numpy as np

from matplotlib import pyplot as plt

from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorIndexer, IndexToString
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col

from sklearn.metrics import confusion_matrix

import findspark
findspark.init()


class Classifier:

    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName('Classification') \
            .getOrCreate()
        self.sc = self.spark.sparkContext

    def _preprocess(self, df):
        # Dataset preprocessing
        # Converts integer to double and converts 'quality' column to categorical

        condition = udf(lambda r: 'bad' if r == 0 else 'good')

        df = df.withColumn("NO2", df["NO2"].cast('double'))
        df = df.withColumn("O3", df["O3"].cast('double'))
        df = df.withColumn("PM10", df["PM10"].cast('double'))
        df = df.withColumn("PM25", df["PM25"].cast('double'))

        return df.withColumn('quality', condition(col('quality')))

    def _transData(self, data):
        return data.rdd.map(lambda r: [Vectors.dense(r[2:-1]), r[-1]]).toDF(['features', 'label'])

    def _train(self, trainingData, labelIndexer, featureIndexer, labelConverter):
        nb = NaiveBayes(featuresCol='indexedFeatures', labelCol='indexedLabel')

        # Chain indexers and tree in a Pipeline
        pipeline = Pipeline(stages=[labelIndexer, featureIndexer, nb, labelConverter])

        # Train model.  This also runs the indexers.
        model = pipeline.fit(trainingData)

        return model

    def _load_data(self):
        df = self.spark.read.options(delimiter=';', inferSchema=True, header=True) \
            .csv("data/dataset.csv")

        return df

    def run(self):
        print('loading training data')
        df = self._load_data()

        print('training data preprocessing')
        df = self._preprocess(df)

        print('transforming training data')
        transformed = self._transData(df)

        # Assigns index values to the features and label columns
        labelIndexer = StringIndexer(inputCol='label',
                                     outputCol='indexedLabel').fit(transformed)

        # Automatically identify categorical features, and index them.
        # Set maxCategories so features with > 4 distinct values are treated as continuous.
        featureIndexer = VectorIndexer(inputCol="features",
                                       outputCol="indexedFeatures",
                                       maxCategories=4).fit(transformed)

        # Classification
        # Dividing into training data and testing data
        (trainingData, testData) = transformed.randomSplit([0.8, 0.2])

        # Convert indexed labels back to original labels.
        labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                                       labels=labelIndexer.labels)

        print('model training')
        # Train model
        self.model = self._train(trainingData, labelIndexer, featureIndexer, labelConverter)

        # Make predictions.
        return self.predict(testData)

    def predict(self, testData):
        transformedTestData = self._transData(testData)

        return self.model.transform(transformedTestData)

    def compute_error(self, predictions):
        evaluator = MulticlassClassificationEvaluator(
            labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)

        return 1.0 - accuracy

    def _plot_confusion_matrix(self, cm, classes,
                              normalize=False,
                              title='Confusion matrix',
                              cmap=plt.cm.Blues):
        """
        This function prints and plots the confusion matrix.
        Normalization can be applied by setting `normalize=True`.
        """
        if normalize:
            cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
            print("Normalized confusion matrix")
        else:
            print('Confusion matrix, without normalization')

        print(cm)

        plt.imshow(cm, interpolation='nearest', cmap=cmap)
        plt.title(title)
        plt.colorbar()
        tick_marks = np.arange(len(classes))
        plt.xticks(tick_marks, classes, rotation=45)
        plt.yticks(tick_marks, classes)

        fmt = '.2f' if normalize else 'd'
        thresh = cm.max() / 2.
        for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
            plt.text(j, i, format(cm[i, j], fmt),
                     horizontalalignment="center",
                     color="white" if cm[i, j] > thresh else "black")

        plt.tight_layout()
        plt.ylabel('True label')
        plt.xlabel('Predicted label')

    def plot_confusion_matrix(self, predictions):
        class_temp = predictions.select("label").groupBy("label") \
            .count().sort('count', ascending=False).toPandas()
        class_temp = class_temp["label"].values.tolist()

        y_true = predictions.select("label")
        y_true = y_true.toPandas()

        y_pred = predictions.select("predictedLabel")
        y_pred = y_pred.toPandas()

        cnf_matrix = confusion_matrix(y_true, y_pred, labels=class_temp)

        plt.figure()
        self._plot_confusion_matrix(cnf_matrix, classes=class_temp,
                              title='Confusion matrix')
        plt.show()


if __name__ == "__main__":
    classifier = Classifier()
    predictions = classifier.run()
    predictions.show(5)
    print(classifier.compute_error(predictions))
