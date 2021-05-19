# -*- coding: utf-8 -*-
import unittest
from AQPython.Utilities import *
from AQPython.Query import *
import pyspark 
from pyspark.sql import Row

class UtilitiesTestSuite(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        spark = pyspark.sql.SparkSession.builder \
                       .getOrCreate()   

        spark.conf.set("spark.sql.shuffle.partitions",4)

        if os.path.exists("/tmp/S0022314X13001777"):
            os.remove("/tmp/S0022314X13001777")

    # Test GetAQAnnotations count 
    def test_Utilities1(self):
        annots = GetAQAnnotations(spark.read.parquet("./tests/resources/genia/"),
                                      ["orig", "lemma", "pos", "excludes"],
                                      ["lemma", "pos"],
                                      ["orig", "lemma"])
        self.assertEquals(4066, annots.count())

    # Test GetAQAnnotations annotation
    def test_Utilities2(self):
        annots = GetAQAnnotations(spark.read.parquet("./tests/resources/genia/"),
                                      ["orig", "lemma", "pos", "excludes"],
                                      ["lemma", "pos"],
                                      ["orig", "lemma"]) \
                                    .orderBy(["docId", "startOffset","endOffset","annotType"])
        result = annots.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").collect()[0]
        expected = Row(docId='S0022314X13001777', annotSet='ge', annotType='word', startOffset=18546, endOffset=18551, annotId=3, properties={'lemma': 'sylow', 'pos': 'jj', 'orig': 'Sylow'})
        self.assertEquals(result.docId,expected.docId)
        self.assertEquals(result.annotSet,expected.annotSet)
        self.assertEquals(result.annotType,expected.annotType)
        self.assertEquals(result.startOffset,expected.startOffset)
        self.assertEquals(result.endOffset,expected.endOffset)
        self.assertEquals(result.annotId,expected.annotId)
        self.assertEquals(result.properties,expected.properties)

    # Test GetAQAnnotations property wildcard
    def test_Utilities3(self):
        annots = GetAQAnnotations(spark.read.parquet("./tests/resources/genia/"),
                                        ["*"]) \
                                    .orderBy(["docId", "startOffset","endOffset","annotType"])

        result = annots.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").collect()[0]
        expected = Row(docId='S0022314X13001777', annotSet='ge', annotType='word', startOffset=18546, endOffset=18551, annotId=3, properties={'lemma': 'sylow', 'origAnnotID': '4055', 'pos': 'JJ', 'orig': 'Sylow', 'tokidx': '1', 'parentId': '4054'})
        self.assertEquals(result.docId,expected.docId)
        self.assertEquals(result.annotSet,expected.annotSet)
        self.assertEquals(result.annotType,expected.annotType)
        self.assertEquals(result.startOffset,expected.startOffset)
        self.assertEquals(result.endOffset,expected.endOffset)
        self.assertEquals(result.annotId,expected.annotId)
        self.assertEquals(result.properties,expected.properties)        

    # Test GetAQAnnotations lower case wildcard
    def test_Utilities4(self):
        annots = GetAQAnnotations(spark.read.parquet("./tests/resources/genia/"),
                                      ["*"],
                                      ["*"]) \
                                    .orderBy(["docId", "startOffset","endOffset","annotType"])

        result = annots.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").collect()[0]
        expected = Row(docId='S0022314X13001777', annotSet='ge', annotType='word', startOffset=18546, endOffset=18551, annotId=3, properties={'lemma': 'sylow', 'origAnnotID': '4055', 'pos': 'jj', 'orig': 'sylow', 'tokidx': '1', 'parentId': '4054'})
        self.assertEquals(result.docId,expected.docId)
        self.assertEquals(result.annotSet,expected.annotSet)
        self.assertEquals(result.annotType,expected.annotType)
        self.assertEquals(result.startOffset,expected.startOffset)
        self.assertEquals(result.endOffset,expected.endOffset)
        self.assertEquals(result.annotId,expected.annotId)
        self.assertEquals(result.properties,expected.properties) 

    # Test GetAQAnnotations url decode wildcard
    def test_Utilities5(self):
        annots = GetAQAnnotations(spark.read.parquet("./tests/resources/genia/"),
                                      ["*"],
                                      [],
                                      ['*"']) \
                                    .orderBy(["docId", "startOffset","endOffset","annotType"])

        result = annots.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").collect()[0]
        expected = Row(docId='S0022314X13001777', annotSet='ge', annotType='word', startOffset=18546, endOffset=18551, annotId=3, properties={'lemma': 'sylow', 'origAnnotID': '4055', 'pos': 'JJ', 'orig': 'Sylow', 'tokidx': '1', 'parentId': '4054'})
        self.assertEquals(result.docId,expected.docId)
        self.assertEquals(result.annotSet,expected.annotSet)
        self.assertEquals(result.annotType,expected.annotType)
        self.assertEquals(result.startOffset,expected.startOffset)
        self.assertEquals(result.endOffset,expected.endOffset)
        self.assertEquals(result.annotId,expected.annotId)
        self.assertEquals(result.properties,expected.properties)        

    # Test GetCATAnnotations count
    def test_Utilities6(self):
        annots = GetAQAnnotations(spark.read.parquet("./tests/resources/genia/"),
                                      ["orig", "lemma", "pos", "excludes"],
                                      ["lemma", "pos"],
                                      ["orig", "lemma"])   

        catAnnots = GetCATAnnotations(annots,["orig", "lemma", "pos"],["orig", "lemma"])
        self.assertEquals(4066, catAnnots.count())  

    # Test GetCATAnnotations annotation
    def test_Utilities7(self):
        annots = GetAQAnnotations(spark.read.parquet("./tests/resources/genia/"),
                                      ["orig", "lemma", "pos", "excludes"],
                                      ["lemma", "pos"],
                                      ["orig", "lemma"])  

        catAnnots = GetCATAnnotations(annots,["orig", "lemma", "pos"],["orig", "lemma"]) \
                                        .orderBy(["docId", "startOffset","endOffset"])

        result = catAnnots.select("annotId","annotSet","annotType","docId","endOffset","other","startOffset").collect()[3]
        expected = Row(docId='S0022314X13001777', annotSet='ge', annotType='word', startOffset=18552, endOffset=18560, annotId=4, other='lemma=p-group&pos=nns&orig=p-groups')
        self.assertEquals(result.docId,expected.docId)
        self.assertEquals(result.annotSet,expected.annotSet)
        self.assertEquals(result.annotType,expected.annotType)
        self.assertEquals(result.startOffset,expected.startOffset)
        self.assertEquals(result.endOffset,expected.endOffset)
        self.assertEquals(result.annotId,expected.annotId)
        self.assertEquals(result.other,expected.other)

    # Test GetCATAnnotations property wildcard
    def test_Utilities8(self):
        annots = GetAQAnnotations(spark.read.parquet("./tests/resources/genia/"),
                                      ["orig", "lemma", "pos", "excludes"],
                                      ["lemma", "pos"],
                                      ["orig", "lemma"])     

        catAnnots = GetCATAnnotations(annots,["*"]) \
                                        .orderBy(["docId", "startOffset","endOffset"])

        result = catAnnots.select("annotId","annotSet","annotType","docId","endOffset","other","startOffset").collect()[3]
        expected = Row(docId='S0022314X13001777', annotSet='ge', annotType='word', startOffset=18552, endOffset=18560, annotId=4, other='lemma=p-group&pos=nns&orig=p-groups')
        self.assertEquals(result.docId,expected.docId)
        self.assertEquals(result.annotSet,expected.annotSet)
        self.assertEquals(result.annotType,expected.annotType)
        self.assertEquals(result.startOffset,expected.startOffset)
        self.assertEquals(result.endOffset,expected.endOffset)
        self.assertEquals(result.annotId,expected.annotId)
        self.assertEquals(result.other,expected.other)

    # Test GetCATAnnotations encode wildcard
    def test_Utilities9(self):
        annots = GetAQAnnotations(spark.read.parquet("./tests/resources/genia/"),
                                      ["orig", "lemma", "pos", "excludes"],
                                      ["lemma", "pos"],
                                      ["orig", "lemma"])     

        catAnnots = GetCATAnnotations(annots,["*"],["*"]) \
                                        .orderBy(["docId", "startOffset","endOffset"])
                
        result = catAnnots.select("annotId","annotSet","annotType","docId","endOffset","other","startOffset").collect()[3]
        expected = Row(docId='S0022314X13001777', annotSet='ge', annotType='word', startOffset=18552, endOffset=18560, annotId=4, other='lemma=p-group&pos=nns&orig=p-groups')
        self.assertEquals(result.docId,expected.docId)
        self.assertEquals(result.annotSet,expected.annotSet)
        self.assertEquals(result.annotType,expected.annotType)
        self.assertEquals(result.startOffset,expected.startOffset)
        self.assertEquals(result.endOffset,expected.endOffset)
        self.assertEquals(result.annotId,expected.annotId)
        self.assertEquals(result.other,expected.other)

    # Test Hydrate missing annotation file
    def test_Utilities10(self):
        annots = GetAQAnnotations(spark.read.parquet("./tests/resources/genia/"),
                                      ["orig", "lemma", "pos", "excludes"],
                                      ["lemma", "pos"],
                                      ["orig", "lemma"])  
        
        sentenceAnnots = FilterType(annots, "sentence").limit(1)
        hydratedAnnots = Hydrate(sentenceAnnots,"./tests/resources/junk/")       
        result = hydratedAnnots.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").collect()[0]
        #result = hydratedAnnots.select("annotId","annotSet","annotType","docId","endOffset","startOffset").collect()[0]
        expected = Row(docId='S0022314X13001777', annotSet='ge', annotType='sentence', startOffset=18546, endOffset=18607, annotId=1, properties={}) 
        #expected = Row(docId='S0022314X13001777', annotSet='ge', annotType='sentence', startOffset=18546, endOffset=18607, annotId=1) 
        self.assertEquals(result.docId,expected.docId)
        self.assertEquals(result.annotSet,expected.annotSet)
        self.assertEquals(result.annotType,expected.annotType)
        self.assertEquals(result.startOffset,expected.startOffset)
        self.assertEquals(result.endOffset,expected.endOffset)
        self.assertEquals(result.annotId,expected.annotId)

    # Test Hydrate sentence
    def test_Utilities11(self):
        annots = GetAQAnnotations(spark.read.parquet("./tests/resources/genia/"),
                                      ["orig", "lemma", "pos", "excludes"],
                                      ["lemma", "pos"],
                                      ["orig", "lemma"]) 

        sentenceAnnots = FilterType(annots, "sentence")                              
        hydratedAnnots = Hydrate(sentenceAnnots,"./tests/resources/str/")
        hydratedAnnots.count()
        result = hydratedAnnots.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").collect()[0]
        expected = Row(docId='S0022314X13001777', annotSet='ge', annotType='sentence', startOffset=18546, endOffset=18607, annotId=1, properties={'text': 'Sylow p-groups of polynomial permutations on the integers mod'})
        self.assertEquals(result.docId,expected.docId)
        self.assertEquals(result.annotSet,expected.annotSet)
        self.assertEquals(result.annotType,expected.annotType)
        self.assertEquals(result.startOffset,expected.startOffset)
        self.assertEquals(result.endOffset,expected.endOffset)
        self.assertEquals(result.annotId,expected.annotId)
        self.assertEquals(result.properties,expected.properties)

    # Test Hydrate sentence with excludes
    def test_Utilities12(self):
        annots = GetAQAnnotations(spark.read.parquet("./tests/resources/genia/"),
                                      ["orig", "lemma", "pos", "excludes"],
                                      ["lemma", "pos"],
                                      ["orig", "lemma"])   

        sentenceAnnots = FilterType(annots, "sentence") 
        hydratedAnnots = Hydrate(sentenceAnnots,"./tests/resources/str/")

        result = hydratedAnnots.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").collect()[8]
        expected = Row(docId='S0022314X13001777', annotSet='ge', annotType='sentence', startOffset=20490, endOffset=20777, annotId=256, properties={'excludes': '2872,om,mml:math,20501,20510|2894,om,mml:math,20540,20546|2907,om,mml:math,20586,20590|2913,om,mml:math,20627,20630|2923,om,mml:math,20645,20651|2933,om,mml:math,20718,20721', 'text': 'A function  arising from a polynomial in  or, equivalently, from a polynomial in , is called a polynomial function on . We denote by  the monoid with respect to composition of polynomial functions on . By monoid, we mean semigroup with an identity element.'})
        self.assertEquals(result.docId,expected.docId)
        self.assertEquals(result.annotSet,expected.annotSet)
        self.assertEquals(result.annotType,expected.annotType)
        self.assertEquals(result.startOffset,expected.startOffset)
        self.assertEquals(result.endOffset,expected.endOffset)
        self.assertEquals(result.annotId,expected.annotId)
        self.assertEquals(result.properties,expected.properties)

    # Test Hydrate sentence without excludes
    def test_Utilities13(self):
        annots = GetAQAnnotations(spark.read.parquet("./tests/resources/genia/"),
                                      ["orig", "lemma", "pos", "excludes"],
                                      ["lemma", "pos"],
                                      ["orig", "lemma"])   

        sentenceAnnots = FilterType(annots, "sentence") 
        hydratedAnnots = Hydrate(sentenceAnnots,"./tests/resources/str/",False)
        result = hydratedAnnots.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").collect()[8]
        expected = Row(docId='S0022314X13001777', annotSet='ge', annotType='sentence', startOffset=20490, endOffset=20777, annotId=256, properties={'excludes': '2872,om,mml:math,20501,20510|2894,om,mml:math,20540,20546|2907,om,mml:math,20586,20590|2913,om,mml:math,20627,20630|2923,om,mml:math,20645,20651|2933,om,mml:math,20718,20721', 'text': 'A function g:Zpn→Zpn arising from a polynomial in Zpn[x] or, equivalently, from a polynomial in Z[x], is called a polynomial function on Zpn. We denote by (Fn,∘) the monoid with respect to composition of polynomial functions on Zpn. By monoid, we mean semigroup with an identity element.'})
        self.assertEquals(result.docId,expected.docId)
        self.assertEquals(result.annotSet,expected.annotSet)
        self.assertEquals(result.annotType,expected.annotType)
        self.assertEquals(result.startOffset,expected.startOffset)
        self.assertEquals(result.endOffset,expected.endOffset)
        self.assertEquals(result.annotId,expected.annotId)
        self.assertEquals(result.properties,expected.properties) 