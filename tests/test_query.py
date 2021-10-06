# -*- coding: utf-8 -*-
import unittest
from AQPython.Query import *
from AQPython.Utilities import *
import pyspark 
from pyspark.storagelevel import StorageLevel

class QueryTestSuite(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        spark = pyspark.sql.SparkSession.builder \
                       .getOrCreate()   

        spark.conf.set("spark.sql.shuffle.partitions",4)

        cls.omAnnots = GetAQAnnotations(spark.read.parquet("./tests/resources/om/"),
                                    ["orig"],
                                    ["orig"],
                                    ["orig"],
                                    int(spark.conf.get("spark.sql.shuffle.partitions"))) \
                                    .persist(StorageLevel.DISK_ONLY)

        cls.geniaAnnots = GetAQAnnotations(spark.read.parquet("./tests/resources/genia/"),
                                      ["orig", "lemma", "pos"],
                                      ["lemma", "pos"],
                                      ["orig", "lemma"],
                                      int(spark.conf.get("spark.sql.shuffle.partitions"))) \
                                      .persist(StorageLevel.DISK_ONLY)

        cls.annots = cls.omAnnots.union(cls.geniaAnnots) \
                         .repartition(int(spark.conf.get("spark.sql.shuffle.partitions")),col("docId")) \
                         .sortWithinPartitions(col("docId"),col("startOffset"),col("endOffset")) \
                         .persist(StorageLevel.DISK_ONLY)

    @classmethod
    def tearDownClass(cls):
        cls.omAnnots.unpersist()
        cls.geniaAnnots.unpersist()
        cls.annots.unpersist()


    # Test FilterProperty
    def test_FilterProperty1(self):
        self.assertEquals(108, FilterProperty(self.annots, "orig", "and").count())

    def test_FilterProperty2(self):
        self.assertEquals(5,FilterProperty(self.annots, "orig", "and", limit = 5).count())

    def test_FilterProperty3(self):
        self.assertEquals(3907,FilterProperty(self.annots, "orig", "and", negate = True).count())

    def test_FilterProperty4(self):
        self.assertEquals(3907,FilterProperty(self.annots, "orig", "and", valueCompare = "!=").count())

    def test_FilterProperty5(self):
        self.assertEquals(153,FilterProperty(self.annots, "orig", valueArr = ["and", "to"]).count())

    # Test RegexProperty
    def test_RegexProperty1(self):
        self.assertEquals(135,RegexProperty(self.annots, "orig", ".*and.*").count())

    def test_RegexProperty2(self):
        self.assertEquals(3880,RegexProperty(self.annots, "orig", ".*and.*", negate = True).count())

    # Test FilterSet
    def test_FilterSet1(self):
        self.assertEquals(4066,FilterSet(self.annots, "ge").count())

    def test_FilterSet2(self):
        self.assertEquals(9754,FilterSet(self.annots, "om").count())

    def test_FilterSet3(self):
        self.assertEquals(13820,FilterSet(self.annots, annotSetArr = ["ge", "om"]).count())

    # Test FilterType
    def test_FilterType1(self):
        self.assertEquals(128,FilterType(self.annots, "sentence").count())

    def test_FilterType2(self):
        self.assertEquals(142,FilterType(self.annots, "ce:para").count())

    def test_FilterType3(self):
        self.assertEquals(270,FilterType(self.annots, annotTypeArr = ["sentence", "ce:para"]).count())

    # Test Contains
    def test_Contains1(self):
        self.assertEquals(138,Contains(FilterType(self.annots, "ce:para"), FilterType(self.annots, "sentence")).count())       

    def test_Contains2(self):
        self.assertEquals(4,Contains(FilterType(self.annots, "ce:para"), FilterType(self.annots, "sentence"), negate = True).count())       

    # Test ContainedIn
    def test_ContainedIn1(self):
        self.assertEquals(126,ContainedIn(FilterType(self.annots, "sentence"), FilterType(self.annots, "ce:para")).count())       

    def test_ContainedIn2(self):
        self.assertEquals(2,ContainedIn(FilterType(self.annots, "sentence"), FilterType(self.annots, "ce:para"), negate = True).count())       

    # Test ContainedInList
    def test_ContainedInList1(self):
        result = ContainedInList(FilterProperty(self.annots,'orig','polynomial'),FilterType(self.annots, 'sentence')) \
                 .collect()
        sortedResults =  sorted(result, key=lambda tup: (tup[0]["startOffset"],tup[0]["endOffset"]))
        self.assertEquals(1, len(sortedResults[0][1]))
        self.assertEquals(Row(annotId=1, annotSet='ge', annotType='sentence', docId='S0022314X13001777', endOffset=18607, properties={}, startOffset=18546),
                          Row(annotId=sortedResults[0][0].annotId,
                              annotSet=sortedResults[0][0].annotSet,
                              annotType=sortedResults[0][0].annotType,
                              docId=sortedResults[0][0].docId,
                              endOffset=sortedResults[0][0].endOffset,
                              properties=sortedResults[0][0].properties,
                              startOffset=sortedResults[0][0].startOffset))  
        self.assertEquals(Row(annotId=7, annotSet='ge', annotType='word', docId='S0022314X13001777', endOffset=18574, properties={'lemma': 'polynomial', 'orig': 'polynomial', 'pos': 'jj'}, startOffset=18564),
                          Row(annotId=sortedResults[0][1][0].annotId,
                              annotSet=sortedResults[0][1][0].annotSet,
                              annotType=sortedResults[0][1][0].annotType,
                              docId=sortedResults[0][1][0].docId,
                              endOffset=sortedResults[0][1][0].endOffset,
                              properties=sortedResults[0][1][0].properties,
                              startOffset=sortedResults[0][1][0].startOffset))

    # Test Before
    def test_Before1(self):
        self.assertEquals(47,Before(FilterProperty(self.annots, "orig", "polynomial"), FilterProperty(self.annots, "orig", "function")).count())       

    def test_Before2(self):
        self.assertEquals(11,Before(FilterProperty(self.annots, "orig", "polynomial"), FilterProperty(self.annots, "orig", "function"), dist = 50).count())       

    def test_Before3(self):
        self.assertEquals(36,Before(FilterProperty(self.annots, "orig", "polynomial"), FilterProperty(self.annots, "orig", "function"), dist = 50, negate = True).count())       

    # Test After
    def test_After1(self):
        self.assertEquals(27,After(FilterProperty(self.annots, "orig", "function"), FilterProperty(self.annots, "orig", "polynomial")).count())       

    def test_After2(self):
        self.assertEquals(9,After(FilterProperty(self.annots, "orig", "function"), FilterProperty(self.annots, "orig", "polynomial"), dist = 50).count())       
    
    def test_After3(self):
        self.assertEquals(18,After(FilterProperty(self.annots, "orig", "function"), FilterProperty(self.annots, "orig", "polynomial"), dist = 50, negate = True).count())       

    # Test Between
    def test_Between1(self):
        self.assertEquals(2,Between(FilterProperty(self.annots, "orig", "permutations"), FilterProperty(self.annots, "orig", "polynomial"), FilterProperty(self.annots, "orig", "integers")).count())       

    def test_Between2(self):
        self.assertEquals(2,Between(FilterProperty(self.annots, "orig", "permutations"), FilterProperty(self.annots, "orig", "polynomial"), FilterProperty(self.annots, "orig", "integers"), dist = 50).count())       
    
    def test_Between3(self):
        self.assertEquals(16,Between(FilterProperty(self.annots, "orig", "permutations"), FilterProperty(self.annots, "orig", "polynomial"), FilterProperty(self.annots, "orig", "integers"), dist = 50, negate = True).count())       

    # Test Sequence
    def test_Sequence1(self):
        self.assertEquals(5,Sequence(FilterProperty(self.annots, "orig", "to"), FilterProperty(self.annots, "orig", "be"), dist = 5).count())       

    # Test Or
    def test_Or1(self):
        self.assertEquals(13820,Or(self.omAnnots, self.geniaAnnots).count())       

    # Test And
    def test_And1(self):
        self.assertEquals(9754,And(self.omAnnots, self.omAnnots).count()) 

    def test_And2(self):
        self.assertEquals(0,And(self.omAnnots, self.omAnnots, negate = True).count()) 

    def test_And3(self):
        self.assertEquals(9754,And(self.omAnnots, self.geniaAnnots).count()) 

    def test_And4(self):
        self.assertEquals(0,And(self.omAnnots, self.geniaAnnots, negate = True).count()) 

    def test_And5(self):
        self.assertEquals(9754,And(self.omAnnots, self.omAnnots, leftOnly = False).count())    

    def test_And6(self):
        self.assertEquals(13820,And(self.omAnnots, self.geniaAnnots, leftOnly = False).count())  
 
    # Test MatchProperty
    def test_MatchProperty1(self):
        self.assertEquals(1,MatchProperty(self.omAnnots, FilterType(self.omAnnots, "xocs:doi"), "orig").count())  

    # Test Preceding
    def test_Preceding1(self):
        result = Preceding(FilterType(self.annots, "sentence"), Contains(FilterType(self.annots, "sentence"), FilterProperty(self.annots, "orig", "function"))) \
                 .collect()
        sortedResults =  sorted(result, key=lambda tup: (tup[0]["startOffset"],tup[0]["endOffset"]))
        self.assertEquals(2, len(sortedResults[0][1]))
        self.assertEquals(Row(annotId=59, annotSet='ge', annotType='sentence', docId='S0022314X13001777', endOffset=19739, properties={}, startOffset=19649),
                          Row(annotId=sortedResults[0][0].annotId,
                              annotSet=sortedResults[0][0].annotSet,
                              annotType=sortedResults[0][0].annotType,
                              docId=sortedResults[0][0].docId,
                              endOffset=sortedResults[0][0].endOffset,
                              properties=sortedResults[0][0].properties,
                              startOffset=sortedResults[0][0].startOffset))
        self.assertEquals(Row(annotId=14, annotSet='ge', annotType='sentence', docId='S0022314X13001777', endOffset=19471, properties={}, startOffset=19280),
                          Row(annotId=sortedResults[0][1][0].annotId,
                              annotSet=sortedResults[0][1][0].annotSet,
                              annotType=sortedResults[0][1][0].annotType,
                              docId=sortedResults[0][1][0].docId,
                              endOffset=sortedResults[0][1][0].endOffset,
                              properties=sortedResults[0][1][0].properties,
                              startOffset=sortedResults[0][1][0].startOffset))
        self.assertEquals(Row(annotId=1, annotSet='ge', annotType='sentence', docId='S0022314X13001777', endOffset=18607, properties={}, startOffset=18546),
                          Row(annotId=sortedResults[0][1][1].annotId,
                              annotSet=sortedResults[0][1][1].annotSet,
                              annotType=sortedResults[0][1][1].annotType,
                              docId=sortedResults[0][1][1].docId,
                              endOffset=sortedResults[0][1][1].endOffset,
                              properties=sortedResults[0][1][1].properties,
                              startOffset=sortedResults[0][1][1].startOffset))                              
    # Test Preceding
    def test_Preceding2(self):
        result = Preceding(FilterType(self.annots, "sentence"), Contains(FilterType(self.annots, "sentence"), FilterProperty(self.annots, "orig", "function")), FilterType(self.annots,'ce:para')) \
                 .collect()
        sortedResults =  sorted(result, key=lambda tup: (tup[0]["startOffset"],tup[0]["endOffset"]))
        self.assertEquals(0, len(sortedResults[0][1]))
        self.assertEquals(Row(annotId=59, annotSet='ge', annotType='sentence', docId='S0022314X13001777', endOffset=19739, properties={}, startOffset=19649),
                          Row(annotId=sortedResults[0][0].annotId,
                              annotSet=sortedResults[0][0].annotSet,
                              annotType=sortedResults[0][0].annotType,
                              docId=sortedResults[0][0].docId,
                              endOffset=sortedResults[0][0].endOffset,
                              properties=sortedResults[0][0].properties,
                              startOffset=sortedResults[0][0].startOffset))
        
    # Test Following
    def test_Following1(self):
        result = Following(FilterType(self.annots, "sentence"), Contains(FilterType(self.annots, "sentence"), FilterProperty(self.annots, "orig", "function")),cnt=20) \
                 .collect()
        sortedResults =  sorted(result, key=lambda tup: (tup[0]["startOffset"],tup[0]["endOffset"]))
        self.assertEquals(20, len(sortedResults[0][1]))
        self.assertEquals(Row(annotId=59, annotSet='ge', annotType='sentence', docId='S0022314X13001777', endOffset=19739, properties={}, startOffset=19649),
                          Row(annotId=sortedResults[0][0].annotId,
                              annotSet=sortedResults[0][0].annotSet,
                              annotType=sortedResults[0][0].annotType,
                              docId=sortedResults[0][0].docId,
                              endOffset=sortedResults[0][0].endOffset,
                              properties=sortedResults[0][0].properties,
                              startOffset=sortedResults[0][0].startOffset))       
        self.assertEquals(Row(annotId=80, annotSet='ge', annotType='sentence', docId='S0022314X13001777', endOffset=19911, properties={}, startOffset=19740),
                          Row(annotId=sortedResults[0][1][0].annotId,
                              annotSet=sortedResults[0][1][0].annotSet,
                              annotType=sortedResults[0][1][0].annotType,
                              docId=sortedResults[0][1][0].docId,
                              endOffset=sortedResults[0][1][0].endOffset,
                              properties=sortedResults[0][1][0].properties,
                              startOffset=sortedResults[0][1][0].startOffset))      
        self.assertEquals(Row(annotId=119, annotSet='ge', annotType='sentence', docId='S0022314X13001777', endOffset=20133, properties={}, startOffset=19912),
                          Row(annotId=sortedResults[0][1][1].annotId,
                              annotSet=sortedResults[0][1][1].annotSet,
                              annotType=sortedResults[0][1][1].annotType,
                              docId=sortedResults[0][1][1].docId,
                              endOffset=sortedResults[0][1][1].endOffset,
                              properties=sortedResults[0][1][1].properties,
                              startOffset=sortedResults[0][1][1].startOffset))          
        self.assertEquals(Row(annotId=167, annotSet='ge', annotType='sentence', docId='S0022314X13001777', endOffset=20311, properties={}, startOffset=20134),
                          Row(annotId=sortedResults[0][1][2].annotId,
                              annotSet=sortedResults[0][1][2].annotSet,
                              annotType=sortedResults[0][1][2].annotType,
                              docId=sortedResults[0][1][2].docId,
                              endOffset=sortedResults[0][1][2].endOffset,
                              properties=sortedResults[0][1][2].properties,
                              startOffset=sortedResults[0][1][2].startOffset))      

    # Test Following
    def test_Following2(self):
        result = Following(FilterType(self.annots, "sentence"), Contains(FilterType(self.annots, "sentence"), FilterProperty(self.annots, "orig", "function")),FilterType(self.annots,'ce:para'),20) \
                 .collect()
        sortedResults =  sorted(result, key=lambda tup: (tup[0]["startOffset"],tup[0]["endOffset"]))
        self.assertEquals(3, len(sortedResults[0][1]))
        self.assertEquals(Row(annotId=59, annotSet='ge', annotType='sentence', docId='S0022314X13001777', endOffset=19739, properties={}, startOffset=19649),
                          Row(annotId=sortedResults[0][0].annotId,
                              annotSet=sortedResults[0][0].annotSet,
                              annotType=sortedResults[0][0].annotType,
                              docId=sortedResults[0][0].docId,
                              endOffset=sortedResults[0][0].endOffset,
                              properties=sortedResults[0][0].properties,
                              startOffset=sortedResults[0][0].startOffset))         
        self.assertEquals(Row(annotId=80, annotSet='ge', annotType='sentence', docId='S0022314X13001777', endOffset=19911, properties={}, startOffset=19740),
                          Row(annotId=sortedResults[0][1][0].annotId,
                              annotSet=sortedResults[0][1][0].annotSet,
                              annotType=sortedResults[0][1][0].annotType,
                              docId=sortedResults[0][1][0].docId,
                              endOffset=sortedResults[0][1][0].endOffset,
                              properties=sortedResults[0][1][0].properties,
                              startOffset=sortedResults[0][1][0].startOffset))      
        self.assertEquals(Row(annotId=119, annotSet='ge', annotType='sentence', docId='S0022314X13001777', endOffset=20133, properties={}, startOffset=19912),
                          Row(annotId=sortedResults[0][1][1].annotId,
                              annotSet=sortedResults[0][1][1].annotSet,
                              annotType=sortedResults[0][1][1].annotType,
                              docId=sortedResults[0][1][1].docId,
                              endOffset=sortedResults[0][1][1].endOffset,
                              properties=sortedResults[0][1][1].properties,
                              startOffset=sortedResults[0][1][1].startOffset))           
        self.assertEquals(Row(annotId=167, annotSet='ge', annotType='sentence', docId='S0022314X13001777', endOffset=20311, properties={}, startOffset=20134),
                          Row(annotId=sortedResults[0][1][2].annotId,
                              annotSet=sortedResults[0][1][2].annotSet,
                              annotType=sortedResults[0][1][2].annotType,
                              docId=sortedResults[0][1][2].docId,
                              endOffset=sortedResults[0][1][2].endOffset,
                              properties=sortedResults[0][1][2].properties,
                              startOffset=sortedResults[0][1][2].startOffset))    

    def test_TokensSpan(self):
        result = TokensSpan(FilterType(self.annots,"word"),FilterType(self.annots,"sentence"),"orig").collect()
        sortedResults =  sorted(result, key=lambda rec: (rec.docId,rec.startOffset))   
        self.assertEquals(128,len(sortedResults)) 
        self.assertEquals(Row(annotId=1, annotSet='ge', annotType='sentence', docId='S0022314X13001777', endOffset=18607, properties={'origToksEpos' : '5|18551 14|18560 17|18563 28|18574 41|18587 44|18590 48|18594 57|18603 61|18607', 'origToksSpos' : '0|18546 6|18552 15|18561 18|18564 29|18575 42|18588 45|18591 49|18595 58|18604', 'origToksStr': 'Sylow p-groups of polynomial permutations on the integers mod'},startOffset=18546),
                          Row(annotId=sortedResults[0].annotId,
                              annotSet=sortedResults[0].annotSet,
                              annotType=sortedResults[0].annotType,
                              docId=sortedResults[0].docId,
                              endOffset=sortedResults[0].endOffset,
                              properties=sortedResults[0].properties,
                              startOffset=sortedResults[0].startOffset))    

    def test_RegexTokensSpan(self):
        tokensSpan = TokensSpan(FilterType(self.annots,"word"),FilterType(self.annots,"sentence"),"orig")
        result = RegexTokensSpan(tokensSpan,"orig",r"(?i)(?<= |^)poly[a-z]+ perm[a-z]+(?= |$$)","newSet","newType").collect()
        sortedResults =  sorted(result, key=lambda rec: (rec.docId,rec.startOffset)) 
        self.assertEquals(18,len(sortedResults))
        self.assertEquals(Row(annotId=1, annotSet='newSet', annotType='newType', docId='S0022314X13001777', endOffset=18587, properties={'origMatch': 'polynomial permutations'}, startOffset=18564),
                         Row(annotId=sortedResults[0].annotId,
                              annotSet=sortedResults[0].annotSet,
                              annotType=sortedResults[0].annotType,
                              docId=sortedResults[0].docId,
                              endOffset=sortedResults[0].endOffset,
                              properties=sortedResults[0].properties,
                              startOffset=sortedResults[0].startOffset))
            
if __name__ == "__main__":
    unittest.main()