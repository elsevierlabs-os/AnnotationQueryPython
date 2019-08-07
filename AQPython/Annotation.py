from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import MapType
from pyspark.sql.types import LongType
from pyspark.sql.types import ArrayType

def AQSchema():
  """AQAnnotation Schema.
    Document Id (such as PII)  
    Annotation set (such as scnlp, ge)     
    Annotation type (such as text, sentence)    
    Starting offset for the annotation (based on the text file for the document)
    Ending offset for the annotation (based on the text file for the document)
    Annotation Id (after the annotations have been reordered)
    Contains any attributes such as exclude annotations, original annotation id, parent id, etc. Stored as a map.                   
  """
  return StructType([StructField('docId', StringType(), False),
                     StructField('annotSet', StringType(), False),
                     StructField('annotType', StringType(), False),
                     StructField('startOffset', LongType(), False),
                     StructField('endOffset', LongType(), False),
                     StructField('annotId', LongType(), False),
                     StructField('properties', MapType(StringType(), StringType()), True)])                  

def AQSchemaList():
  """Schema used for Preceding and Following functions.
  """
  return StructType([StructField('annot', AQSchema(), False),
                     StructField('annots',ArrayType(AQSchema(),True),True)])

def CATSchema():
  """CATAnnotation Schema.
    Document Id (such as PII)  
    Annotation set (such as scnlp, ge)     
    Annotation type (such as text, sentence)    
    Starting offset for the annotation (based on the text file for the document)
    Ending offset for the annotation (based on the text file for the document)
    Annotation Id (after the annotations have been reordered)
    Other contains any attributes such as exclude annotations, original annotation id, parent id, etc. Stored as a name-value & delimited string.
  """
  return StructType([StructField('docId', StringType(), False),
                     StructField('annotSet', StringType(), False),
                     StructField('annotType', StringType(), False),
                     StructField('startOffset', LongType(), False),
                     StructField('endOffset', LongType(), False),
                     StructField('annotId', LongType(), False),
                     StructField('other', StringType(), True)])