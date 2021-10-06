import sys
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql import SparkSession
from AQPython.Annotation import *

spark = SparkSession.builder.getOrCreate()

def FilterProperty(df, name, value="", valueArr=[], valueCompare="=", limit=0, negate=False):
  """Provide the ability to filter a Dataframe of AQAnnotations based on the value matching the specified property value in the map.

  Args:
    df: Dataframe of AQAnnotations that will be filtered by the specified property name and value.
    name: Name of the property to filter.
    value: Value of the named property to filter.
    valueArr: The array of values of the named property  to filter. An OR will be applied to the Strings. Only used if value was not specified.
    valueCompare: Comparison operator to use for the property filter. Default is '='. Possible values are '=' and '!=' when valueArr specified. Possible values are '=','!=','<','<=','>', and '>=' otherwise.
    limit: Number of AQAnnotations to return.
    negate: Whether to negate the entire query. Default is false.

  Returns:
    Dataframe of AQAnnotations

  """
  query = ""

  if value != "":

    query += ("properties.`" + name + "` " + valueCompare + " '" + value  + "'")

  elif len(valueArr) > 0:

    if valueCompare == '=':

      query += ("properties.`" + name + "` in " + "('" + "','".join(map(str,valueArr)) + "')")

    else:

      query += ("properties.`" + name + "` not in " + "('" + "','".join(map(str,valueArr)) + "')")

  if negate:

    query = "!(" + query + ")"

  results = df.filter(query)

  if limit > 0:
    results = results.limit(limit)

  return results


def RegexProperty(df, name, regex, limit=0, negate=False):
  """Provide the ability to filter a Dataframe of AQAnnotations based on the regex applied to the specified property value in the map.

  Args:
    df: Dataframe of AQAnnotations that will be filtered by the specified property name and regex expression.
    name: Name of the property to filter.
    regex: Regex expression to use for the filter.
    limit: Number of AQAnnotations to return.
    negate: Whether to negate the entire query. Default is false.

  Returns:
    Dataframe of AQAnnotations

  """
  query = "properties.`" + name + "` rlike " + "'" + regex + "'"

  if negate:

    query = "!(" + query + ")"

  results = df.filter(query)

  if limit > 0:

    results = results.limit(limit)

  return results


def FilterSet(df, annotSet="", annotSetArr=[], annotSetCompare="=", limit=0, negate=False):
  """Provide the ability to filter a Dataframe of AQAnnotations based on the value in the annotSet field.

  Args:
    df: Dataframe of AQAnnotations that will be filtered by the specified annotation set.
    annotSet: String to filter against the annotSet field in the dataset of AQAnnotations.
    annotSetArr: Array of Strings to filter against the annotSet field in the dataframe of AQAnnotations. An OR will be applied to the Strings.  Only used if annotSet was not specified.
    annotSetCompare: Comparison operator to use for the annotSet field in the dataframe of AQAnnotations.  Default is '='.  Possible values are '=' and '!='.
    limit: Number of AQAnnotations to return.
    negate: Whether to negate the entire query.  Default is false.

  Returns:
      Dataframe of AQAnnotations

  """
  query = ""

  if annotSet != "":

    query += ("annotSet " + annotSetCompare + " \"" + annotSet + "\"")

  elif len(annotSetArr) > 0:

      if annotSetCompare == "=":

        query += ("annotSet in " + "('" + "','".join(map(str,annotSetArr)) + "')")

      else:

        query += ("annotSet not in " + "('" + "','".join(map(str,annotSetArr)) + "')")

  if negate:

    query = "!(" + query + ")"

  results = df.filter(query)

  if limit > 0:

    results = results.limit(limit)

  return results


def FilterType(df, annotType="", annotTypeArr=[], annotTypeCompare="=", limit=0, negate=False):
  """Provide the ability to filter a Dataframe of AQAnnotations based on the value in the annotType field.

  Args:
    df: Dataframe of AQAnnotations that will be filtered by the specified annotation type.
    annotType: String to filter against the annotType field in the dataframe of AQAnnotations.
    annotTypeArr: Array of Strings to filter against the annotType field in the dataframe of AQAnnotations. An OR will be applied to the Strings.  Only used if annotType was not specified.
    annotTypeCompare: Comparison operator to use for the annotType field in the dataframe of AQAnnotations.  Default is '='.  Possible values are '=' and '!='.
    limit: Number of AQAnnotations to return.
    negate: Whether to negate the entire query.  Default is false.

  Returns:
    Dataframe of AQAnnotations

  """
  query = ""

  if annotType != "":

    query += ("annotType " + annotTypeCompare + " \"" + annotType + "\"")

  elif len(annotTypeArr) > 0:

      if annotTypeCompare == "=":

        query += ("annotType in " + "('" + "','".join(map(str,annotTypeArr)) + "')")

      else:

        query += ("annotType not in " + "('" + "','".join(map(str,annotTypeArr)) + "')")

  if negate:

    query = "!(" + query + ")"

  results = df.filter(query)

  if limit > 0:

    results = results.limit(limit)

  return results


def Contains(left, right, limit=0, negate=False):
  """Provide the ability to find annotations that contain another annotation.

    The input is 2 Dataframes of AQAnnotations. We will call them A and B.
    The purpose is to find those annotations in A that contain B. What that means is the start/end offset for an annotation from A must contain the start/end offset from an annotation in B.
    The start/end offsets are inclusive.  We ultimately return the container annotations (A) that meet this criteria.
    We also deduplicate the A annotations as there could be many annotations from B that could be contained by an annotation in A but it only makes sense to return the unique container annotations.
    There is also the option of negating the query (think Not Contains) so that we return only A where it does not contain B.

  Args:
    left: Dataframe of AQAnnotations, the ones we will return if they contain AQAnnotations from 'right'.
    right: Dataframe of AQAnnotations, the ones we are looking to see if they occur in the AQAnnotations from 'left'.
    limit: Number of AQAnnotations to return.
    negate: Whether to negate the entire query (think NOT contains).  Default is false.

  Returns:
    Dataframe of AQAnnotations

  """
  results = None

  # Workaround for Catalyst optimization issues when working with two Dataframes derived from the same Dataframe - Catalyst gets confused
  tmpRight = right.select("annotId", "annotSet", "annotType", "docId", "endOffset", "startOffset", "properties") \
                  .toDF("R_annotId", "R_annotSet", "R_annotType", "R_docId", "R_endOffset", "R_startOffset", 'R_properties')

  if negate:

    results = left.alias("L").join(tmpRight,
                                   ((col("L.docId") == col("R_docId")) &
                                    (col("L.startOffset") <= col("R_startOffset")) &
                                    (col("L.endOffset") >= col("R_endOffset")) &
                                    (~((col("L.annotSet") == col("R_annotSet")) &
                                     (col("L.annotType") == col("R_annotType")) &
                                     (col("L.startOffset") == col("R_startOffset")) &
                                     (col("L.endOffset") == col("R_endOffset"))))),"leftouter") \
                             .filter(col("R_docId").isNull()) \
                             .select("L.*")

  else:

    results = left.alias("L").join(tmpRight,
                                   ((col("L.docId") == col("R_docId")) &
                                    (col("L.startOffset") <= col("R_startOffset")) &
                                    (col("L.endOffset") >= col("R_endOffset")) &
                                    (~((col("L.annotSet") == col("R_annotSet")) &
                                     (col("L.annotType") == col("R_annotType")) &
                                     (col("L.startOffset") == col("R_startOffset")) &
                                     (col("L.endOffset") == col("R_endOffset"))))),"leftsemi")

  if limit > 0:

    results = results.limit(limit)

  return results



def ContainedIn(left, right, limit=0, negate=False):
  """Provide the ability to find annotations that are contained by another annotation.

    The input is 2 Dataframes of AQAnnotations. We will call them A and B.
    The purpose is to find those annotations in A that are contained in B.
    What that means is the start/end offset for an annotation from A must be contained by the start/end offset from an annotation in B.
    The start/end offsets are inclusive.  We ultimately return the contained annotations (A) that meet this criteria.
    There is also the option of negating the query (think Not Contains) so that we return only A where it is not contained in B.

  Args:
    left: Dataframe of AQAnnotations, the ones we will return if they are contained in AQAnnotations from 'right'.
    right: Dataframe of AQAnnotations, the ones we are looking to see if they contain AQAnnotations from 'left'.
    limit: Number of AQAnnotations to return.
    negate: Whether to negate the entire query (think NOT contained in).  Default is false.

  Returns:
    Dataframe of AQAnnotations

  """
  results = None

  # Workaround for Catalyst optimization issues when working with two Dataframes derived from the same Dataframe - Catalyst gets confused
  tmpRight = right.select("annotId", "annotSet", "annotType", "docId", "endOffset", "startOffset", "properties") \
                  .toDF("R_annotId", "R_annotSet", "R_annotType", "R_docId", "R_endOffset", "R_startOffset", 'R_properties')

  if negate:

    results = left.alias("L").join(tmpRight,
                                   ((col("L.docId") == col("R_docId")) &
                                    (col("L.startOffset") >= col("R_startOffset")) &
                                    (col("L.endOffset") <= col("R_endOffset")) &
                                    (~((col("L.annotSet") == col("R_annotSet")) &
                                     (col("L.annotType") == col("R_annotType")) &
                                     (col("L.startOffset") == col("R_startOffset")) &
                                     (col("L.endOffset") == col("R_endOffset"))))),"leftouter") \
                             .filter(col("R_docId").isNull()) \
                             .select("L.*")

  else:

    results = left.alias("L").join(tmpRight,
                                   ((col("L.docId") == col("R_docId")) &
                                    (col("L.startOffset") >= col("R_startOffset")) &
                                    (col("L.endOffset") <= col("R_endOffset")) &
                                    (~((col("L.annotSet") == col("R_annotSet")) &
                                     (col("L.annotType") == col("R_annotType")) &
                                     (col("L.startOffset") == col("R_startOffset")) &
                                     (col("L.endOffset") == col("R_endOffset"))))),"leftsemi")

  if limit > 0:

    results = results.limit(limit)

  return results


def ContainedInList(left, right):
  """Provide the ability to find annotations that are contained by another annotation.  
  The input is 2 Dataframes of AQAnnotations.  We will call them A and B.  
  The purpose is to find those annotations in A that are contained in B.  What that means is the start/end offset for an annotation from A  must be contained by the start/end offset from an annotation in  B.  
  We of course have to also match on the document id.  
  We ultimately return a Dataframe with 2 fields where the first field is an annotation from B and the second field is an array of entries from A
  that are contained in the first entry.   

  Args:
  left: Dataframe of AQAnnotations, the ones we will return (as a list) if they are contained in AQAnnotations from 'right'.
  right: Dataframe of AQAnnotations, the ones we are looking to see if they contain AQAnnotations from 'left'.
  
  Returns:
    Dataframe of (AQAnnotations,Array[AQAnnotations])
  """

  def containedAQ(rec):
    # Sort the contained annotations 
    #srecs = sorted(rec[1], key=lambda x: (-1 if x.LendOffset == None else x.LendOffset),reverse=True)
    srecs = sorted(rec[1], key=lambda x: (1 if x.LstartOffset == None else x.LstartOffset),reverse=False)

    # We can extract the key from the any 'right' entry in sorted recs (we will use the first one)
    key = Row(docId = srecs[0].RdocId,
              annotSet = srecs[0].RannotSet,
              annotType = srecs[0].RannotType,
              startOffset = int(srecs[0].RstartOffset),
              endOffset = int(srecs[0].RendOffset),
              annotId = int(srecs[0].RannotId),
              properties = srecs[0].Rproperties)

    # Construct the array
    values = []
    for rec in srecs:
      if rec.LdocId != None:
        values.append(Row(docId = rec.LdocId,
                          annotSet = rec.LannotSet,
                          annotType = rec.LannotType,
                          startOffset = int(rec.LstartOffset),
                          endOffset = int(rec.LendOffset),
                          annotId = int(rec.LannotId),
                          properties = rec.Lproperties))
    return(key,values)  

  l = left.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").toDF("LannotId","LannotSet","LannotType","LdocId","LendOffset","Lproperties","LstartOffset")
  r = right.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").toDF("RannotId","RannotSet","RannotType","RdocId","RendOffset","Rproperties","RstartOffset")

  results = l.join(r,
                   ((col("LdocId") == col("RdocId")) &
                    (col("LstartOffset") >= col("RstartOffset")) &
                    (col("LendOffset") <= col("RendOffset")) &
                    (~((col("LannotSet") == col("RannotSet")) &
                      (col("LannotType") == col("RannotType")) &
                      (col("LstartOffset") == col("RstartOffset")) &
                      (col("LendOffset") == col("RendOffset")))))) \
                    .rdd \
                    .groupBy(lambda x: (x["RdocId"],x["RstartOffset"],x["RendOffset"])) \
                    .map(lambda rec: containedAQ(rec))

  return spark.createDataFrame(results.map(lambda x: x),AQSchemaList())
  

def Before(left, right, dist=sys.maxsize , limit=0, negate=False):
  """Provide the ability to find annotations that are before another annotation.

    The input is 2 Dataframes of AQAnnotations. We will call them A and B.
    The purpose is to find those annotations in A that are before B.
    What that means is the end offset for an annotation from A must be before (or equal to) the start offset from an annotation in B.
    We ultimately return the A annotations that meet this criteria.
    A distance operator can also be optionally specified.
    This would require an A annotation (endOffset) to occur n characters (or less) before the B annotation (startOffset).
    There is also the option of negating the query (think Not Before) so that we return only A where it is not before B.

  Args:
    left: Dataframe of AQAnnotations, the ones we will return if they are before AQAnnotations from 'right'.
    right: Dataframe of AQAnnotations, the ones we are looking to see if are after AQAnnotations from 'left'.
    dist: Number of characters  where endOffset from 'left' must occur before startOffset from 'right'. Default is sys.maxsize.
    limit: Number of AQAnnotations to return.
    negate: Whether to negate the entire query (think NOT before).  Default is false.

  Returns:
    Dataframe of AQAnnotations

  """
  results = None

  # Workaround for Catalyst optimization issues when working with two Dataframes derived from the same Dataframe - Catalyst gets confused
  tmpRight = right.select("annotId", "annotSet", "annotType", "docId", "endOffset", "startOffset", "properties") \
                  .toDF("R_annotId", "R_annotSet", "R_annotType", "R_docId", "R_endOffset", "R_startOffset", 'R_properties')

  if negate:

    results = left.alias("L").join(tmpRight,
                                   ((col("L.docId") == col("R_docId")) &
                                    (col("R_startOffset") >=  col("L.endOffset")) &
                                    (col("R_startOffset") - col("L.endOffset") < dist)) &
                                    (~((col("L.annotSet") == col("R_annotSet")) &
                                     (col("L.annotType") == col("R_annotType")) &
                                     (col("L.startOffset") == col("R_startOffset")) &
                                     (col("L.endOffset") == col("R_endOffset")))),"leftouter") \
                             .filter(col("R_docId").isNull()) \
                             .select("L.*")

  else:

    results = left.alias("L").join(tmpRight,
                        ((col("L.docId") == col("R_docId")) &
                         (col("R_startOffset") >=  col("L.endOffset")) &
                         (col("R_startOffset") - col("L.endOffset") < dist) &
                         (~((col("L.annotSet") == col("R_annotSet")) &
                          (col("L.annotType") == col("R_annotType")) &
                          (col("L.startOffset") == col("R_startOffset")) &
                          (col("L.endOffset") == col("R_endOffset"))))),"leftsemi")

  if limit > 0:

    results = results.limit(limit)

  return results


def After(left, right, dist=sys.maxsize , limit=0, negate=False):
  """Provide the ability to find annotations that are after another annotation.

    The input is 2 Dataframes of AQAnnotations. We will call them A and B.
    The purpose is to find those annotations in A that are after B.
    What that means is the start offset for an annotation from A must be after (or equal to) the end offset from an annotation in B.
    We ultimately return the A annotations that meet this criteria.
    A distance operator can also be optionally specified.
    This would require an A annotation (startOffset) to occur n characters (or less) after the B annotation (endOffset).
    There is also the option of negating the query (think Not After) so that we return only A where it is not after B.

  Args:
    left: Dataframe of AQAnnotations, the ones we will return if they are after AQAnnotations from 'right'.
    right: Dataframe of AQAnnotations, the ones we are looking to see if are before AQAnnotations from 'left'.
    dist: Number of characters  where startOffset from 'left' must occur after endOffset from 'right'. Default is sys.maxsize.
    limit: Number of AQAnnotations to return.
    negate: Whether to negate the entire query (think NOT after).  Default is false.

  Returns:
    Dataframe of AQAnnotations

  """
  results = None

  # Workaround for Catalyst optimization issues when working with two Dataframes derived from the same Dataframe - Catalyst gets confused
  tmpRight = right.select("annotId", "annotSet", "annotType", "docId", "endOffset", "startOffset", "properties") \
                  .toDF("R_annotId", "R_annotSet", "R_annotType", "R_docId", "R_endOffset", "R_startOffset", 'R_properties')

  if negate:

    results = left.alias("L").join(tmpRight,
                                   ((col("L.docId") == col("R_docId")) &
                                    (col("L.startOffset") >=  col("R_endOffset")) &
                                    (col("L.startOffset") - col("R_endOffset") < dist)) &
                                    (~((col("L.annotSet") == col("R_annotSet")) &
                                     (col("L.annotType") == col("R_annotType")) &
                                     (col("L.startOffset") == col("R_startOffset")) &
                                     (col("L.endOffset") == col("R_endOffset")))),"leftouter") \
                             .filter(col("R_docId").isNull()) \
                             .select("L.*")

  else:

    results = left.alias("L").join(tmpRight,
                        ((col("L.docId") == col("R_docId")) &
                         (col("L.startOffset") >=  col("R_endOffset")) &
                         (col("L.startOffset") - col("R_endOffset") < dist) &
                         (~((col("L.annotSet") == col("R_annotSet")) &
                          (col("L.annotType") == col("R_annotType")) &
                          (col("L.startOffset") == col("R_startOffset")) &
                          (col("L.endOffset") == col("R_endOffset"))))),"leftsemi")

  if limit > 0:

    results = results.limit(limit)

  return results


def Between(middle, left, right, dist=sys.maxsize , limit=0, negate=False):
  """Provide the ability to find annotations that are before one annotation and after another.

    The input is 3 Dataframes of AQAnnotations. We will call them A, B and C.
    The purpose is to find those annotations in A that are before B and after C.
    What that means is the end offset for an annotation from A must be before (or equal to) the start offset from an annotation in B and the start offset for A be after (or equal to) the end offset from C.
    We ultimately return the A annotations that meet this criteria.
    A distance operator can also be optionally specified.
    This would require an A annotation (endOffset) to occur n characters (or less) before the B annotation (startOffset) and would require the A annotation (startOffset) to occur n characters (or less) after the C annotation (endOffset) .
    There is also the option of negating the query (think Not Between) so that we return only A where it is not before B nor after C.

  Args:
    middle: Dataframe of AQAnnotations, the ones we will return if they are between AQAnnotations from 'left' and AQAnnotations from 'right.
    left: Dataframe of AQAnnotations, the ones we are looking to see if they are before AQAnnotations from 'middle'.
    right: Dataframe of AQAnnotations, the ones we are looking to see if they are after AQAnnotations from 'middle'.
    dist: Number of characters  where startOffset from 'middle' must occur after endOffset of 'left' or endOffset from 'middle' must occur before startOffset of 'right'
    limit: Number of AQAnnotations to return.
    negate: Whether to negate the entire query (think NOT between).  Default is false.

  Returns:
    Dataframe of AQAnnotations

  """
  intermediate = None
  intermediate2 = None
  results = None

  if negate:

    intermediate = middle.alias("L").join(right.alias("R"),
                                   ((col("L.docId") == col("R.docId")) &
                                    (col("R.startOffset") >=  col("L.endOffset")) &
                                    (col("R.startOffset") - col("L.endOffset") < dist)) &
                                    (~((col("L.annotSet") == col("R.annotSet")) &
                                     (col("L.annotType") == col("R.annotType")) &
                                     (col("L.startOffset") == col("R.startOffset")) &
                                     (col("L.endOffset") == col("R.endOffset")))),"leftsemi")

    intermediate2 = intermediate.alias("L").join(left.alias("R"),
                                   ((col("L.docId") == col("R.docId")) &
                                    (col("L.startOffset") >=  col("R.endOffset")) &
                                    (col("L.startOffset") - col("R.endOffset") < dist) &
                                    (~((col("L.annotSet") == col("R.annotSet")) &
                                     (col("L.annotType") == col("R.annotType")) &
                                     (col("L.annotId") == col("R.annotId")) &
                                     (col("L.endOffset") == col("R.endOffset"))))),"leftsemi")


    results = middle.alias("L").join(intermediate2.alias("R"),
                                   ((col("L.docId") == col("R.docId")) &
                                    (col("L.annotSet") ==  col("R.annotSet")) &
                                    (col("L.annotType") == col("R.annotType")) &
                                    (col("L.annotId") == col("R.annotId"))),"leftouter") \
                               .filter(col("R.docId").isNull()) \
                               .select("L.*")
  else:

    intermediate = middle.alias("L").join(right.alias("R"),
                                   ((col("L.docId") == col("R.docId")) &
                                    (col("R.startOffset") >=  col("L.endOffset")) &
                                    (col("R.startOffset") - col("L.endOffset") < dist)) &
                                    (~((col("L.annotSet") == col("R.annotSet")) &
                                     (col("L.annotType") == col("R.annotType")) &
                                     (col("L.startOffset") == col("R.startOffset")) &
                                     (col("L.endOffset") == col("R.endOffset")))),"leftsemi")

    results = intermediate.alias("L").join(left.alias("R"),
                                   ((col("L.docId") == col("R.docId")) &
                                    (col("L.startOffset") >=  col("R.endOffset")) &
                                    (col("L.startOffset") - col("R.endOffset") < dist) &
                                    (~((col("L.annotSet") == col("R.annotSet")) &
                                     (col("L.annotType") == col("R.annotType")) &
                                     (col("L.startOffset") == col("R.startOffset")) &
                                     (col("L.endOffset") == col("R.endOffset"))))),"leftsemi")

  if limit > 0:

    results = results.limit(limit)

  return results


def Sequence(left, right, dist=sys.maxsize, limit=0):
  """Provide the ability to find annotations that are before another annotation.

    The input is 2 Dataframes of AQAnnotations. We will call them A and B.
    The purpose is to find those annotations in A that are before B.
    What that means is the end offset for an annotation from A must be before (or equal to) the start offset from an annotation in B.
    We ultimately return the annotations that meet this criteria.
    Unlike the Before function, we adjust the returned annotation a bit.
    For example, we set the annotType to "seq" and we use the A startOffset and the B endOffset.
    A distance operator can also be optionally specified. This would require an A annotation (endOffset) to occur n characters (or less) before the B annotation (startOffset).

  Args:
    left: Dataframe of AQAnnotations, the ones we will return if they are before AQAnnotations from 'right'.
    right: Dataframe of AQAnnotations, the ones we are looking to see if are after AQAnnotations from 'left'.
    dist: Number of characters  where endOffset from 'left' must occur before startOffset from 'right'. Default is sys.maxsize.
    limit: Number of AQAnnotations to return.

  Returns:
    Dataframe of AQAnnotations

 """
  results = None

  # Workaround for Catalyst optimization issues when working with two Dataframes derived from the same Dataframe - Catalyst gets confused
  tmpRight = right.select("annotId", "annotSet", "annotType", "docId", "endOffset", "startOffset", "properties") \
                  .toDF("R_annotId", "R_annotSet", "R_annotType", "R_docId", "R_endOffset", "R_startOffset", 'R_properties')

  results = left.alias("L").join(tmpRight,
                                 ((col("L.docId") == col("R_docId")) &
                                  (col("R_startOffset") >=  col("L.endOffset")) &
                                  (col("R_startOffset") - col("L.endOffset") < dist)) &
                                  (~((col("L.annotSet") == col("R_annotSet")) &
                                    (col("L.annotType") == col("R_annotType")) &
                                    (col("L.startOffset") == col("R_startOffset")) &
                                    (col("L.endOffset") == col("R_endOffset"))))) \
                           .select("L.docId", "L.annotSet", "L.startOffset", "R_endOffset", "L.annotId") \
                           .withColumnRenamed("R_endOffset", "endOffset") \
                           .withColumn("annotType",lit("seq")) \
                           .withColumn("properties",lit(None)) \
                           .dropDuplicates(["docId","annotSet","annotType","annotId","startOffset","endOffset"])


  if limit > 0:

    results = results.limit(limit)

  return results


def Or(left, right, limit=0):
  """Provide the ability to combine (union) Dataframes of AQAnnotations.

    The input is 2 Dataframes of AQAnnotations. The output is the union of these annotations.

  Args:
    left: Dataframe of AQAnnotations
    right: Dataframe of AQAnnotations
    limit: Number of AQAnnotations to return.

  Returns:
    Dataframe of AQAnnotations

  """
  results = None

  # Will change number of partitions (and impact performance)
  results = left.union(right) \
                .dropDuplicates(["docId","annotSet","annotType","annotId","startOffset","endOffset"])

  if limit > 0:

    results = results.limit(limit)

  return results


def And(left, right, limit=0, negate=False, leftOnly=True):
  """Provide the ability to find annotations that are in the same document.

    The input is 2 Dataframes of AQAnnotations. We will call them A and B.
    The purpose is to find those annotations in A and B that are in the same document.

  Args:
    left: Dataframe of AQAnnotations
    right: Dataframe of AQAnnotations.
    limit: Number of AQAnnotations to return.
    negate: think and NOT (only return annotations from A that are not in B).  Default is false.
    leftOnly: Reuturn only the left or the left and right.  The default is to only return the left.

  Returns:
    Dataframe of AQAnnotations

  """
  results = None

  # Workaround for Catalyst optimization issues when working with two Dataframes derived from the same Dataframe - Catalyst gets confused
  tmpRight = right.select("annotId", "annotSet", "annotType", "docId", "endOffset", "startOffset", "properties") \
                  .toDF("R_annotId", "R_annotSet", "R_annotType", "R_docId", "R_endOffset", "R_startOffset", 'R_properties')

  tmpLeft = left.select("annotId", "annotSet", "annotType", "docId", "endOffset", "startOffset", "properties") \
                  .toDF("L_annotId", "L_annotSet", "L_annotType", "L_docId", "L_endOffset", "L_startOffset", 'L_properties')

  if negate:

    results = left.alias("L").join(tmpRight.select("R_docId").distinct(),
                                   (col("L.docId") == col("R_docId")),"leftouter") \
                             .filter(col("R_docId").isNull()) \
                             .select("L.*")

  else:

    if leftOnly:

      results = left.alias("L").join(tmpRight.select("R_docId").distinct(),
                                     (col("L.docId") == col("R_docId")),"leftsemi")

    else:

      a = left.alias("L").join(tmpRight.select("R_docId").distinct(),
                               (col("L.docId") == col("R_docId")),"leftsemi")

      b = right.alias("L").join(tmpLeft.select("L_docId").distinct(),
                                (col("L.docId") == col("L_docId")),"leftsemi")

      # Will change number of partitions (and impact performance)
      results = a.union(b) \
                 .dropDuplicates(["docId","annotSet","annotType","annotId","startOffset","endOffset"])

  if limit > 0:

    results = results.limit(limit)

  return results



def MatchProperty(left, right, name, negate=False, limit=0):
  """Provide the ability to find annotations (looking at their property) that are in the same document.

    The input is 2 Dataframes of AQAnnotations. We will call them A and B.
    The purpose is to find those annotations in A that are in the same document as B and also match values on the specified property.

  Args:
    left: Dataframe of AQAnnotations, the ones we will return if they match AQAnnotations from 'right'.
    right: Dataframe of AQAnnotations the ones we are looking to see if they match AQAnnotations from 'left'.
    name: Name of the property to match.
    limit: Number of AQAnnotations to return.
    negate: Whether to negate the entire query (think NOT contains).  Default is false.

  Returns:
    Dataframe of AQAnnotations

 """
  results = None

  # Workaround for Catalyst optimization issues when working with two Dataframes derived from the same Dataframe - Catalyst gets confused
  tmpRight = right.select("annotId", "annotSet", "annotType", "docId", "endOffset", "startOffset", "properties") \
                  .toDF("R_annotId", "R_annotSet", "R_annotType", "R_docId", "R_endOffset", "R_startOffset", 'R_properties')

  if negate:

    results = left.alias("L").join(tmpRight,
                                   ((col("L.docId") == col("R_docId")) &
                                    (col("L.properties.`" + name + "`")  == col("R_properties.`" + name + "`"))) ,"leftouter") \
                           .filter(col("R_docId").isNull()) \
                           .select("L.*")

  else:

    results = left.alias("L").join(tmpRight,
                                   ((col("L.docId") == col("R_docId")) &
                                    (col("L.properties.`" + name + "`") == col("R_properties.`" + name + "`"))),"leftsemi")

  if limit > 0:

    results = results.limit(limit)

  return results


def Preceding(annot, anchor, container=None, cnt=3):
  """Provide the ability to find the preceding sibling annotations for every annotation in the anchor Dataframe of AQAnnotations.

    The preceding sibling annotations can optionally be required to be contained in a container Dataframe of AQAnnotations.
    The return type of this function is different from other functions.
    Instead of returning a Dataframe of AQAnnotations this function returns a Dataframe of (AQAnnotation,Array[AQAnnotation]).

  Args:
    annot: Dataframe of AQAnnotations, the ones we will be using to look for preceding sibling annotations.
    anchor: Dataframe of AQAnnotations  starting point for using to look for preceding sibling annotations (use the startOffset and docId).
    container: Dataframe of AQAnnotations to use when requiring the preceding sibling annotations to be contained in a specific annotation.
    cnt: Number of preceding sibling AQAnnotations to return.

  Returns:
    Dataframe of (AQAnnotation,Array[AQAnnotation])

  """
  # Get the preceding annotations
  def precedingAQ(rec,cnt):
    # Sort the preceding annotations (limit the number of results to the cnt)
    srecs = sorted(rec[1], key=lambda x: (-1 if x.LendOffset == None else x.LendOffset),reverse=True)[0:cnt]

    # We can extract the key from the any 'right' entry in sorted recs (we will use the first one)
    key = Row(docId = srecs[0].RdocId,
              annotSet = srecs[0].RannotSet,
              annotType = srecs[0].RannotType,
              startOffset = int(srecs[0].RstartOffset),
              endOffset = int(srecs[0].RendOffset),
              annotId = int(srecs[0].RannotId),
              properties = srecs[0].Rproperties)

    # Construct the array
    values = []
    for rec in srecs:
      if rec.LdocId != None:
        values.append(Row(docId = rec.LdocId,
                          annotSet = rec.LannotSet,
                          annotType = rec.LannotType,
                          startOffset = int(rec.LstartOffset),
                          endOffset = int(rec.LendOffset),
                          annotId = int(rec.LannotId),
                          properties = rec.Lproperties))
    return(key,values)


  # Get the preceding 'contained' annotations
  def precedingContainedAQ(rec):
    if rec.CdocId == None:
      return (rec.annot,[])
    else:
        values = []
        for entry in rec.annots:
          if (entry.startOffset >= rec.CstartOffset) and (entry.endOffset <= rec.CendOffset):
            values.append(entry)
        return (rec.annot,values)


  l = annot.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").toDF("LannotId","LannotSet","LannotType","LdocId","LendOffset","Lproperties","LstartOffset")
  r = anchor.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").toDF("RannotId","RannotSet","RannotType","RdocId","RendOffset","Rproperties","RstartOffset")


  # Group on the anchor annotation
  results = l.join(r,
                   (col("LdocId") == col("RdocId")) &
                   (col("LendOffset") <= col("RstartOffset")),
                    "rightouter") \
             .rdd \
             .groupBy(lambda x: (x["RdocId"],x["RstartOffset"],x["RendOffset"])) \
             .map(lambda rec: precedingAQ(rec,cnt))

  results = spark.createDataFrame(results.map(lambda x: x),AQSchemaList())

  if (container != None) and (not(container.rdd.isEmpty())):
    c = container.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").toDF("CannotId","CannotSet","CannotType","CdocId","CendOffset","Cproperties","CstartOffset")
    cResults = results.join(c,
                           (col("annot.docId") == col("CdocId")) &
                           (col("annot.startOffset") >= col("CstartOffset")) &
                           (col("annot.endOffset") <= col("CendOffset")),
                            "leftouter") \
                      .rdd \
                      .map(lambda rec: precedingContainedAQ(rec))


    # Need to drop duplicates
    return spark.createDataFrame(cResults.map(lambda x: x),AQSchemaList())

  else:
    return results


def Following(annot, anchor, container=None, cnt=3):
  """Provide the ability to find the following sibling annotations for every annotation in the anchor Dataframe of AQAnnotations.

    The following sibling annotations can optionally be required to be contained in a container Dataframe of AQAnnotations.
    The return type of this function is different from other functions.
    Instead of returning a Dataframe of AQAnnotations this function returns a Dataframe (AQAnnotation,Array[AQAnnotation]).

  Args:
    annot: Dataframe of AQAnnotations, the ones we will be using to look for following sibling annotations.
    anchor: Dataframe of AQAnnotations  starting point for using to look for following sibling annotations (use the endOffset and docId).
    container: Dataframe of AQAnnotations to use when requiring the following sibling annotations to be contained in a specific annotation.
    cnt: Number of preceding sibling AQAnnotations to return.

  Returns:
    Dataframe of (AQAnnotation,Array[AQAnnotation])

  """
  # Get the following annotations
  def followingAQ(rec,cnt):
    # Sort the following annotations (limit the number of results to the cnt)
    srecs = sorted(rec[1], key=lambda x: (-1 if x.LstartOffset == None else x.LstartOffset))[0:cnt]

    # We can extract the key from the any 'right' entry in sorted recs (we will use the first one)
    key = Row(docId = srecs[0].RdocId,
              annotSet = srecs[0].RannotSet,
              annotType = srecs[0].RannotType,
              startOffset = int(srecs[0].RstartOffset),
              endOffset = int(srecs[0].RendOffset),
              annotId = int(srecs[0].RannotId),
              properties = srecs[0].Rproperties)

    # Construct the array
    values = []
    for rec in srecs:
      if rec.LdocId != None:
        values.append(Row(docId = rec.LdocId,
                          annotSet = rec.LannotSet,
                          annotType = rec.LannotType,
                          startOffset = int(rec.LstartOffset),
                          endOffset = int(rec.LendOffset),
                          annotId = int(rec.LannotId),
                          properties = rec.Lproperties))

    return(key,values)


  # Get the following 'contained' annotations
  def followingContainedAQ(rec):
    if rec.CdocId == None:
      return (rec.annot,[])
    else:
        values = []
        for entry in rec.annots:
          if (entry.startOffset >= rec.CstartOffset) and (entry.endOffset <= rec.CendOffset):
            values.append(entry)
        return (rec.annot,values)

  l = annot.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").toDF("LannotId","LannotSet","LannotType","LdocId","LendOffset","Lproperties","LstartOffset")
  r = anchor.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").toDF("RannotId","RannotSet","RannotType","RdocId","RendOffset","Rproperties","RstartOffset")
  # Group on the anchor annotation
  results = l.join(r,
                   (col("LdocId") == col("RdocId")) &
                   (col("LstartOffset") >= col("RendOffset")),
                    "rightouter") \
             .rdd \
             .groupBy(lambda x: (x["RdocId"],x["RstartOffset"],x["RendOffset"])) \
             .map(lambda rec: followingAQ(rec,cnt))

  results = spark.createDataFrame(results.map(lambda x: x),AQSchemaList())

  if (container != None) and (not(container.rdd.isEmpty())):
    c = container.select("annotId","annotSet","annotType","docId","endOffset","properties","startOffset").toDF("CannotId","CannotSet","CannotType","CdocId","CendOffset","Cproperties","CstartOffset")
    cResults = results.join(c,
                           (col("annot.docId") == col("CdocId")) &
                           (col("annot.startOffset") >= col("CstartOffset")) &
                           (col("annot.endOffset") <= col("CendOffset")),
                            "leftouter") \
                      .rdd \
                      .map(lambda rec: followingContainedAQ(rec))

    # Need to drop duplicates
    return spark.createDataFrame(cResults.map(lambda x: x),AQSchemaList())

  else:
    return results


  
def TokensSpan(tokens, spans, tokenProperty):
  """Provides the ability to create a string from a list of tokens that are contained in a span.

    The specified tokenProperty is used to extract the values from the tokens when creating the string. 
    For SCNLP, this tokenProperty could be values like 'orig', 'lemma', or 'pos'. The spans would typically be a SCNLP 'sentence' or could even be things like an OM 'ce:para'.

  Args:
    tokens: Dataframe of AQAnnotations (which we will use to concatenate for the string)
    spans: Dataframe of AQAnnotations (identifies the start/end for the tokens to be used for the concatenated string)
    tokenProperty: The property field in the tokens to use for extracting the value for the concatenated string

  Returns:
     Dataframe[AQAnnotation] spans with 3 new properties all prefixed with the specified tokenProperty value followed by (ToksStr, ToksSpos, ToksEpos) The ToksStr property will be the 
     concatenated string of token property values contained in the span. The ToksSPos and ToksEpos are properties that will help us determine the start/end offset for each of the individual tokens in the ToksStr. 
     These helper properties are needed for the function RegexTokensSpan so we can generate accurate accurate start/end offsets based on the str file.
  """
  def process(rec):
    span = rec[0]
    tokens = rec[1]
    newProps = {}
    oldProps = span.properties
    for key in oldProps.keys():
      newProps[key] = oldProps[key]
    toksStr = []
    toksSpos = []
    toksEpos = []
    offset = 0
    for token in tokens:
      tokeStr = ""
      if (token.properties != None) and (tokenProperty in token.properties):
        tokStr = token.properties[tokenProperty]
        toksStr.append(tokStr)
        toksSpos.append((str(offset) + "|" + str(token.startOffset)))
        offset += len(tokStr)
        toksEpos.append((str(offset) + "|" + str(token.endOffset)))
        offset += 1
    newProps[tokenProperty + "ToksStr"] = " ".join(toksStr)
    newProps[tokenProperty + "ToksSpos"] =  " ".join(toksSpos)
    newProps[tokenProperty + "ToksEpos"]  = " ".join(toksEpos)

    return Row(docId = span.docId,
              annotSet = span.annotSet,
              annotType = span.annotType,
              startOffset = span.startOffset,
              endOffset = span.endOffset,
              annotId = span.annotId,
              properties = newProps)

  results = ContainedInList(tokens,spans).rdd.map(lambda rec: process(rec))
  return spark.createDataFrame(results.map(lambda x: x),AQSchema())

def RegexTokensSpan(tokensSpan, prop, regex, annotSet="",annotType="", annotProps={}):
  """Provides the ability to apply a regular expression to the concatenated string generated by TokensSpan.

    For the strings matching the regex,a Dataframe[AQAnnotations] will be returned.  
    The AQAnnotation will correspond to the offsets within the concatenated string containing the match.

  Args:
    tokensSpan: Datafrane of AQAnnotations (the annotations returned from the TokensSpan function)
    prop: the property name (orig, lemma, pos) that was used to generate the string for the span in TokensSpan
    regex: the regular expression to apply to the span
    annotSet: the value to assign to annotSet for the returned matched annotations (default will be the annotSet from the tokensSpan)
    annotType: the value to assign to annotType for the returned matched annotations (default will be the annotType from the tokensSpan)
    annotProps: the additional properties to append to the properties map for the returned matched annotations 

  Returns:
     Dataframe[AQAnnotation] for the strings matching the regex
  """
  def process(partition,prop,regex,annotSet,annotType,annotProps):
    import regex as re
    import builtins as py_builtin

    results = []
    annotId = 0
    pattern = re.compile(regex)

    for rec in partition:
      if (rec.properties != None) and (prop+"ToksStr" in rec.properties):
        span = rec.properties[prop+"ToksStr"]
        for match in re.finditer(pattern, span):
          annotId += 1
          newAnnotSet = annotSet
          newAnnotType = annotType
          if (annotSet == ""):
            newAnnotSet = rec.annotSet
          if (annotType == ""):
            newAnnotType = rec.annotType
          props = {}
          oldProps = rec.properties
          for key in annotProps.keys():
            props[key] = annotProps[key]
          # start
          startPos = -1
          startPosLB = []
          for start in oldProps[prop+"ToksSpos"].split(" "):
            startToks = start.split("|")
            if int(startToks[0]) == match.start():
              startPos = int(startToks[1])
            if int(startToks[0]) < match.start():
              startPosLB.append(int(startToks[1]))
          if startPos == -1:
            startPos = py_builtin.max(startPosLB)
          # end
          endPos = -1
          endPosLB = []
          for end in oldProps[prop+"ToksEpos"].split(" "):
            endToks = end.split("|")
            if int(endToks[0]) == match.end():
              endPos = int(endToks[1])
            if int(endToks[0]) > match.end():
              endPosLB.append(int(endToks[1]))
          if endPos == -1:
            endPos = py_builtin.min(endPosLB)
          props[prop+"Match"] = span[match.start():match.end()]
          # get the excludes from the span (but only include those contained in within the match)
          for key in oldProps.keys():
            if key == "excludes":
              excludesLB = []
              for exclude in oldProps[key].split("|"):
                arr = exclude.split(",")
                excludeStart = int(arr[3])
                excludeEnd = int(arr[4])
                if excludeStart >= startPos and excludeEnd <= endPos:
                  excludesLB.append(exclude)
              if len(excludesLB):
                props["excludes"] = "|".join(excludesLB)

          annot = Row(docId = rec.docId,
                      annotSet = newAnnotSet,
                      annotType = newAnnotType,
                      startOffset = startPos,
                      endOffset = endPos,
                      annotId = annotId,
                      properties = props)
          
          results.append(annot)

    return iter(results)

  results = tokensSpan.rdd.mapPartitions(lambda partition: process(partition,prop,regex,annotSet,annotType,annotProps))
  return spark.createDataFrame(results.map(lambda x: x),AQSchema())
