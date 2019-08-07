from pyspark.sql.functions import *
import builtins
import io
from urllib.parse import unquote_plus

def _buildTableEntry(docId, annotSet, annotType, beforeText, text, afterText):
  return ("<tr><td>" + docId + "</td>" + 
          "<td>" + annotSet  +"</td>" + 
          "<td>" + annotType + "</td><td><div style='word-break:break-all;'>" + 
          beforeText + 
          "<font color='green'> &gt; </font>" + 
          text + 
          "<font color='green'> &lt; </font>"  + 
          afterText + 
          "</div></td></tr>")

def _buildEmptyTableEntry(docId, annotSet, annotType):
  return ("<tr><td>" + docId + "</td>" + 
          "<td>" + annotSet  +"</td>" + 
          "<td>" + annotType + "</td><td><div style='word-break:break-all;'></div></td></tr>")

def _buildHTML(text):
    return "<html><body><table border='1' style='font-family: monospace;table-layout: fixed;'>" + text + "</table></body></html>"   


def Concordancer(results, textMnt, nrows=10, offset=0, highlightAnnotations=None):
  """Output HTML for the text identified by the AQAnnotation and highlight in 'red' the text that was ignored (excluded).

  Args:
    results: Dataframe of AQAnnotations that you would like to display.  
    textPath: Path for the str files.  The annotations in results must be for documents contained in these str files.
    nrows: Number of results to display
    offset: Number of characters before/after each annotation in results to display
    highlightAnnotations: Dataframe of AQAnnotations that you would like to highlight in the results

  Returns:
    HTML

  """

  limResults = results.sort("docId","startOffset").limit(nrows)
  
  # Get the Annotations to be highlighted (there may not be any)
  highlightTokens = None
  if highlightAnnotations != None:
    highlightTokens = limResults.alias("L").join(highlightAnnotations.alias("R"), 
                                              ((col("L.docId") == col("R.docId")) & 
                                               (col("L.startOffset") <= col("R.startOffset")) & 
                                               (col("L.endOffset") >= col("R.endOffset")))) \
                                           .select("R.*") \
                                           .sort("startOffset","annotType") \
                                           .collect()
  
  # Get the entries for our table (one for each annotation)
  entries = []
  for annot in limResults.take(nrows):
    try:
      # Get the document
      docText = ""
      with io.open(textMnt + annot.docId,'r',encoding='utf-8') as f:
        docText = f.read()

      # Get the text for annotation (account for the offset)
      origText = docText[annot.startOffset:annot.endOffset]   
      
      # Process the highlightTokens
      hlToks = []
      if highlightTokens != None:
        hlTokens = [ hlToken for hlToken in highlightTokens if hlToken.docId == annot.docId and hlToken.startOffset >= annot.startOffset and hlToken.endOffset <= annot.endOffset ]
        for hlToken in hlTokens:
          hlToks.append((hlToken.startOffset,hlToken.startOffset,"hl", "<font color='blue'>"))
          hlToks.append((hlToken.endOffset,hlToken.endOffset,"hl", "</font>")) 
      
      # Process the excludeTokens
      exToks = []
      if (annot.properties != None) and ('excludes' in annot.properties) and (len(annot.properties['excludes']) > 0):
        excludes = []
        for excludesEntry in annot.properties['excludes'].split("|"):
          toks = excludesEntry.split(",")  
          excludes.append((int(toks[0]),toks[1],toks[2],int(toks[3]),int(toks[4])))
        excludes = list(set(excludes))
        for exclude in excludes:
          exToks.append((exclude[3],exclude[4],"xcl",""))    
      
      # Combine Highlight Markup and Exclude annotations. Sort by startOffset, endOffset, type
      allToks = hlToks + exToks
      allToks.sort(key=lambda tup: (tup[0], tup[1], tup[2]))
      
      # Start stepping through the tags.  Add them to the buffer and substring from text.
      modText = ""
      if len(allToks) == 0:
        modText = origText
      else:
        txt = ""
        curOffset = annot.startOffset
        for tok in allToks:
          if tok[0] <= curOffset:
            if tok[2] == "hl":
              modText += tok[3]
            else:
              modText += " <font color='red'>" + \
                         origText[tok[0] - annot.startOffset:tok[1] - annot.startOffset] + \
                         "</font> "
              curOffset = tok[1]
          else:
            modText +=origText[curOffset - annot.startOffset:tok[0] - annot.startOffset]
            if tok[2] == "hl":
              modText += tok[3]
            else:
              modText += "<font color='red'>" + \
                         origText[tok[0] - annot.startOffset:tok[1] - annot.startOffset] + \
                         "</font>"                             

            curOffset = tok[1]

        if curOffset < annot.endOffset:
          modText += origText[curOffset - annot.startOffset:]
      
      entries.append(_buildTableEntry(annot.docId, 
                                      annot.annotSet,
                                      annot.annotType, 
                                      docText[builtins.max([0,annot.startOffset - offset]):annot.startOffset],
                                      modText,
                                      docText[annot.endOffset:builtins.min([len(docText),annot.endOffset + offset])]))

    except Exception as ex:
      print(ex)
      entries.append(_buildEmptyTableEntry(annot.docId,
                                           annot.annotSet,
                                           annot.annotType))

  tmpStr = "\n".join(entries)
  return _buildHTML(tmpStr)


def XMLConcordancer(results, textMnt, om, nrows=10, offset=0, highlightAnnotations=None, displayAttrs=True):
  """Output HTML for the text identified by the AQannotation and highlight in 'red' the text that was ignored (excluded).  

    Also add the XML tags (in 'orange') that would have occurred in this string.  
    Note, there are no guarantees that the XML will be well-formed.
  
  Args:
    results: Dataframe of AQAnnotations that you would like to display.  
    textPath: Path for the str files.  The annotations in results must be for documents contained in these str files.
    om: Dataframe of AQAnnotations of the Original Markup annotations.  The annotations should be for the same documents contained in the results.
    nrows: Number of results to display
    offset: Number of characters before/after each annotation in results to display
    highlightAnnotations: Dataframe of AQAnnotations that you would like to highlight in the results

  Returns:
    HTML

  """  
  limResults = results.sort("docId","startOffset").limit(nrows)
  
  # Get the Original Markup tokens for the number of results to display
  xmlTokens = limResults.alias("L").join(om.alias("R"),
                                      ((col("L.docId") == col("R.docId")) & 
                                       (col("L.startOffset") <= col("R.startOffset")) & 
                                       (col("L.endOffset") >= col("R.endOffset")))) \
                                .select("R.*") \
                                .dropDuplicates(["docId","annotSet","annotType","annotId","startOffset","endOffset"]) \
                                .sort("startOffset","annotType") \
                                .collect()
              
  # Get the Annotations to be highlighted (there may not be any)
  highlightTokens = None
  if highlightAnnotations != None:
    highlightTokens = limResults.alias("L").join(highlightAnnotations.alias("R"), 
                                              ((col("L.docId") == col("R.docId")) & 
                                               (col("L.startOffset") <= col("R.startOffset")) & 
                                               (col("L.endOffset") >= col("R.endOffset")))) \
                                           .select("R.*") \
                                           .sort("startOffset","annotType") \
                                           .collect()
  
  # Get the entries for our table (one for each annotation)
  entries = []
  for annot in limResults.take(nrows):
    try:  
      # Get the document
      docText = ""
      with io.open(textMnt + annot.docId,'r',encoding='utf-8') as f:
        docText = f.read()

      # Get the text for annotation (account for the offset)
      origText = docText[annot.startOffset:annot.endOffset]        
                                                   
      # Process the xmlTokens
      omToks = []
      xmlToks = [ xmlToken for xmlToken in xmlTokens if xmlToken.docId == annot.docId and xmlToken.startOffset >= annot.startOffset and xmlToken.endOffset <= annot.endOffset ]
      for xmlTok in xmlToks:
        # Process the attributes
        attrs = ""
        if (displayAttrs and xmlTok.properties != None) and ('attr' in xmlTok.properties) and (len(xmlTok.properties['attr']) > 0):
          for attrEntry in xmlTok.properties['attr'].split("&"):
            attrNameValue = attrEntry.split("=")
            attrs += " " + attrNameValue[0] + "='" +  unquote_plus(attrNameValue[1]) + "'"
        # Check if begin/end are the same and add only one entry
        if (xmlTok.startOffset == xmlTok.endOffset):
          omToks.append((xmlTok.startOffset, xmlTok.startOffset, "om", "<font color='orange'>&lt;" + xmlTok.annotType +  attrs + "/&gt;</font>"))
        else:
          omToks.append((xmlTok.startOffset, xmlTok.startOffset, "om", "<font color='orange'>&lt;" + xmlTok.annotType +  attrs + "&gt;</font>"))
          omToks.append((xmlTok.endOffset, xmlTok.endOffset, "om", "<font color='orange'>&lt;/" + xmlTok.annotType + "&gt;</font>"))      

      # Process the highlightTokens
      hlToks = []
      if highlightTokens != None:
        hlTokens = [ hlToken for hlToken in highlightTokens if hlToken.docId == annot.docId and hlToken.startOffset >= annot.startOffset and hlToken.endOffset <= annot.endOffset ]
        for hlToken in hlTokens:
          hlToks.append((hlToken.startOffset,hlToken.startOffset,"hl", "<font color='blue'>"))
          hlToks.append((hlToken.endOffset,hlToken.endOffset,"hl", "</font>")) 
          
      # Process the excludeTokens
      exToks = []
      if (annot.properties != None) and ('excludes' in annot.properties) and (len(annot.properties['excludes']) > 0):
        excludes = []
        for excludesEntry in annot.properties['excludes'].split("|"):
          toks = excludesEntry.split(",")  
          excludes.append((int(toks[0]),toks[1],toks[2],int(toks[3]),int(toks[4])))
        excludes = list(set(excludes))
        for exclude in excludes:
          exToks.append((exclude[3],exclude[4],"xcl",""))   
      
      # Combine Highlight Markup and Exclude annotations. Sort by startOffset, endOffset, type
      allToks = omToks + hlToks + exToks
      allToks.sort(key=lambda tup: (tup[0], tup[1], tup[2]))

      # Start stepping through the tags.  Add them to the buffer and substring from text.
      modText = ""
      if len(allToks) == 0:
        modText = origText
      else:
        txt = ""
        curOffset = annot.startOffset
        for tok in allToks:
          if tok[0] <= curOffset:
            if tok[2] == "om":
              modText += tok[3]  
            elif tok[2] == "hl":
              modText += tok[3]
            else:
              modText += " <font color='red'>" + \
                         origText[tok[0] - annot.startOffset:tok[1] - annot.startOffset] + \
                         "</font> "
              curOffset = tok[1]
          else:
            modText +=origText[curOffset - annot.startOffset:tok[0] - annot.startOffset]
            if tok[2] == "om":
              modText += tok[3]
            elif tok[2] == "hl":
              modText += tok[3]
            else:
              modText += "<font color='red'>" + \
                         origText[tok[0] - annot.startOffset:tok[1] - annot.startOffset] + \
                         "</font>"                             

            curOffset = tok[1]

        if curOffset < annot.endOffset:
          modText += origText[curOffset - annot.startOffset:]
      
      entries.append(_buildTableEntry(annot.docId,
                                      annot.annotSet,
                                      annot.annotType,
                                      docText[builtins.max([0,annot.startOffset - offset]):annot.startOffset],
                                      modText,
                                      docText[annot.endOffset:builtins.min([len(docText),annot.endOffset + offset])]))

    except Exception as ex:
      print(ex)
      entries.append(_buildEmptyTableEntry(annot.docId,
                                           annot.annotSet,
                                           annot.annotType))

  tmpStr = "\n".join(entries)
  return _buildHTML(tmpStr)


def OrigPosLemConcordancer(sentences, annots, textMnt, wordType="word", nrows=10):
  """Output HTML for the text (including lemma and pos tags) identified by the AQAnnotation (typically a sentence annotation). 
 
    Below the sentence (in successive rows) output the original terms, parts of speech, and lemma terms for the text identified by the AQAnnotation.

  Args:
    sentences: Sentence annotations that you would like to display.  
    annots: The Dataframe of AQAnnotations that will contain the the AQAnnotations (orig, lemma, pos) for the above sentences
    textPath: Path for the str files.  The sentence annotations must be for documents contained in these str files.
    wordType: The annotType that identies the AQAnnotation in the above annotations.
    nrows: Number of sentences to display

  Returns:
    HTML

  """  

  def _buildOrigPosLemmaRow(entryType, entry):
    return ("<tr>" +
            "<td>" + entryType + "</td>" +
            "<td bgcolor='grey'/>" +
            "<td bgcolor='grey'/>" +
            entry +
            "</tr>")

  sentenceAnnots = sentences.sort("docId","startOffset").limit(nrows).collect()
  tmpStr = ""
  docId = ""
  docText = ""
  text= ""
  lastDoc = ""
  curDoc = ""
  
  # Get the TextAnnotations (for the specified annotType) for each sentence
  for sentence in sentenceAnnots:
    textAnnots = annots.filter((col("docId") == sentence.docId) & 
                               (col("annotType") == wordType) & 
                               (col("startOffset") >= sentence.startOffset) & 
                               (col("endOffset") <= sentence.endOffset)) \
                       .sort("startOffset") \
                       .collect()

    # Get the raw text for the sentence annotation
    if docId != sentence.docId:
      docid = sentence.docId
      try:
        with io.open(textMnt + sentence.docId,'r',encoding='utf-8') as f:
          docText = f.read()
      except Exception as ex:
          print(ex)
          docText = ""
    if docText != "":
      text = docText[sentence.startOffset:sentence.endOffset]
    else:
      text = ""
    tmpStr += "<table border='1' style='font-family: monospace;table-layout: fixed;'><tr>"
    tmpStr += ("<td>" + sentence.docId + "</td>")
    tmpStr += ("<td>" + str(sentence.startOffset) + "</td>")
    tmpStr += ("<td>" + str(sentence.endOffset) + "</td>")
    tmpStr += ("<td colspan='" + str(len(textAnnots)) + "'>" + text + "</td>")
    tmpStr += "</tr>"
    
    # Get original row
    origEntry = ""
    for annot in textAnnots:
      if (annot.properties != None) and ('orig' in annot.properties) and (len(annot.properties['orig']) > 0):
        origEntry += ("<td>" + unquote_plus(annot.properties['orig']) + "</td>")
      else:
        origEntry += ("<td> </td>")
    tmpStr += _buildOrigPosLemmaRow('orig',origEntry)
    
    # Get pos row
    posEntry = ""
    for annot in textAnnots:
      if (annot.properties != None) and ('pos' in annot.properties) and (len(annot.properties['pos']) > 0):
        posEntry += ("<td>" + unquote_plus(annot.properties['pos']) + "</td>")
      else:
        posEntry += ("<td> </td>")
    tmpStr += _buildOrigPosLemmaRow('pos',posEntry)

    # Get lemma row
    lemmaEntry = ""
    for annot in textAnnots:
      if (annot.properties != None) and ('lemma' in annot.properties) and (len(annot.properties['lemma']) > 0):
        lemmaEntry += ("<td>" + unquote_plus(annot.properties['lemma']) + "</td>")
      else:
        lemmaEntry += ("<td> </td>")
    tmpStr += _buildOrigPosLemmaRow('lemma',lemmaEntry)
    
    tmpStr += "</table><p/><p/><p/>"

  return "<html><body>" + tmpStr + "</body></html>"