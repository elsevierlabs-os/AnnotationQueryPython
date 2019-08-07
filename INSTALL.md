## Installing AnnotationQueryPython

This document gives the instructions to install a Python distribution with all dependencies required for AnnotationQueryPython.

1. Create a Python 3.5 environment

2. Activate/use the Python environment created in Step 1.

3. Run 'make install' to install all of the requirements for AnnotationQueryPython

4. Run 'make test' to execute the test suites and confirm sucessful installation.

If the test cases fail, you may want to temporarily unset any Spark/Pyspark ENV variables you have previously set for other projects.

The above instructions work fine for when testing AnnotationQueryPython in Spark 'local' mode (which is true for the unit test cases).  However, if you are not in 'local' mode, then the following minimal steps will need to be added to the above installation instructions.

1. Install the Spark 2.4.3 distribution avaialable from [Spark](https://spark.apache.org/downloads.html)

2. Set the SPARK_HOME environment variable to the installed Spark distribution.
    ```
    export SPARK_HOME="path to Spark distribution"
    ```

### Dependencies

The following libraries are required:

  * Python 3.5+
  * pyspark==2.4.3
  * psutil
  * nose 
