Concordancers
===========================================
The following functions have proven useful when looking at AQAnnotations. When displaying an annotation, the starting text for the annotation will begin with a green ">" and end with a green "<". If you use the XMLConcordancer that outputs the original XML (from the OM annotations), the XML tags will be in orange. The XML may not be well-formed). When generating annotations, you sometimes may want to exclude some text. In AQAnnotations, this is done with the excludes property. When an annotation is encountered that has an excludes property, the text excluded will be highlighted in red. For more details on the implementation, view the corresponding class for each function in the AQPython Utilities module. For usage examples, view the test_utilities module.

.. currentmodule:: AQPython.Concordancers

.. automodule:: AQPython.Concordancers

.. autosummary::
    :toctree: generated
    :template: function.rst

    Concordancer
    XMLConcordancer
    OrigPosLemConcordancer