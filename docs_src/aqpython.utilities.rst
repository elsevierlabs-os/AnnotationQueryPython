Utilities
===========================================

The GetAQAnnotation and GetCATAnnotation and utility classes have been developed to create an AQAnnotation from the archive format (CATAnnotation) and vise versa. When creating the AQAnnotation, the ampersand separated string of name-value pairs in the CATAnnotation other field is mapped to a Map in the AQAnnotation record. To minimize memory consumption and increase performance, you can specify which name-value pairs to include in the Map. For more details on the implementation, view the corresponding class for each function in the AQPython Utilities module. For usage examples, view the GetAQAnnotation and GetCATAnnotation classes in the test_utilities module.


.. currentmodule:: AQPython.Utilities

.. automodule:: AQPython.Utilities

.. autosummary::
    :toctree: generated
    :template: function.rst

    GetAQAnnotations
    GetCATAnnotations
    Hydrate
