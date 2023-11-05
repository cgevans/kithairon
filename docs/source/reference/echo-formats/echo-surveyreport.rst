==========================================================================
Echo SurveyReport (Cherry Pick and Plate Reformat, possibly others) format
==========================================================================

.. currentmodule:: kithairon.surveys

.. contents:: :local:

---------------------------
EchoSurveyReport: base file
---------------------------

.. autopydantic_model:: EchoSurveyReport

------------------------------------
EchoReportHeader: survey information
------------------------------------

.. autopydantic_model:: EchoReportHeader

------------------------------------------------
EchoReportBody and EchoReportRecord: survey data
------------------------------------------------

.. autopydantic_model:: EchoReportBody

.. autopydantic_model:: EchoReportRecord

-------------------------------------
EchoReportFooter: machine information
-------------------------------------

.. autopydantic_model:: EchoReportFooter
