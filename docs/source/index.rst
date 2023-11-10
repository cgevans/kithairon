.. currentmodule:: kithairon

-------------------------------
Kithairon and processed classes
-------------------------------

.. autosummary::
   :toctree: api/
   :nosignatures:
   :recursive:
   :template: custom-class-template.rst

   surveys.SurveyData
   picklists.PickList
   labware.Labware
   labware.PlateInfo

-------
Formats
-------

.. toctree::
   :maxdepth: 2

   picklists

----------------
Raw Echo classes
----------------

These classes represent files/data directly from the Echo and
related software, with some minor additions and modifications for echo_lib.

.. toctree::
   :maxdepth: 1

   echo-formats/echo-platesurvey
   echo-formats/echo-surveyreport
   echo-formats/echo-labware
