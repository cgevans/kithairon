# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import re
import sys
from pathlib import Path

import sphinx_autosummary_accessors
from pkg_resources import get_distribution

sys.path.insert(0, str(Path("../..").resolve()))

extensions = [
    # Sphinx extensions
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.githubpages",
    "sphinx.ext.intersphinx",
    # "sphinx.ext.linkcode",
    "sphinx.ext.mathjax",
    # Third-party extensions
    "autodocsumm",
    "numpydoc",
    "sphinx_autosummary_accessors",
    "sphinxcontrib.autodoc_pydantic",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinx_favicon",
]

__version__ = get_distribution("kithairon").version
...  # I use __version__ to define Sphinx variables


# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "kithairon"
project_copyright = "2023, Constantine Evans"
author = "Constantine Evans"
release = __version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration


templates_path = ["_templates"]
exclude_patterns = []


# Render docstring text in `single backticks` as code.
default_role = "code"

maximum_signature_line_length = 88

# Below setting is used by
# sphinx-autosummary-accessors - build docs for namespace accessors like `Series.str`
# https://sphinx-autosummary-accessors.readthedocs.io/en/stable/
templates_path = ["_templates", sphinx_autosummary_accessors.templates_path]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["Thumbs.db", ".DS_Store"]

# sphinx.ext.intersphinx - link to other projects' documentation
# https://www.sphinx-doc.org/en/master/usage/extensions/intersphinx.html
intersphinx_mapping = {
    "numpy": ("https://numpy.org/doc/stable/", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
    "pyarrow": ("https://arrow.apache.org/docs/", None),
    "python": ("https://docs.python.org/3", None),
    "polars": ("https://pola-rs.github.io/polars/py-polars/html", None),
}

# numpydoc - parse numpy docstrings
# https://numpydoc.readthedocs.io/en/latest/
# Used in favor of sphinx.ext.napoleon for nicer render of docstring sections
numpydoc_show_class_members = False

# Sphinx-copybutton - add copy button to code blocks
# https://sphinx-copybutton.readthedocs.io/en/latest/index.html
# strip the '>>>' and '...' prompt/continuation prefixes.
copybutton_prompt_text = r">>> |\.\.\. "
copybutton_prompt_is_regexp = True


def _minify_classpaths(s: str) -> str:
    # from polars:
    # strip private polars classpaths, leaving the classname:
    # * "pl.Expr" -> "Expr"
    # * "polars.expr.expr.Expr" -> "Expr"
    # * "polars.lazyframe.frame.LazyFrame" -> "LazyFrame"
    # also:
    # * "datetime.date" => "date"
    s = s.replace("datetime.", "")
    return re.sub(
        pattern=r"""
        ~?
        (
          (?:pl|
            (?:polars\.
              (?:_reexport|datatypes)
            )
          )
          (?:\.[a-z.]+)?\.
          ([A-Z][\w.]+)
        )
        """,
        repl=r"\2",
        string=s,
        flags=re.VERBOSE,
    )


# # sphinx-ext-linkcode - Add external links to source code
# # https://www.sphinx-doc.org/en/master/usage/extensions/linkcode.html
# def linkcode_resolve(domain: str, info: dict[str, Any]) -> str | None:
#     """
#     Determine the URL corresponding to Python object.

#     Based on pandas equivalent:
#     https://github.com/pandas-dev/pandas/blob/main/doc/source/conf.py#L629-L686
#     """
#     if domain != "py":
#         return None

#     modname = info["module"]
#     fullname = info["fullname"]

#     submod = sys.modules.get(modname)
#     if submod is None:
#         return None

#     obj = submod
#     for part in fullname.split("."):
#         try:
#             with warnings.catch_warnings():
#                 # Accessing deprecated objects will generate noisy warnings
#                 warnings.simplefilter("ignore", FutureWarning)
#                 obj = getattr(obj, part)
#         except AttributeError:
#             return None

#     try:
#         fn = inspect.getsourcefile(inspect.unwrap(obj))
#     except TypeError:
#         try:  # property
#             fn = inspect.getsourcefile(inspect.unwrap(obj.fget))
#         except (AttributeError, TypeError):
#             fn = None
#     if not fn:
#         return None

#     try:
#         source, lineno = inspect.getsourcelines(obj)
#     except TypeError:
#         try:  # property
#             source, lineno = inspect.getsourcelines(obj.fget)
#         except (AttributeError, TypeError):
#             lineno = None
#     except OSError:
#         lineno = None

#     linespec = f"#L{lineno}-L{lineno + len(source) - 1}" if lineno else ""

#     conf_dir_path = Path(__file__).absolute().parent
#     polars_root = (conf_dir_path.parent.parent / "polars").absolute()

#     fn = os.path.relpath(fn, start=polars_root)
#     return f"{github_root}/blob/{git_ref}/py-polars/polars/{fn}{linespec}"


def process_signature(app, what, name, obj, opts, sig, ret):  # noqa: PLR0913
    return (
        _minify_classpaths(sig) if sig else sig,
        _minify_classpaths(ret) if ret else ret,
    )


def setup(app):
    # TODO: a handful of methods do not seem to trigger the event for
    #  some reason (possibly @overloads?) - investigate further...
    app.connect("autodoc-process-signature", process_signature)


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_static_path = ["_static"]
