# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information


import os, sys
from importlib.metadata import version as _get_pkg_version

sys.path.insert(0, os.path.abspath("../src"))

project = "UMAAPy"
release = _get_pkg_version("umaapy")
copyright = "2025, Devon Reed"
author = "Devon Reed"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",  # generate docs from docstrings
    "sphinx.ext.autosummary",  # create summary .rst files
    "sphinx.ext.napoleon",  # Google/NumPy style docstrings
    "sphinx_autodoc_typehints",  # inline type hints
    "sphinx.ext.viewcode",  # link to highlighted source
    "autodocsumm",
]
autosummary_generate = True  # turn on autosummary

autodoc_mock_imports = [
    "rti.connextdds",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

language = "en"
source_suffix = ".rst"
html_favicon = "_static/favicon.png"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
