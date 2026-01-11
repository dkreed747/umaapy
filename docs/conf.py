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
try:
    release = _get_pkg_version("umaapy")
    version = _get_pkg_version("umaapy")
except Exception:
    # Fallback during CI when package isn't installed yet
    release = version = "0.0.0"
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
    "sphinxcontrib.mermaid",
]
autosummary_generate = True  # turn on autosummary

autodoc_mock_imports = ["cyclonedds", "umaapy.umaa_types"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

language = "en"
source_suffix = ".rst"
html_favicon = "_static/favicon.png"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_theme_options = {
    "collapse_navigation": False,  # leave all sections expanded
    "sticky_navigation": True,  # keep the nav on-screen as you scroll
    "navigation_depth": 4,  # show 4 levels deep
    "version_selector": True,
}
html_static_path = ["_static"]

html_sidebars = {
    # apply to _all_ pages
    "**": [
        "globaltoc.html",  # the full toctree
        "relations.html",  # “Prev / Next” links
        "searchbox.html",
    ]
}

mermaid_env_cmd = os.getenv("MERMAID_CMD")
if mermaid_env_cmd:
    mermaid_cmd = [mermaid_env_cmd]

latex_elements = {
    "preamble": r"""
\usepackage[utf8]{inputenc}
\usepackage{amssymb}
\usepackage{amssymb}
\usepackage{pifont}
\DeclareUnicodeCharacter{2705}{\checkmark}
\DeclareUnicodeCharacter{274C}{\ding{55}}
\DeclareUnicodeCharacter{1F4A1}{\ding{92}}
\DeclareUnicodeCharacter{2610}{$\square$}
"""
}
