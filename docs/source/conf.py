import os
import sys

# Configuration file for the Sphinx documentation builder.
#

# -- Project information -----------------------------------------------------

project = "Arroyo"
copyright = "2022, Sentry Team and Contributors"
author = "Sentry Team and Contributors"

sys.path.insert(0, os.path.abspath("../.."))

# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.githubpages",
    "sphinx.ext.intersphinx",
    "sphinxcontrib.mermaid",
    "sphinx.ext.autodoc",
    "sphinx_autodoc_typehints",
]

always_document_param_types = True

# This is relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ["build"]

pygments_style = "sphinx"

source_suffix = ".rst"

# -- Options for HTML output -------------------------------------------------

html_theme = "sphinx_rtd_theme"

html_static_path = ["_static"]

# html_logo = "_static/arroyo.png"
