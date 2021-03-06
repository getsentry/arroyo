# Configuration file for the Sphinx documentation builder.
#

# -- Project information -----------------------------------------------------

project = "Arroyo"
copyright = "2022, Sentry Team and Contributors"
author = "Sentry Team and Contributors"

release = "0.0.20"


# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.githubpages",
    "sphinx.ext.intersphinx",
]

# This is relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ["build"]

source_suffix = ".rst"

# -- Options for HTML output -------------------------------------------------

html_theme = "alabaster"

html_static_path = ["_static"]

html_logo = "_static/arroyo.png"
