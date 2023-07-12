Template for bachelor and master theses with pdflatex based on the ITS layout
Fabian Bleier (fabian.bleier@kit.edu), September 2016

Brief description:
* The main folder primarily contains the document "entire.tex". This in turn contains all the necessary user input (author, title, etc.).
* The individual chapters are loaded into the overall document and are located in the "chapters" folder.
* The folder "miscellaneous" contains all pages outside of the actual text body (formatting of the headers, cover sheets, the packages to be included, institute logos, macros, directories, etc.)
* The "literature" folder contains all files that are necessary for creating a bibliography (the bibtex file, the bst file, and the actual directory).
* All images can be stored in the "images" folder.
* The "documentation" folder contains various documentation for the packages used, as well as an expandable documentation of the LaTeX document
* IMPORTANT: Make sure all packages are up to date.
* The command line tools texdoc [package] (TeXLive) or mthelp [package] (Miktex) provide help and instructions for packages, e.g. For example, calling mthelp scrguide provides instructions on KOMA-Script