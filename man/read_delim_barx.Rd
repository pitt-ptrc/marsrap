% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/read.R
\name{read_delim_barx}
\alias{read_delim_barx}
\title{Read Delimited Text File with Bars as Delimiters}
\usage{
read_delim_barx(out.txt, col_names)
}
\arguments{
\item{out.txt}{The path to the input text file.}

\item{col_names}{A character vector of column names to set for the output data frame.}
}
\value{
A tibble with columns named according to \code{col_names}.
}
\description{
This function reads a delimited text file using a specified delimiter (bar "|"),
selects non-empty columns, and renames them.
}
