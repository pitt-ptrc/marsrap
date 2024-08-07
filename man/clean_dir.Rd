% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/read.R
\name{clean_dir}
\alias{clean_dir}
\title{Clean Directory and Output Data in Specified Format}
\usage{
clean_dir(dir, outdir = "data", format = "arrow", .keep = FALSE)
}
\arguments{
\item{dir}{The input directory containing the files to be processed.}

\item{outdir}{The output directory where the processed files will be saved. Defaults to "data".}

\item{format}{The format to save the processed data in. Possible values are "csv" and "arrow".}

\item{.keep}{removes files in directory prepped}
}
\value{
Invisible NULL. This function is used for its side effects of reading, processing,
and saving data.
}
\description{
This function reads all files from a directory, processes them, and outputs the data
in a specified format (CSV or Apache Arrow).
}
