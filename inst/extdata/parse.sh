#!/usr/bin/env bash

# Awk function to handle NODATA and insert delimiters
process_file() {
  local input_file="$1"
  local temp_file="$input_file.tmp"
  awk '
  BEGIN {
      FS = "[ ]+"
      OFS = " "
  }

  # Function to print an error message and exit
  function error_out() {
      print "Error: NODATA found in unexpected location, cannot parse. See end of file." > "/dev/stderr"
      exit 1
  }

  {
      if ($1 == "ACC:") {
          for (i = 1; i <= NF; i++) {
              if ($i == "NODATA") {
                  if (i == 3) {  # acc_date and acc_time slot
                      $i = "NA NA"
                  } else if (i == 5) {  # acc_id slot
                      $i = "NA"
                  } else {
                      error_out()
                  }
              }
          }
          # Print the processed line
          print $1, $2, $3, $4, $5, $6, $7
      } else {
          for (i = 1; i <= NF; i++) {
              if ($i == "NODATA") {
                  error_out()
              }
          }
          # Print the original line if no NODATA found
          print $0
      }
  }' "$input_file" > "$temp_file"
}

# Function to insert a custom delimiter after the first two whitespaces
insert_delimiter() {
  local input_file="$1"
  awk '{
    match($0, /[[:space:]]+[^[:space:]]+[[:space:]]+/)
    prefix = substr($0, 1, RSTART-1)
    middle = substr($0, RSTART, RLENGTH)
    suffix = substr($0, RSTART + RLENGTH)
    print prefix "|" middle "|" suffix
  }' "$input_file" > "$input_file.tmp" && mv "$input_file.tmp" "$input_file"
}

# Set up the directories
raw_dir_culture="raw/lc"
raw_dir_blood="raw/lb"
prepped_dir="prepped"
mkdir -p "$prepped_dir"

echo "Preparing files..."
# Loop through each file in the raw/culture directory
for file in "$raw_dir_culture"/*; do
  # Process the raw file with awk and create a temporary file
  process_file "$file"

  # Apply grep commands to the temporary file and save the results to separate files
  # Split pull*.out files into components. Needs raw and prepped dirs
  grep -E '^\d.*' "$file.tmp" > "$prepped_dir/head_$(basename "$file").txt"
  grep -E '^ACC.*' "$file.tmp" > "$prepped_dir/acc_$(basename "$file").txt"
  grep -E '^BAT.*' "$file.tmp" > "$prepped_dir/bat_$(basename "$file").txt"
  grep -E '^ACC|BAT|DAT|TXT.*' "$file.tmp" > "$prepped_dir/body_$(basename "$file").txt"

  # Call insert_delimiter for the body file
  insert_delimiter "$prepped_dir/body_$(basename "$file").txt"

  # Clean up temporary file
  rm "$file.tmp"
done

# Loop through each file in the raw/blood directory
for file in "$raw_dir_blood"/*; do
  # Process the raw file with awk and create a temporary file
  process_file "$file"

  # Apply grep commands to the temporary file and save the results to separate files
  grep -E '^\d.*' "$file.tmp" > "$prepped_dir/head_$(basename "$file").txt"
  grep -E '^ACC.*' "$file.tmp" > "$prepped_dir/acc_$(basename "$file").txt"
  grep -E '^BAT.*' "$file.tmp" > "$prepped_dir/bat_$(basename "$file").txt"
  grep -E '^ACC|BAT|DAT|TXT.*' "$file.tmp" > "$prepped_dir/body_$(basename "$file").txt"

  # Call insert_delimiter for the body file
  insert_delimiter "$prepped_dir/body_$(basename "$file").txt"

  # Clean up temporary file
  rm "$file.tmp"
done
echo "Done"
