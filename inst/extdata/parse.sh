#!/usr/bin/env bash

# Function to insert a custom delimiter after the first two whitespaces
insert_delimiter() {
  local input_file="$1"
  local temp_file="$(mktemp)"

  while IFS= read -r line; do
    # This uses a more precise approach to replace only the first two whitespaces
    modified_line=$(echo "$line" | sed -E 's/([[:space:]]+[^[:space:]]+[[:space:]]+)/|\1|/')
    echo "$modified_line" >> "$temp_file"
  done < "$input_file"

  mv "$temp_file" "$input_file"
}

# Set up the directories
raw_dir_culture="raw/lc"
raw_dir_blood="raw/lb"
prepped_dir="prepped"
mkdir -p "$prepped_dir"

echo "Preparing files..."
# Loop through each file in the raw/culture directory
for file in "$raw_dir_culture"/*; do
  # Apply grep commands to the file and save the results to separate files
  # Split pull*.out files into components. Needs raw and prepped dirs
  grep -E '^\d.*' "$file" > "$prepped_dir/head_$(basename "$file").txt"
  grep -E '^ACC.*' "$file" > "$prepped_dir/acc_$(basename "$file").txt"
  grep -E '^BAT.*' "$file" > "$prepped_dir/bat_$(basename "$file").txt"
  grep -E '^ACC|BAT|DAT|TXT.*' "$file" > "$prepped_dir/body_$(basename "$file").txt"

  # Call insert_delimiter for the body file
  insert_delimiter "$prepped_dir/body_$(basename "$file").txt"
done

# Loop through each file in the raw/blood directory
for file in "$raw_dir_blood"/*; do
  # Apply grep commands to the file and save the results to separate files
  grep -E '^\d.*' "$file" > "$prepped_dir/head_$(basename "$file").txt"
  grep -E '^ACC.*' "$file" > "$prepped_dir/acc_$(basename "$file").txt"
  grep -E '^BAT.*' "$file" > "$prepped_dir/bat_$(basename "$file").txt"
  grep -E '^ACC|BAT|DAT|TXT.*' "$file" > "$prepped_dir/body_$(basename "$file").txt"

  # Call insert_delimiter for the body file
  insert_delimiter "$prepped_dir/body_$(basename "$file").txt"
done
echo "Done"
