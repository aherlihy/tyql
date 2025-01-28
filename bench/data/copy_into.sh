#!/bin/bash

 # Set the outer directory
 datadir=$(pwd)

 # List of target directories within datadir
 m_directories=("asps" "bom" "cc" "cspa" "orbits" "party" "trustchain")
 s_directories=("andersens" "cba" "dataflow" "javapointsto" "pointstocount")
 l_directories=("sssp" "tc" "ancestry")
 asc=("evenodd")

 bash ./generate_all.sh 1 MB
 bash ./generate_all.sh 10 MB

 mkdir -p "$datadir/$asc/m_data"
 cd "$datadir/$asc/m_data"
 python3 ../../generate_numbers.py
 cd "$datadir"

 for dir in "${m_directories[@]}"; do
   source_dir="$datadir/$dir/data_1MB"
   target_dir="$datadir/$dir/m_data"

   if [ -d "$source_dir" ]; then
     mkdir -p "$target_dir"

     cp -r "$source_dir"/* "$target_dir/"
     echo "Copied contents from $source_dir to $target_dir"
   else
     echo "Source directory $source_dir does not exist. Skipping."
   fi
 done

 for dir in "${s_directories[@]}"; do
   source_dir="$datadir/$dir/data_1MB"
   target_dir="$datadir/$dir/m_data"

   if [ -d "$source_dir" ]; then
     mkdir -p "$target_dir"

     # Set linecount based on the value of dir
     if [[ "$dir" == *"andersen"* ]]; then
         linecount=1000
     elif [[ "$dir" == *"dataflow"* ]]; then
         linecount=2000
     elif [[ "$dir" == *"java"* || "$dir" == *"count"* ]]; then
         linecount=1500
     elif [[ "$dir" == *"cba"* ]]; then
         linecount=1000
     else
         echo "Directory not medium sized?"
         exit 1
     fi

     for file in "$source_dir"/*; do
         if [[ -f "$file" ]]; then
             filename=$(basename "$file")
             head -n "$linecount" "$file" > "$target_dir/$filename"
         fi
     done
     echo "Copied contents from $source_dir to $target_dir with $linecount lines"
   else
     echo "Source directory $source_dir does not exist. Skipping."
   fi
 done

 for dir in "${l_directories[@]}"; do
   source_dir="$datadir/$dir/data_10MB"
   target_dir="$datadir/$dir/m_data"

   if [ -d "$source_dir" ]; then
     mkdir -p "$target_dir"

     cp -r "$source_dir"/* "$target_dir/"
     echo "Copied contents from $source_dir to $target_dir"
   else
     echo "Source directory $source_dir does not exist. Skipping."
   fi
 done

echo "copying XL data"
 for dir in "${l_directories[@]}"; do

   if [[ "$dir" == *"ancestry"* ]]; then
       target_size=100
       mkdir -p "$datadir/$dir/data_${target_size}MB"
       cd "$datadir/$dir/data_${target_size}MB"
       python3 ../../generate.py --size "$target_size" --units MB --columns "(parent, int) (child, int)" --filename parents.csv   --acyclic
       cd "$datadir"
   elif [[ "$dir" == *"sssp"* ]]; then
       target_size=25
       mkdir -p "$datadir/$dir/data_${target_size}MB"
       cd "$datadir/$dir/data_${target_size}MB"
       python3 ../../generate.py --size "$target_size" --units MB --columns "(src, int) (dst, int) (cost, int)" --filename edge.csv --acyclic
       cd "$datadir"
   elif [[ "$dir" == *"tc" ]]; then
       target_size=5
       mkdir -p "$datadir/$dir/data_${target_size}MB"
       cd "$datadir/$dir/data_${target_size}MB"
       python3 ../../generate.py --size "$target_size" --units MB --columns "(x, int) (y, int)" --filename edge.csv  --base1
       cd "$datadir"
   else
       echo "Directory not medium sized?"
       exit 1
   fi

    source_dir="$datadir/$dir/data_${target_size}MB"
    target_dir="$datadir/$dir/l_data"

    if [ -d "$source_dir" ]; then
      mkdir -p "$target_dir"

      cp -r "$source_dir"/* "$target_dir/"
      echo "Copied contents from $source_dir to $target_dir"
    else
      echo "Source directory $source_dir does not exist. Skipping."
    fi
  done