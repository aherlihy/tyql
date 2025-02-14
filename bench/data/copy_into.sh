#!/bin/bash

 # Set the outer directory
 datadir=$(pwd)

 # List of target directories within datadir
 misc_directories=("asps" "bom" "cc" "cspa" "orbits" "party" "trustchain")
 programanalysis_directories=("andersens" "cba" "dataflow" "javapointsto" "pointstocount")
 graph_directories=("sssp" "tc" "ancestry")
 asc=("evenodd")

 bash ./generate_all.sh 1 MB
 bash ./generate_all.sh 10 MB

echo "copying M and L data"

 mkdir -p "$datadir/$asc/m_data"
 cd "$datadir/$asc/m_data"
 python3 ../../generate_numbers.py 1
 mkdir -p "$datadir/$asc/l_data"
 cd "$datadir/$asc/l_data"
 python3 ../../generate_numbers.py 2

 cd "$datadir"

 for dir in "${misc_directories[@]}"; do
   m_source_dir="$datadir/$dir/data_1MB"
   m_target_dir="$datadir/$dir/m_data"
   l_source_dir="$datadir/$dir/data_10MB"
   l_target_dir="$datadir/$dir/l_data"

   if [ -d "$m_source_dir" ]; then
     mkdir -p "$m_target_dir"
     cp -r "$m_source_dir"/* "$m_target_dir/"
     echo "Copied contents from $m_source_dir to $m_target_dir"
   else
     echo "Source directory $m_source_dir does not exist. Skipping."
   fi
   if [ -d "$l_source_dir" ]; then
     mkdir -p "$l_target_dir"
     if [[ "$dir" == "asps" ]]; then
         linecount=250000
         for file in "$l_source_dir"/*; do
             if [[ -f "$file" ]]; then
                 filename=$(basename "$file")
                 head -n "$linecount" "$file" > "$l_target_dir/$filename"
             fi
         done
     elif [[ "$dir" == "cc" ]]; then
         linecount=400000
         for file in "$l_source_dir"/*; do
             if [[ -f "$file" ]]; then
                 filename=$(basename "$file")
                 head -n "$linecount" "$file" > "$l_target_dir/$filename"
             fi
         done
     elif [[ "$dir" == "cspa" ]]; then
         linecount=400000
         for file in "$l_source_dir"/*; do
             if [[ -f "$file" ]]; then
                 filename=$(basename "$file")
                 head -n "$linecount" "$file" > "$l_target_dir/$filename"
             fi
         done
      elif [[ "$dir" == "cc" ]]; then
          linecount=130000
          for file in "$l_source_dir"/*; do
              if [[ -f "$file" ]]; then
                  filename=$(basename "$file")
                  head -n "$linecount" "$file" > "$l_target_dir/$filename"
              fi
          done
     else
       cp -r "$l_source_dir"/* "$l_target_dir/"
     fi
     echo "Copied contents from $l_source_dir to $l_target_dir"
   else
     echo "Source directory $l_source_dir does not exist. Skipping."
   fi
 done

 for dir in "${programanalysis_directories[@]}"; do
   source_dir="$datadir/$dir/data_1MB"
   m_target_dir="$datadir/$dir/m_data"
   l_target_dir="$datadir/$dir/l_data"

   if [ -d "$source_dir" ]; then
     mkdir -p "$m_target_dir"
     mkdir -p "$l_target_dir"

     # Set linecount based on the value of dir
     if [[ "$dir" == *"andersen"* ]]; then
         linecount_m=500
         linecount_l=1000
     elif [[ "$dir" == *"dataflow"* ]]; then
         linecount_m=1000
         linecount_l=2000
     elif [[ "$dir" == *"java"* || "$dir" == *"count"* ]]; then
         linecount_m=1500
         linecount_l=3000
     elif [[ "$dir" == *"cba"* ]]; then
         linecount_m=1000
         linecount_l=2000
     else
         echo "Directory not program analysis benchmark?"
         exit 1
     fi

     for file in "$source_dir"/*; do
         if [[ -f "$file" ]]; then
             filename=$(basename "$file")
             head -n "$linecount_m" "$file" > "$m_target_dir/$filename"
             head -n "$linecount_l" "$file" > "$l_target_dir/$filename"
         fi
     done
     echo "Copied contents from $source_dir to $m_target_dir with $linecount lines, and to $l_target_dir with $((linecount*2)) lines"
   else
     echo "Source directory $source_dir does not exist. Skipping."
   fi
 done

 for dir in "${graph_directories[@]}"; do
   m_source_dir="$datadir/$dir/data_10MB"
   m_target_dir="$datadir/$dir/m_data"

   if [ -d "$m_source_dir" ]; then
     mkdir -p "$m_target_dir"

     if [[ "$dir" == "tc" ]]; then
         linecount=400000
         for file in "$m_source_dir"/*; do
             if [[ -f "$file" ]]; then
                 filename=$(basename "$file")
                 head -n "$linecount" "$file" > "$m_target_dir/$filename"
             fi
         done
     else
       cp -r "$m_source_dir"/* "$m_target_dir/"
     fi

     echo "Copied contents from $m_source_dir to $m_target_dir"
   else
     echo "Source directory $m_source_dir does not exist. Skipping."
   fi
 done

echo "copying XL data"
 for dir in "${graph_directories[@]}"; do
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
       base_csv_path="$datadir/$dir/data_${target_size}MB/base.csv"
       echo -e "dst,cost\n1,0" > "$base_csv_path"
       echo "    ---> Generated $base_csv_path"
       cd "$datadir"
   elif [[ "$dir" == *"tc" ]]; then
       target_size=10
       mkdir -p "$datadir/$dir/data_${target_size}MB"
       cd "$datadir/$dir/data_${target_size}MB"
       python3 ../../generate.py --size "$target_size" --units MB --columns "(x, int) (y, int)" --filename edge.csv  --base1
       cd "$datadir"
   else
       echo "Directory not graph benchmark?"
       exit 1
   fi

    l_source_dir="$datadir/$dir/data_${target_size}MB"
    l_target_dir="$datadir/$dir/l_data"

    if [ -d "$l_source_dir" ]; then
      mkdir -p "$l_target_dir"

      cp -r "$l_source_dir"/* "$l_target_dir/"
      echo "Copied contents from $l_source_dir to $l_target_dir"
    else
      echo "Source directory $l_source_dir does not exist. Skipping."
    fi
  done