#!/bin/bash

 # Set the outer directory
 datadir="."

 # List of target directories within datadir
 m_directories=("asps" "bom" "cc" "cspa" "evenodd" "orbits" "party" "trustchain")
 s_directories=("andersens" "cba" "dataflow" "javapointsto" "pointstocount")
 l_directories=("sssp" "tc" "ancestry")


 for dir in "${m_directories[@]}"; do
   source_dir="$datadir/$dir/data_2MB"
   target_dir="$datadir/$dir/final_data2MB"

   if [ -d "$source_dir" ]; then
     mkdir -p "$target_dir"

     cp -r "$source_dir"/* "$target_dir/"
     echo "Copied contents from $source_dir to $target_dir"
   else
     echo "Source directory $source_dir does not exist. Skipping."
   fi
 done

 for dir in "${s_directories[@]}"; do
   source_dir="$datadir/$dir/final_data"
   target_dir="$datadir/$dir/final_data2MB"

   if [ -d "$source_dir" ]; then
     mkdir -p "$target_dir"

     cp -r "$source_dir"/* "$target_dir/"
     echo "Copied contents from $source_dir to $target_dir"
   else
     echo "Source directory $source_dir does not exist. Skipping."
   fi
 done

 for dir in "${l_directories[@]}"; do
   source_dir="$datadir/$dir/data_10MB"
   target_dir="$datadir/$dir/final_data2MB"

   if [ -d "$source_dir" ]; then
     mkdir -p "$target_dir"

     cp -r "$source_dir"/* "$target_dir/"
     echo "Copied contents from $source_dir to $target_dir"
   else
     echo "Source directory $source_dir does not exist. Skipping."
   fi
done