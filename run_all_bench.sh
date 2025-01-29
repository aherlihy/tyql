#!/bin/bash

s_data_dir="data"
m_data_dir="m_data"
l_data_dir="l_data"

rm -f bench_s.out bench_m.out bench_l.out bench/benchmark_out_s.csv bench/benchmark_out_m.csv bench/benchmark_out_l.csv

export TYQL_DATA_DIR=$s_data_dir
sbt -java-home /scratch2/herlihy/graalvm-community-openjdk-17.0.9+9.1 "clean; bench/Jmh/clean"
sbt -java-home /scratch2/herlihy/graalvm-community-openjdk-17.0.9+9.1 "bench/Jmh/run -wi 5 -i 5 -rff benchmark_out_s.csv -jvmArgs \"-Xmx8G\"" &> bench_s.out

export TYQL_DATA_DIR=$m_data_dir
sbt -java-home /scratch2/herlihy/graalvm-community-openjdk-17.0.9+9.1 "clean; bench/Jmh/clean"
sbt -java-home /scratch2/herlihy/graalvm-community-openjdk-17.0.9+9.1 "bench/Jmh/run -wi 5 -i 5 -rff benchmark_out_m.csv -jvmArgs \"-Xmx8G\"" &> bench_m.out


export TYQL_DATA_DIR=$l_data_dir
sbt -java-home /scratch2/herlihy/graalvm-community-openjdk-17.0.9+9.1 "clean; bench/Jmh/clean"
sbt -java-home /scratch2/herlihy/graalvm-community-openjdk-17.0.9+9.1 "bench/Jmh/run -wi 5 -i 5 -rff benchmark_out_l.csv -jvmArgs \"-Xmx8G\"" &> bench_l.out