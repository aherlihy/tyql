# TyQL: Type-Safe Recursive Queries Reproducibility 

# Experimental Section 
Experiments in this section are run on Intel(R) Xeon(R) Gold 5118 CPU @ 2.30GHz (2 x 12-core) with 376GB RAM, 
on Ubuntu 22.04 LTS with Linux kernel Linux 5.15.0-27-generic (diascld48) and Scala 3.5.1-RC1 The JVM used is
GraalVM Community Java 17.0.9 with sbt 1.9.9 with heap size 8GB

Each experiment is run with the Java Benchmarking Harness (JMH) with 5 iterations, 5 warm-up iterations.
The library versions are specified in the root `build.sbt` file.

# Input datasets
The experiments are run on 3 differently sized datasets that are stored as CSV files in the `bench/data` directory.
The DDL for each dataset is stored in `bench/data/<benchmark>/schema.ddl` and the column formats used as helpers to generate 
randomized data are stored in `bench/data/<benchmark>/csv_columns.txt`.
The smallest, s, is stored on github in the `bench/data/<benchmark>/data` folders. The output of each benchmark will write 
to the `bench/data/<benchmark>/out` folder.
The medium and large datasets are too large to store on github so we provide zipped folders containing `<benchmark>/m_data` 
and `l_data` directories. Unzip these folders so the structure is `bench/data/<benchmark>/m_data` and `bench/data/<benchmark>/l_data`. 

# Build TyQL
```shell
# Always clean + recompile both source and benchmarking code between runs, otherwise JMH can cause SBT to exit if something is out of sync 
$ sbt -java-home <path to jdk> "clean;compile;bench/Jmh/clean;bench/Jmh/compile"
```
# Run Benchmarks
Run benchmarks, on a dedicated server without other processes running at the same time. 
There is a script "run_all_bench.sh" that should help run all three sizes consecutively. Note that you will need to update
the `java-home` argument for your machine. The log of the benchmarks will write to `bench_<size>.out` and the results will
be written to a csv file `bench/benchmark_out_<size>.csv`. You can follow the progress with `tail -f bench_<size>.out`.
```shell
$ bash run_all_bench.sh
```

# Post-processing result data
This section describes how to generate the tables/charts used in the paper.
Search `bench_<size>.out` for `"Exception"`, there should not be any that are not timeouts, but if there were 
problems running the benchmarks then it will show up there.

## Cleaning the benchmark file
NOTE: I do the cleaning on my laptop which runs MacOS and zsh. If you are running this step on a different machine,
you may need to update the `sed` command below with the appropriate arguments for your operating system or shell. 
```shell
cat benchmark_out_<size>.csv | sed -e 's/tyql.bench.TO//' -e 's/Benchmark\./\",\"/' -e 's/_graph//' -e 's/_misc//' -e 's/_programanalysis//' > benchmark_<size>_clean.csv
```

## Generating aggregate data 
The excel file used to generate results is "repro-tyql.xlsx", import the data and the charts should auto-generate.
Steps:
1) Open `tyql-repro.xlsx` with Excel Version 16.76 (23081101)
2) Add an empty sheet to import the data into.
3) In that new sheet, navigate to "Data > Get Data (Power Query) > From Text (Legacy) > select the small datasize <benchmark_out>_clean.csv 
file produced by the section above > "Get Data"
4) In the text import wizard click "Delimited" > next > select "Comma". 
5) Do not treat consecutive delimiters as one. > Finish and put the data, ignoring the header, in the "small" sheet starting at row 3 column A. It should go to line 50.
6) Repeat steps 3-5 for the medium and large datasets in their respective sheets.

Due to the JIT there is some variability in the calculated speedups 
but the numbers should be within the same order of magnitude and consistent, relative to each other, with the ones in the paper.
If you want to additionally count iterations, then you can uncomment the printlines in `bench/src/main/scala/TimeoutQueryBenchmark/<query>.scala` 
and rerun the benchmarks, then post-processes the output to count the number of iterations.

If there are any questions or problems running the benchmark you can always contact me at herlihyap at gmail and I will do my best to clarify.
