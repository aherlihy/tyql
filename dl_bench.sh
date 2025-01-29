#!/usr/bin/env bash

if [[ $# -ne 2 ]] ; then
    echo 'USAGE: <server #> <output file name (no extension)>'
    exit 0
fi
 
scp herlihy@diascld$1.iccluster.epfl.ch:/scratch2/herlihy/tyql-repro/bench/benchmark_out_s.csv results/$2$1_small.csv
scp herlihy@diascld$1.iccluster.epfl.ch:/scratch2/herlihy/tyql-repro/bench_s.out results/$2$1_small.out

scp herlihy@diascld$1.iccluster.epfl.ch:/scratch2/herlihy/tyql-repro/bench/benchmark_out_m.csv results/$2$1_medium.csv
scp herlihy@diascld$1.iccluster.epfl.ch:/scratch2/herlihy/tyql-repro/bench_m.out results/$2$1_medium.out

scp herlihy@diascld$1.iccluster.epfl.ch:/scratch2/herlihy/tyql-repro/bench/benchmark_out_l.csv results/$2$1_large.csv
scp herlihy@diascld$1.iccluster.epfl.ch:/scratch2/herlihy/tyql-repro/bench_l.out results/$2$1_large.out

cat results/$2$1_small.csv | sed -e 's/tyql.bench.TO//' -e 's/Benchmark\./\",\"/' -e 's/_graph//' -e 's/_misc//' -e 's/_programanalysis//' > results/$2$1_small_clean.csv
sed -n 's/^\[info\] IT, *//p' results/$2$1_small.out > results/$2$1_small_iterations.csv

cat results/$2$1_medium.csv | sed -e 's/tyql.bench.TO//' -e 's/Benchmark\./\",\"/' -e 's/_graph//' -e 's/_misc//' -e 's/_programanalysis//' > results/$2$1_medium_clean.csv
sed -n 's/^\[info\] IT, *//p' results/$2$1_medium.out > results/$2$1_medium_iterations.csv

cat results/$2$1_large.csv | sed -e 's/tyql.bench.TO//' -e 's/Benchmark\./\",\"/' -e 's/_graph//' -e 's/_misc//' -e 's/_programanalysis//' > results/$2$1_large_clean.csv
sed -n 's/^\[info\] IT, *//p' results/$2$1_large.out > results/$2$1_large_iterations.csv
