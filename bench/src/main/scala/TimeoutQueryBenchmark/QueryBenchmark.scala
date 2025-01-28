package tyql.bench

import buildinfo.BuildInfo

import scala.annotation.experimental

@experimental
trait QueryBenchmark {
    def name: String
    def datadir = s"${BuildInfo.baseDirectory}/bench/data/$name"
    def outdir = s"$datadir/out"
    def set: Boolean

    def initializeCollections(): Unit
    def executeTyQL(ddb: DuckDBBackend): Unit
    def executeScalaSQL(ddb: DuckDBBackend): Unit
    def executeCollections(): Unit
    def writeTyQLResult(): Unit
    def writeCollectionsResult(): Unit
    def writeScalaSQLResult(): Unit
}