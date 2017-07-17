import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Paths
import scala.io.Source

object Main extends App {
  val baseDir = "/data/DataSet/BGP"

  def parseRIB(line: String): RIB = {
    val cols = line.substring(1, line.length - 1).split(",")
      .map(_.trim.stripPrefix("'").stripSuffix("'"))
    RIB(
      cols(0), cols(1), cols(2), cols(3).toLong,
      cols(4).eq("valid"), cols(5), cols(6), cols(7).toInt,
      cols(8), cols(9).split(" ").map(_.toInt).toSeq
    )
  }

  def parseUpdate(line: String): Update = {
    val cols = line.substring(1, line.length - 1).split(",")
      .map(_.trim.stripPrefix("'").stripSuffix("'"))
    Update(
      cols(0), cols(1), cols(2), cols(3).toLong,
      cols(4).eq("valid"), cols(5), cols(6),
      cols(7).toInt, cols(8)
    )
  }

  def trans(): Unit = {


    val dataSet = (0 until 3).map(no => s"$baseDir/rib$no.txt")

    val prefer: Set[Int] = Set(
      8287, 9002, 29073, 34305, 38197, 49544, 50215, 8342, 15169, 16265, 21219, 29078, 33626, 34109, 35415,
      32, 109, 714, 794, 1313, 2510, 10934, 12008, 14618, 17184, 19836, 23576, 34922, 53297, 50098
    )

    val records = dataSet
      .map(path => new File(path))
      .map(f => resource.managed(Source.fromFile(f)).map(_.getLines).toTraversable.toIterable.map(parseRIB).toArray)
      .reduce((arrayA, arrayB) => arrayA ++ arrayB).toSeq

    println(s"total records ${records.length}")

    records.par
      .filter(rib => prefer.contains(rib.asPath.last))
      .groupBy(rib => (rib.timestamp / 60 / 60 / 24 * 60 * 60 * 24, rib.asPath.last))
      .toSeq
      .foreach {
        case ((timestamp, asNo), seq) =>
          val path = Paths.get(s"$baseDir/AS", asNo.toString, timestamp.toString + ".tsv")
          path.getParent.toFile.mkdirs()
          val file = path.toFile
          file.createNewFile()
          println(s"writing to file ${file.getAbsolutePath}")
          val writer = new BufferedWriter(new FileWriter(file))
          seq.par
            .map(rib => rib.prefix).toSet[String]
            .foreach(prefix => writer.write(s"$prefix\n"))
          writer.close()
      }
  }


  def churnCalc(dataDirPath: String): Unit = {
    type PrefixSet = Set[Prefix]
    type FilePrefixSets = (PrefixSet, PrefixSet, PrefixSet, PrefixSet)


    def stencil(f: File): FilePrefixSets = {
      val source = Source.fromFile(f)
      val groups = source.getLines()
        .map(line => {
          val segment = line.split("/")
          val network = segment(0)
          val mask = segment(1).toInt
          Prefix(network, mask)
        }).toSeq
        .groupBy(_.mask / 8)
        .map { case ((key, group)) => (key, group.toSet) }
      source.close()
      (groups.getOrElse(0, Set()),
        groups.getOrElse(1, Set()),
        groups.getOrElse(2, Set()),
        groups.values.fold(Set())(_ ++ _))
    }

    def extract(fileName: String): String = fileName.split('.')(0)

    implicit def file2Name(file: File): String = file.getName

    val dataDir = new File(dataDirPath)
    dataDir.listFiles(_.isDirectory)
      .map(asDir => asDir.listFiles(_.isFile).sorted).par
      .foreach(changes => {
        val result: String = changes
          .sliding(2)
          .map(files => {
            val Array(aPrefixSets, bPrefixSets) = files.map(stencil)
            (files.map(file => extract(file)) ++ aPrefixSets.productIterator.zip(bPrefixSets.productIterator)
              .map { case (aPrefix: PrefixSet, bPrefix: PrefixSet) => Statistics.jaccardIndex(aPrefix, bPrefix) })
              .mkString("\t")
          }).mkString("\n")
        val path = Paths.get(s"$baseDir/Churn/${extract(changes(0).getParentFile)}.tsv")
        path.getParent.toFile.mkdirs()
        val file = path.toFile
        file.createNewFile()
        println(s"writing to file ${file.getAbsolutePath}")
        val writer = new BufferedWriter(new FileWriter(file))
        writer.write(result)
        writer.close()
      })
  }

  def groupUpdate(updatePath: String): Unit = {
    /** Seq[Update] ->
      * {AS}.txt: format ->
      * Seq[Update] all related to $AS ordered by Update timestamp
      */
    resource.managed(Source.fromFile(updatePath))
      .map(_.getLines)
      .toTraversable
      .map(parseUpdate).par
      .groupBy(update => (update.asNo, update.timestamp)).toArray
      .sortBy { case ((_, timestamp), _) => timestamp }
      .foreach { case ((asNo, _), updateIter) =>
        val updateArray = updateIter.toArray.sortBy(_.timestamp)
        val file = Paths.get(s"$baseDir/Updates/$asNo.tsv").toFile
        file.getParentFile.mkdirs()
        file.createNewFile()
        val writer = new BufferedWriter(new FileWriter(file))
        updateArray.foreach(update => writer.write(update.productIterator.toSeq.mkString("\t")))
        writer.close()
      }
  }

  def reachableCalc(dataDirPath: String): Unit = {

  }

  groupUpdate(s"$baseDir/updates.txt")

}
