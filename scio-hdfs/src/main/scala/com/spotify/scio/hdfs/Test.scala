package com.spotify.scio.hdfs

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import scala.collection.mutable.{ListBuffer, Set => MSet}
import scala.util.Random

object Test {

  def main(args: Array[String]): Unit = {
    val path = new Path("hdfs:///anonym/cleaned/endsong/2016-07-01/00")
    val fs = FileSystem.get(path.toUri, new Configuration)
    val status = fs.listStatus(path, new ExtPathFilter(".avro"))

    new AvroFileRandomSampler(status(0).getPath).sample(100)
  }

}

class ExtPathFilter(ext: String) extends PathFilter {
  override def accept(path: Path): Boolean = path.getName.endsWith(ext)
}


trait RandomSampler[T] {
  protected val random: Random
  protected def nextLong(n: Long): Long = {
    require(n > 0, "n must be positive")
    var bits: Long = 0
    var value: Long = 0
    do {
      bits = random.nextLong() << 1 >>> 1
      value = bits % n;
    } while (bits - value + (n - 1) < 0)
    value
  }

  def sample(n: Long): Seq[T]
}

class AvroFileRandomSampler(path: Path, seed: Long = 0) extends RandomSampler[GenericRecord] {
  protected val random: Random = new Random(seed)

  override def sample(n: Long): Seq[GenericRecord] = {
    require(n > 0, "n must be > 0")

    val input = new AvroFSInput(FileContext.getFileContext, path)
    val datumReader = new GenericDatumReader[GenericRecord]()
    val fileReader = DataFileReader.openReader(input, datumReader)

    val schema = fileReader.getSchema
    val start = fileReader.tell()
    val end = input.length()
    val range = end - start

    val result = ListBuffer.empty[GenericRecord]
    val positions = MSet.empty[Long]

    // sample at regular interval
    /*
    var off = start
    val interval = range / n
    while (result.size < n && off < end) {
      fileReader.sync(off)
      val pos = fileReader.tell()
      if (!positions.contains(pos)) {
        positions.add(pos)
        result.append(fileReader.next())
        println(off + " " + pos + " " + result.last)
      } else {
        println(off + " " + pos)
      }
      off += interval
    }
    */

    // sample at random positions until 10 collisions in a roll
    var collisions = 0
    while (result.size < n && collisions < 10) {
      val off = start + nextLong(range)
      fileReader.sync(off)
      val pos = fileReader.tell()
      if (!positions.contains(pos)) {
        collisions = 0
        positions.add(pos)
        result.append(fileReader.next())
        println(off + " " + pos + " " + result.last)
      } else {
        collisions += 1
        println(off + " " + pos + " " + collisions)
      }
    }

    result
  }
}