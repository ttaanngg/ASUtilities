/**
  * Created by tangweiqiang on 2017/7/8.
  */
object Statistics {
  def characteristic[T: Ordering](s: IndexedSeq[T]): (T, T, T) = {
    val ss = s.sorted
    implicit def int2Double(int: Int): Double = int.toDouble
    implicit def double2Int(double: Double): Int = double.round.toInt
    def ?(seq: IndexedSeq[T], rate: Double) = seq(rate * (seq.length - 1))
    if (s.isEmpty) throw new RuntimeException("can not get characteristic of empty seq.")
    else (?(ss, 0.05), ?(ss, 0.5), ?(ss, 0.95))
  }

  def jaccardIndex[T](a: Set[T], b: Set[T]): Double = a.intersect(b).size / a.union(b).size.toDouble

}