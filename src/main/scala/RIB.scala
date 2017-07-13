/**
  * Created by tangweiqiang on 2017/7/10.
  */
sealed case class RIB(routerView: String, collector: String, format: String,
                      timestamp: Long, valid: Boolean, method: String, dst: String,
                      fromAS: Int, prefix: String, asPath: Seq[Int])
