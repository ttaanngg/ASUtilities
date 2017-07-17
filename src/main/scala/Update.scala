/**
  * Created by tangweiqiang on 2017/7/16.
  */
sealed case class Update(routerView: String, collector: String, format: String,
                         timestamp: Long, valid: Boolean, method: String, fromIP: String,
                         asNo:Int, prefix: String)
