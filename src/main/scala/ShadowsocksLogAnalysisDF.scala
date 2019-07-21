import com.xiumuzjq.shadowsockslogparser._
import org.apache.spark.sql.SparkSession


object ShadowsocksLogAnalysisDF {

    /**
     * 用 spark dataframe 的方式分析 shadowsockslog, 并将结果写入文件
     *
     */
    def main(args: Array[String]) {
        val spark = SparkSession.builder().appName("experiment").getOrCreate()
        import spark.implicits._

        val p = new ShadowsocksLogParser
        val log = spark.read.text("/mnt/resource/shadowsocks.log").map(line => p.parseRecordReturningNullObjectOnFailure(line.getString(0))).registerTempTable("logs")
        
        val aggRst = spark.sql("select count(1) as num, destHost from logs group by destHost order by num desc limit 10")

        aggRst.show()
    }
}
