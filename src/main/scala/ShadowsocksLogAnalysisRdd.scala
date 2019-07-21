import com.xiumuzjq.shadowsockslogparser._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object ShadowsocksLogAnalysisRdd {

    /**
     * 用 spark rdd 的方式分析 shadowsockslog, 并将结果写入文件
     *
     */
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("master").setMaster("local")
        val sc = new SparkContext(conf)

        val p = new ShadowsocksLogParser
        val log = sc.textFile("/mnt/resource/shadowsocks.log")
        log.map(line => p.parseRecordReturningNullObjectOnFailure(line).destHost).map(host => (host, 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false).saveAsTextFile("/mnt/output/ShadowsocksLogParserRdd.txt")
    }
}
