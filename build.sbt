name := "spark experiment"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.3.3",
    "org.apache.spark" %% "spark-sql" % "2.3.3"
)
