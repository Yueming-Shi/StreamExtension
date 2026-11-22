ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "3.10.1"
ThisBuild / crossScalaVersions := Seq("2.13.18", "3.10.1")

name := "stream-extension"

val zioVersion = "2.1.27"
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-streams"  % zioVersion,
  "dev.zio" %% "zio-test"     % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test
)
testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
