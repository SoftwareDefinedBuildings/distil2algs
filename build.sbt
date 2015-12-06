name := """distillates"""

organization := "org.cal-sdb"

version := "0.1.0"

scalaVersion := "2.10.4"

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  
libraryDependencies += "io.btrdb" %% "distil" % "0.1.0"

scalacOptions += "-feature"
scalacOptions += "-deprecation"

scalaSource in Compile <<= baseDirectory(_ / "src" / "scala")
