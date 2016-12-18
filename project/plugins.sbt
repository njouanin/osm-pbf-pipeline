addSbtPlugin("com.thesamet" % "sbt-protoc"  % "0.99.3")
addSbtPlugin("me.lessis"    % "bintray-sbt" % "0.3.0")

libraryDependencies += "com.trueaccord.scalapb" %% "compilerplugin" % "0.5.45"

logLevel := Level.Warn
