#!/bin/bash

# Find Java
if [ "$JAVA_HOME" = "" ] ; then
	JAVA="java"
else
	JAVA="$JAVA_HOME/bin/java"
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ] ; then
	JAVA_OPTIONS="-Xms1G -Xmx1G -d64 -server -XX:+UseConcMarkSweepGC"
fi

DIR=`dirname $0`

# Launch the application
$JAVA $JAVA_OPTIONS -cp $DIR"/rdfstream4j-impl/target/classes":$DIR"/rdfstream4j-impl/target/dependency/*" edu.rpi.twc.rdfstream4j.etc.TestRunner $*

# Return the program's exit code
exit $?

