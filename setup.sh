#!/bin/bash

# Install Python dependencies
pip install -r requirements.txt

# Install GraphFrames for Spark
$SPARK_HOME/bin/spark-shell --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 -i setup_graphframes.scala
