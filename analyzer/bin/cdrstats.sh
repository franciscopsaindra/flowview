#!/bin/bash
/home/francisco/spark/bin/spark-submit  --class es.indra.telco.platforms.test.CDRStats /home/francisco/workspace/TestPredictive/target/TestPredictive-0.0.1-SNAPSHOT.jar $*
