#!/bin/bash
/home/francisco/spark/bin/spark-submit ca


--class es.indra.telco.platforms.bigdata.flowview.CDRStats ../target/flowview-analyzer-0.0.1-SNAPSHOT.jar $*
