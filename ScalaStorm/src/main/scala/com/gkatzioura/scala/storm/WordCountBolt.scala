package com.gkatzioura.scala.storm

import org.apache.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}

/**
  * Created by gkatzioura on 2/18/17.
  */
class WordCountBolt extends BaseBasicBolt{

  val counts = scala.collection.mutable.Map[String,Int]()

  override def execute(input: Tuple, collector: BasicOutputCollector): Unit = {

    val word = input.getString(0)

    val optCount = counts.get(word)
    if(optCount.isEmpty) {
      counts.put(word,1)
    } else {
      counts.put(word,optCount.get+1)
    }

    collector.emit(new Values(word,counts))
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {

    declarer.declare(new Fields("word","count"));
  }

}
