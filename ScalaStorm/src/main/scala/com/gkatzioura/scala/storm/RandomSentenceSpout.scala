package com.gkatzioura.scala.storm


import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.{Fields, Values}
import org.apache.storm.utils.Utils

import scala.util.Random

/**
  * Created by gkatzioura on 2/17/17.
  */
class RandomSentenceSpout extends BaseRichSpout {

  var _collector:SpoutOutputCollector = _
  var _rand:Random = _

  override def nextTuple(): Unit = {

    Utils.sleep(100)

    val sentences = Array("the cow jumped over the moon","an apple a day keeps the doctor away",
      "four score and seven years ago","snow white and the seven dwarfs","i am at two with nature")
    val sentence = sentences(_rand.nextInt(sentences.length))
    _collector.emit(new Values(sentence))
  }

  override def open(conf: java.util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit = {
    _collector = collector
    _rand = Random
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("word"))
  }

}
