package com.gkatzioura.scala.storm

import java.text.BreakIterator

import org.apache.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}

/**
  * Created by gkatzioura on 2/18/17.
  */
class SplitSentenceBolt extends BaseBasicBolt {

  override def execute(input: Tuple, collector: BasicOutputCollector): Unit = {

    val sentence = input.getString(0)
    val boundary = BreakIterator.getWordInstance

    boundary.setText(sentence)
    var start = boundary.first
    var end:Int = start

    while(end!=BreakIterator.DONE) {

      end = boundary.next
      val word = sentence.substring(start,end).replaceAll("\\s+","")
      start = end
      if(!word.equals("")) {
        collector.emit(new Values(word))
      }
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("word"))
  }
}
