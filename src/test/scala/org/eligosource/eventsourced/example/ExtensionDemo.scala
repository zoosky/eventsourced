/*
 * Copyright 2012 Eligotech BV.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.eligosource.eventsourced.example

import java.io.File

import akka.actor._
import akka.pattern.ask
import akka.util.duration._
import akka.util.Timeout

import org.eligosource.eventsourced.core.{EventsourcedExperimental => Eventsourced, _}
import org.eligosource.eventsourced.journal.LeveldbJournal

object ExtensionDemo extends App {
  implicit val system = ActorSystem("example")
  implicit val timeout = Timeout(5 seconds)

  // Event sourcing extension
  // (any actor created 'with Eventsourced' will be automatically registered at this extension)
  implicit val extension = EventsourcingExtension(system, LeveldbJournal(new File("target/example")))

  // Modification 'with Eventsourced' makes actor persistent
  // (processorId given via constructor)
  val processorA = system.actorOf(Props(new ProcessorA(1) with Eventsourced))
  val processorB = system.actorOf(Props(new ProcessorB(2) with Eventsourced))

  // Modification 'with Receiver' makes actor an acknowledging event/command message receiver
  val destination = system.actorOf(Props(new Destination with Receiver))

  val channelA = extension.registerChannel(1, "channelA", processorA, None)
  val channelB = extension.registerChannel(2, "channelB", destination, None)

  // Modification 'with Eventsourced' makes actor persistent
  // (processorId and dependent channels given via constructor)
  val processorC = system.actorOf(Props(new ProcessorC(3, channelA, channelB) with Eventsourced with ForwardMessage))

  // await registration of 3 processors and 2 channels
  extension.awaitRegistrations(5, 3 seconds)

  // recover all registered processors from journal
  extension.recover()

  val p: ActorRef = processorC // can be replaced with processorB

  // send event message to p
  p ! Message("some event")

  // send event message to p and receive response (Ack) once the event has been persisted
  // (event message is persisted before the processor receives it)
  p ? Message("some event") onSuccess { case Ack => println("event written to journal") }

  // send event message to p but receive application-level response from processor (or any of its destinations)
  // (event message is persisted before receiving the response)
  p ??? Message("some event") onSuccess { case resp => println("received response %s" format resp) }

  // send message to p (bypasses journaling because it's not an instance of Message)
  p ! "blah"

  // -----------------------------------------------------------
  //  Actor definitions
  // -----------------------------------------------------------

  class ProcessorA(val id: Int) extends Actor {
    def receive = {
      case event => {
        // do something with event
        println("received event = %s" format event)
      }
    }
  }

  class ProcessorB(val id: Int) extends Actor { this: Eventsourced =>
    def receive = {
      case "blah" => {
        println("received non-journaled message")
      }
      case event => {
        // Eventsourced actors have access to:
        val seqnr = sequenceNr       // sequence number of journaled message
        val sdrid = senderMessageId  // message id provided by sender (duplicate detection)
        val initr = initiator        // initiatal sender of message (can be different from current 'sender')
        val prcid = id               // processor id
        // ...

        // do something with event
        println("received event = %s (processor id = %d, sequence nr = %d)" format(event, prcid, seqnr))

        // Eventsourced actors can emit events to named channels
        emitter("channelA").emitEvent("out-a")
        emitter("channelB").emitEvent("out-b")

        // optionally respond to initial sender (initiator)
        // (intitiator == context.system.deadLetters if unknown)
        initiator ! "done"
      }
    }
  }

  // does the same as ProcessorB but doesn't use any attributes of trait Eventsourced
  class ProcessorC(val id: Int, channelA: ActorRef, channelB: ActorRef) extends Actor {
    def receive = {
      case "blah" => {
        println("received non-journaled message")
      }
      case msg: Message => {
        println("received event = %s (processor id = %d, sequence nr = %d)" format(msg.event, id, msg.sequenceNr))

        channelA ! msg.copy(event = "out-a")
        channelB ! msg.copy(event = "out-b")

        msg.sender.foreach(_ ! "done")
      }
    }
  }

  /**
   * Receiver extracts payload (event or command) from received Message
   * and (automatically) acknowledges receipt
   */
  class Destination extends Actor { this: Receiver =>
    def receive = {
      case event => {
        // Receiver actors have access to:
        val seqnr = sequenceNr       // sequence number of journaled message
        val sdrid = senderMessageId  // message id provided by sender (duplicate detection)
        val initr = initiator        // initiatal sender of message (can be different from current 'sender')
        // ...

        // do something with event
        println("received event = %s" format event)
      }
    }
  }
}
