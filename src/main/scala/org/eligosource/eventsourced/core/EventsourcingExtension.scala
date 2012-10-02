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
package org.eligosource.eventsourced.core

import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec

import akka.actor._
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.Duration

object EventsourcingExtension  extends ExtensionId[EventsourcingExtensionImpl] with ExtensionIdProvider {
  def apply(system: ActorSystem, journal: ActorRef): EventsourcingExtensionImpl =
    super.apply(system).registerJournal(journal)

  def createExtension(system: ExtendedActorSystem) =
    new EventsourcingExtensionImpl(system)

  def lookup() = EventsourcingExtension
}

class EventsourcingExtensionImpl(system: ActorSystem) extends Extension {
  import Registrar._

  private val channelsRef = new AtomicReference[Map[String, ActorRef]](Map.empty)
  private val processorsRef = new AtomicReference[Map[Int, ActorRef]](Map.empty)

  private var journalOption: Option[ActorRef] = None
  private val registrar = system.actorOf(Props(new Registrar(channelsRef, processorsRef)))

  val producer: ActorRef = system.actorOf(Props[Producer])

  def journal: ActorRef =
    journalOption.getOrElse(throw new IllegalStateException("no journal registered"))

  def channels: Map[String, ActorRef] =
    channelsRef.get

  def processors: Map[Int, ActorRef] =
    processorsRef.get

  def replay(f: (Int) => Option[Long]) {
    val replays = processors.collect { case kv if (f(kv._1).isDefined) => ReplayInMsgs(kv._1, f(kv._1).get, kv._2) }
    journal ! BatchReplayInMsgs(replays.toList)
  }

  def deliver() {
    journal ! BatchDeliverOutMsgs(channels.values.toList)
  }

  def recover() {
    recover(_ => Some(0))
  }

  def recover(f: (Int) => Option[Long]) {
    replay(f)
    deliver()
  }

  def awaitRegistrations(count: Int, atMost: Duration) =
    Await.result(registrar.ask(AwaitRegistration(count))(atMost), atMost)

  def registerChannel(channelId: Int, destination: ActorRef, replyDestination: Option[ActorRef])
           (implicit system: ActorSystem): ActorRef = {

    registerChannel(channelId, channelId.toString, destination, replyDestination)
  }

  def registerChannel(channelId: Int, channelName: String, destination: ActorRef, replyDestination: Option[ActorRef])
           (implicit system: ActorSystem): ActorRef = {

    val extension = EventsourcingExtension(system)
    val channel = system.actorOf(Props(new DefaultChannel(channelId, extension.journal)))

    channel ! Channel.SetDestination(destination)
    replyDestination.foreach(rd => channel ! Channel.SetReplyDestination(rd))

    extension.registerChannel(channelName, channel)
    channel
  }

  private [core] def registerChannel(channelName: String, channel: ActorRef) {
    registrar ! RegisterChannel(channelName, channel)
  }

  private [core] def registerProcessor(processorId: Int, processor: ActorRef) {
    registrar ! RegisterProcessor(processorId, processor)
  }

  private [core] def registerJournal(journal: ActorRef): EventsourcingExtensionImpl = {
    journalOption = Some(journal)
    this
  }
}

private [core] class Registrar(
  channelsRef: AtomicReference[Map[String, ActorRef]],
  processorsRef: AtomicReference[Map[Int, ActorRef]]) extends Actor {

  import Registrar._

  var waits: List[(Int, ActorRef)] = Nil

  def receive = {
    case wait @ AwaitRegistration(count) => {
      if (count > registrationCount) waits = (count, sender) :: waits else sender ! ()
    }
    case RegisterChannel(channelName, channel) => {
      registerChannel(channelName, channel)
      processWaits()
    }
    case RegisterProcessor(processorId, processor) => {
      registerProcessor(processorId, processor)
      processWaits()
    }
  }

  def registrationCount: Int =
    channelsRef.get.size + processorsRef.get.size

  @tailrec
  private def registerChannel(channelName: String, channel: ActorRef): Unit = {
    val current = channelsRef.get()
    val updated = current + (channelName -> channel)
    if (!channelsRef.compareAndSet(current, updated)) registerChannel(channelName, channel)
  }

  @tailrec
  private def registerProcessor(processorId: Int, processor: ActorRef): Unit = {
    val current = processorsRef.get()
    val updated = current + (processorId -> processor)
    if (!processorsRef.compareAndSet(current, updated)) registerProcessor(processorId, processor)
  }

  private def processWaits() {
    val count = registrationCount
    val (ready, pending) = waits.span(_._1 <= count)
    ready.foreach(_._2 ! ())
    waits = pending
  }
}

private [core] object Registrar {
  case class RegisterChannel(channelName: String, channel: ActorRef)
  case class RegisterProcessor(processorId: Int, processor: ActorRef)
  case class AwaitRegistration(count: Int)
}