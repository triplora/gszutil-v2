package com.google.cloud.pso

import akka.actor.SupervisorStrategy.{Decider, Escalate}
import akka.actor.{OneForOneStrategy, SupervisorStrategy, SupervisorStrategyConfigurator}

final class EscalatingSupervisorStrategy extends SupervisorStrategyConfigurator {
  override def create(): SupervisorStrategy = {
    def defaultDecider: Decider = {
      case _: Exception                    ⇒ Escalate
    }
    OneForOneStrategy()(defaultDecider)
  }
}
