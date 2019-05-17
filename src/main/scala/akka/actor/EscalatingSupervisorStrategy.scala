package akka.actor

import akka.actor.SupervisorStrategy.{Decider, Escalate}

final class EscalatingSupervisorStrategy extends SupervisorStrategyConfigurator {
  override def create(): SupervisorStrategy = {
    def defaultDecider: Decider = {
      case _: Exception                    ⇒ Escalate
    }
    OneForOneStrategy()(defaultDecider)
  }
}
