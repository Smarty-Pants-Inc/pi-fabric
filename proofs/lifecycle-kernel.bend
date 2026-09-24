import Base
import ./lifecycle.bend as L

def bindingStep(life: L.Life, event: L.BindingEvent) -> L.Outcome:
  L.bindingStep(life, event)

type LifecycleKernel is Type:
  LifecycleKernel{step: L.Life -> L.BindingEvent -> L.Outcome}

def main() -> IO(LifecycleKernel):
  IO.pure(LifecycleKernel, LifecycleKernel{bindingStep})
