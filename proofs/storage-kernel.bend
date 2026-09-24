import Base
import ./storage-plans.bend as C

def storageTransition(request: C.Request, slot: C.Slot) -> C.Plan:
  C.reduce(request, slot)

type StorageKernel is Type:
  StorageKernel{transition: C.Request -> C.Slot -> C.Plan}

def main() -> IO(StorageKernel):
  IO.pure(StorageKernel, StorageKernel{storageTransition})
