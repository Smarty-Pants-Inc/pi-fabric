import { createHash } from "node:crypto";

const MACHINE = /^[a-z0-9][a-z0-9-]{0,31}$/;

export const validateMachine = (machine: string): string => {
  if (!MACHINE.test(machine)) throw new Error("invalid machine token");
  return machine;
};

export const hashToken = (value: string): string => createHash("sha256").update(value, "utf8").digest("hex").slice(0, 32);

export const ADVISORY_CAPTURE_STREAM = "FABRIC_ADV";
export const transportAdvisoryStreamSubject = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>";

export const transportInboxSubject = (destinationMachine: string, ownerHostId: string, kind: "cmd" | "ack", targetId: string, sourceMachine: string): string => {
  validateMachine(destinationMachine);
  validateMachine(sourceMachine);
  return `fabric.in.${destinationMachine}.${hashToken(ownerHostId)}.${kind}.${hashToken(targetId)}.${sourceMachine}`;
};

export const transportInboxStream = (machine: string): string => `FABRIC_IN_${validateMachine(machine)}`;
export const transportInboxStreamSubject = (machine: string): string => `fabric.in.${validateMachine(machine)}.>`;
export const transportInboxFilter = (machine: string, ownerHostId: string): string =>
  `fabric.in.${validateMachine(machine)}.${hashToken(ownerHostId)}.>`;
export const transportDurableName = (hostId: string): string => `rt_${hashToken(hostId)}`;
