import { createLogger } from "../../src/logger.ts";

/** Winston logger silenced for deterministic test output. */
export function createSilentLogger() {
  const log = createLogger("error");
  log.silent = true;
  return log;
}

export const silentLogger = createSilentLogger();
