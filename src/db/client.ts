import { sql, type SQL } from "drizzle-orm";
import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";

import * as schema from "./schema.ts";

/** Row shape returned by the pg driver through drizzle. */
export type QueryResultLike = { rows: Record<string, unknown>[] };

/**
 * Minimal SQL port used by every repository. Repositories never build SQL from
 * untrusted strings — they use drizzle's parameterized `sql` template.
 * Tests substitute an in-memory implementation of this port.
 */
export interface SqlExecutor {
  execute(query: SQL): Promise<QueryResultLike>;
}

export interface AppDb extends SqlExecutor {
  transaction<T>(fn: (tx: SqlExecutor) => Promise<T>): Promise<T>;
}

export function createPool(databaseUrl: string, maxConnections = 5): Pool {
  return new Pool({ connectionString: databaseUrl, max: maxConnections });
}

/** Adapt the drizzle node-postgres client to the narrow SQL port. */
export function createDb(pool: Pool): AppDb {
  const db = drizzle(pool, { schema });

  return {
    execute(query: SQL): Promise<QueryResultLike> {
      return db.execute(query) as Promise<QueryResultLike>;
    },
    transaction<T>(fn: (tx: SqlExecutor) => Promise<T>): Promise<T> {
      return db.transaction((tx) =>
        fn({
          execute(query: SQL): Promise<QueryResultLike> {
            return tx.execute(query) as Promise<QueryResultLike>;
          },
        }),
      );
    },
  };
}

/** Readiness probe for the PostgreSQL pool; never throws. */
export async function pingDatabase(pool: Pool): Promise<boolean> {
  try {
    await pool.query("SELECT 1");
    return true;
  } catch {
    return false;
  }
}

export async function closePool(pool: Pool): Promise<void> {
  await pool.end();
}

export const NOW = sql`now()`;
