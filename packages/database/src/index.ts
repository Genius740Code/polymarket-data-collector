// @arb/database — Postgres access helpers (Phase 1: connection + types only).
// All writes in later phases MUST go through parameterized queries; atomic tmp+rename applies
// to parquet paths in the legacy collector, while SQL writes here use transactions.
// Schema DDL lives in infrastructure/database/migrations/001_initial_schema.sql.

export interface DbConfig {
  connectionString: string;
  maxPoolSize?: number;
  statementTimeoutMs?: number;
}

export interface QueryResult<T = Record<string, unknown>> {
  rows: T[];
  rowCount: number;
}

// Minimal pool abstraction so services do not depend directly on `pg` in Phase 1.
// A real `pg` Pool adapter is wired in apps/api (lazy import to keep unit tests dependency-free).
export interface DbPool {
  query<T = Record<string, unknown>>(text: string, params?: unknown[]): Promise<QueryResult<T>>;
  end(): Promise<void>;
}

export function validateDbConfig(cfg: DbConfig): void {
  if (!cfg.connectionString || typeof cfg.connectionString !== 'string') {
    throw new Error('DATABASE_URL must be a non-empty connection string');
  }
  if (!/^postgres(ql)?:\/\//.test(cfg.connectionString)) {
    throw new Error('DATABASE_URL must start with postgres:// or postgresql://');
  }
}

export async function createPgPool(cfg: DbConfig): Promise<DbPool> {
  validateDbConfig(cfg);
  // Lazy import keeps `pg` optional for unit tests / environments without the driver.
  interface PgModule {
    Pool: new (o: unknown) => {
      query: (t: string, p?: unknown[]) => Promise<{ rows: unknown[]; rowCount: number | null }>;
      end: () => Promise<void>;
    };
  }
  let mod: PgModule | null = null;
  try {
    // @ts-ignore TS2307: 'pg' is an optional peer dep, installed only in apps/api image
    const imported: unknown = await import('pg');
    mod = imported as PgModule;
  } catch {
    mod = null;
  }
  if (!mod) {
    throw new Error('npm package "pg" is not installed; add it to apps/api to enable Postgres writes');
  }
  const PgPool = mod.Pool;
  const pool = new PgPool({
    connectionString: cfg.connectionString,
    max: cfg.maxPoolSize ?? 10,
    statement_timeout: cfg.statementTimeoutMs ?? 10_000,
  });
  return {
    async query<T = Record<string, unknown>>(text: string, params?: unknown[]): Promise<QueryResult<T>> {
      const r = await pool.query(text, (params ?? []) as unknown[]);
      return { rows: r.rows as T[], rowCount: r.rowCount ?? 0 };
    },
    async end() {
      await pool.end();
    },
  };
}
