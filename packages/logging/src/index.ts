// @arb/logging — structured JSON logging with secret redaction.
// Every log line: timestamp, level, service, + optional venue/event/market/requestId/correlationId/latency/result/error.
// Never log API keys, secrets, passwords, or private credentials.

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface LogFields {
  service: string;
  venue?: string;
  event?: string;
  market?: string;
  requestId?: string;
  correlationId?: string;
  latencyMs?: number;
  result?: string;
  [key: string]: unknown;
}

const SECRET_KEYS = [
  'apikey',
  'api_key',
  'privatekey',
  'private_key',
  'password',
  'secret',
  'token',
  'authorization',
  'cert',
  'mnemonic',
  'seed',
];

function isSecretKey(k: string): boolean {
  const l = k.toLowerCase();
  return SECRET_KEYS.some((s) => l.includes(s));
}

export function redact(value: unknown, depth = 0): unknown {
  if (depth > 6) return '[truncated]';
  if (Array.isArray(value)) return value.map((v) => redact(v, depth + 1));
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      out[k] = isSecretKey(k) ? '[REDACTED]' : redact(v, depth + 1);
    }
    return out;
  }
  if (typeof value === 'string') {
    // Redact bearer tokens / long hex secrets that may appear in messages.
    if (/bearer\s+[A-Za-z0-9\-_.~+/=]{8,}/i.test(value)) return value.replace(/bearer\s+\S+/i, 'Bearer [REDACTED]');
    return value;
  }
  return value;
}

const LEVEL_ORDER: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };

export class Logger {
  private level: LogLevel;
  private base: LogFields;
  private sink: (line: string) => void;

  constructor(service: string, opts?: { level?: LogLevel; sink?: (line: string) => void; base?: LogFields }) {
    this.level = opts?.level ?? ((process.env['LOG_LEVEL'] as LogLevel) || 'info');
    this.base = { service, ...(opts?.base ?? {}) };
    this.sink = opts?.sink ?? ((line: string) => console.log(line));
  }

  child(fields: Omit<LogFields, 'service'>): Logger {
    const c = new Logger(this.base['service'] as string, { level: this.level, sink: this.sink });
    c.base = { ...this.base, ...fields };
    return c;
  }

  setLevel(level: LogLevel): void {
    this.level = level;
  }

  private emit(level: LogLevel, msg: string, fields?: Omit<LogFields, 'service'>, err?: unknown): void {
    if (LEVEL_ORDER[level] < LEVEL_ORDER[this.level]) return;
    const rec = {
      timestamp: new Date().toISOString(),
      level,
      msg,
      ...this.base,
      ...(fields ?? {}),
      ...(err !== undefined
        ? { error: err instanceof Error ? { name: err.name, message: err.message, stack: err.stack } : String(err) }
        : {}),
    };
    this.sink(JSON.stringify(redact(rec)));
  }

  debug(msg: string, fields?: Omit<LogFields, 'service'>): void {
    this.emit('debug', msg, fields);
  }
  info(msg: string, fields?: Omit<LogFields, 'service'>): void {
    this.emit('info', msg, fields);
  }
  warn(msg: string, fields?: Omit<LogFields, 'service'>, err?: unknown): void {
    this.emit('warn', msg, fields, err);
  }
  error(msg: string, fields?: Omit<LogFields, 'service'>, err?: unknown): void {
    this.emit('error', msg, fields, err);
  }
}

export function createLogger(service: string, opts?: { level?: LogLevel }): Logger {
  return new Logger(service, opts);
}
