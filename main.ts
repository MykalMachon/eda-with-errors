import { Database } from 'jsr:@db/sqlite'
import * as retry from "https://deno.land/x/retry@v2.0.0/mod.ts";

type EventHandler = (data: Record<string, string>, db: Database) => void

const initDatabase = (): Database => {
  // create sqlite db with table for processed events and dead letter queue
  const db = new Database('events.db');
  db.exec(`PRAGMA journal_mode=WAL;`);

  db.exec(`CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT);`);
  db.exec(`CREATE TABLE IF NOT EXISTS dead_letter_queue (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT, failed_at TEXT);`);

  return db;
}

const handleEvent: EventHandler = async (data, db) => {
  try {
    await retry.retryAsync(async () => {
      // wait for 100ms - 500ms
      await new Promise((resolve) => setTimeout(resolve, 100 + Math.random() * 400));

      // simulate error
      if (Math.random() > 0.5) {
        throw new Error('Random error');
      }

      db.exec(`INSERT INTO events (data) VALUES ('${JSON.stringify(data)}');`);
      console.log('Event processed');
    }, { maxTry: 3, delay: 500 })
  } catch (_err) {
    console.log('Event failed');
    db.exec(`INSERT INTO dead_letter_queue (data, failed_at) VALUES ('${JSON.stringify(data)}', '${new Date().toISOString()}');`);
  }
}

const eventGenerator = (callback: EventHandler, db: Database) => {
  const baseFreq = 1000; // 1 second 
  const jitter = ((Math.random() + 1) * 1000) - baseFreq; // 0-1 second

  setTimeout(() => {
    console.log('Event generated');
    callback({ jitter: jitter.toString() }, db);
    eventGenerator(callback, db);
    // garbage below to add some delay and infrequency
  }, jitter > 500 ? jitter + baseFreq : jitter);
}

export function add(a: number, b: number): number {
  return a + b;
}

// Learn more at https://docs.deno.com/runtime/manual/examples/module_metadata#concepts
if (import.meta.main) {
  const db = initDatabase();
  eventGenerator(handleEvent, db);
}
