#!/usr/bin/env bun
/**
 * Recover library from JSON backups
 *
 * This script imports documents and chunks from JSON backup files
 * created by the export-data.mjs script.
 *
 * Prerequisites:
 *   - Fresh/empty library directory
 *   - backup-documents.json and backup-chunks.jsonl in .pdf-library/
 *
 * Usage:
 *   bun run scripts/migration/recover-from-json.ts
 *
 * After running this, run regenerate-embeddings.ts to recreate embeddings.
 */

import { PGlite } from "@electric-sql/pglite";
import { vector } from "@electric-sql/pglite/vector";
import { createReadStream, readFileSync } from "node:fs";
import { join } from "node:path";
import { createInterface } from "node:readline";

const LIBRARY_PATH = join(process.env.HOME ?? "", "Documents/.pdf-library");
const DB_PATH = join(LIBRARY_PATH, "library");
const DOCS_FILE = join(LIBRARY_PATH, "backup-documents.json");
const CHUNKS_FILE = join(LIBRARY_PATH, "backup-chunks.jsonl");

interface Document {
  id: string;
  title: string;
  path: string;
  added_at: string;
  page_count: number;
  size_bytes: number;
  tags?: string[];
  metadata?: Record<string, unknown>;
}

interface Chunk {
  id: string;
  doc_id: string;
  page: number;
  chunk_index: number;
  content: string;
}

async function main() {
  console.log("=== PDF Library Recovery from JSON ===\n");
  console.log(`Database: ${DB_PATH}`);
  console.log(`Documents: ${DOCS_FILE}`);
  console.log(`Chunks: ${CHUNKS_FILE}\n`);

  // Initialize fresh database
  console.log("Initializing database...");
  const db = new PGlite(DB_PATH, { extensions: { vector } });
  await db.waitReady;

  // Create schema
  console.log("Creating schema...");
  await db.exec(`
    CREATE EXTENSION IF NOT EXISTS vector;

    CREATE TABLE IF NOT EXISTS documents (
      id TEXT PRIMARY KEY,
      title TEXT NOT NULL,
      path TEXT NOT NULL UNIQUE,
      added_at TIMESTAMPTZ NOT NULL,
      page_count INTEGER NOT NULL,
      size_bytes INTEGER NOT NULL,
      tags JSONB DEFAULT '[]',
      metadata JSONB DEFAULT '{}'
    );

    CREATE TABLE IF NOT EXISTS chunks (
      id TEXT PRIMARY KEY,
      doc_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      page INTEGER NOT NULL,
      chunk_index INTEGER NOT NULL,
      content TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS embeddings (
      chunk_id TEXT PRIMARY KEY REFERENCES chunks(id) ON DELETE CASCADE,
      embedding vector(1024) NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_chunks_doc_id ON chunks(doc_id);
    CREATE INDEX IF NOT EXISTS idx_embeddings_chunk_id ON embeddings(chunk_id);
  `);

  // Import documents
  console.log("\nImporting documents...");
  const docsJson = readFileSync(DOCS_FILE, "utf-8");
  const documents: Document[] = JSON.parse(docsJson);

  let docCount = 0;
  for (const doc of documents) {
    try {
      await db.query(
        `INSERT INTO documents (id, title, path, added_at, page_count, size_bytes, tags, metadata)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (id) DO NOTHING`,
        [
          doc.id,
          doc.title,
          doc.path,
          doc.added_at,
          doc.page_count,
          doc.size_bytes,
          JSON.stringify(doc.tags || []),
          JSON.stringify(doc.metadata || {}),
        ]
      );
      docCount++;
    } catch (e) {
      console.error(`Error importing doc ${doc.id}: ${e}`);
    }
  }
  console.log(`Imported ${docCount} documents`);

  // Import chunks (streaming for large files)
  console.log("\nImporting chunks...");
  const rl = createInterface({
    input: createReadStream(CHUNKS_FILE),
    crlfDelay: Infinity,
  });

  let chunkCount = 0;
  let errors = 0;
  const BATCH_SIZE = 100;
  let batch: Chunk[] = [];

  const flushBatch = async () => {
    if (batch.length === 0) return;

    // Use a transaction for the batch
    await db.exec("BEGIN");
    try {
      for (const chunk of batch) {
        await db.query(
          `INSERT INTO chunks (id, doc_id, page, chunk_index, content)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (id) DO NOTHING`,
          [chunk.id, chunk.doc_id, chunk.page, chunk.chunk_index, chunk.content]
        );
      }
      await db.exec("COMMIT");
      chunkCount += batch.length;
    } catch (e) {
      await db.exec("ROLLBACK");
      errors += batch.length;
      console.error(`Batch error: ${e}`);
    }
    batch = [];

    if (chunkCount % 10000 === 0) {
      console.log(`  Progress: ${chunkCount} chunks...`);
    }
  };

  for await (const line of rl) {
    if (!line.trim()) continue;
    try {
      const chunk: Chunk = JSON.parse(line);
      batch.push(chunk);

      if (batch.length >= BATCH_SIZE) {
        await flushBatch();
      }
    } catch {
      errors++;
    }
  }

  // Flush remaining
  await flushBatch();

  console.log(`Imported ${chunkCount} chunks (${errors} errors)`);

  // Force checkpoint to prevent WAL accumulation
  console.log("\nRunning CHECKPOINT...");
  await db.exec("CHECKPOINT");

  // Verify
  const docResult = await db.query<{ c: string }>(
    "SELECT COUNT(*) as c FROM documents"
  );
  const chunkResult = await db.query<{ c: string }>(
    "SELECT COUNT(*) as c FROM chunks"
  );

  console.log("\n=== Recovery Complete ===");
  console.log(`Documents: ${docResult.rows[0]?.c}`);
  console.log(`Chunks: ${chunkResult.rows[0]?.c}`);
  console.log(`Embeddings: 0 (run regenerate-embeddings.ts next)`);

  await db.close();

  console.log("\nNext steps:");
  console.log("  1. Start Ollama: ollama serve");
  console.log("  2. Pull model: ollama pull nomic-embed-text");
  console.log(
    "  3. Regenerate: OLLAMA_MODEL=nomic-embed-text bun run scripts/migration/regenerate-embeddings.ts"
  );
}

main().catch((e) => {
  console.error("Recovery failed:", e);
  process.exit(1);
});
