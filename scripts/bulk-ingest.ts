#!/usr/bin/env -S bun run
/**
 * Bulk Ingest - Smart batch import with local LLM tagging
 *
 * Pass multiple directories, processes them all in one go.
 */

import { Effect, Layer, Logger, LogLevel } from "effect";
import { existsSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { basename, extname, join } from "node:path";
import {
  AddOptions,
  type Document,
  PDFLibrary,
  PDFLibraryLive,
} from "../src/index.js";
import { AutoTagger, AutoTaggerLive } from "../src/services/AutoTagger.js";

// ═══════════════════════════════════════════════════════════════════════════
// Config
// ═══════════════════════════════════════════════════════════════════════════

const args = process.argv.slice(2);
const directories = args.filter((a) => !a.startsWith("--"));
const tagsIdx = args.indexOf("--tags");
const manualTags =
  tagsIdx !== -1 ? args[tagsIdx + 1]?.split(",").map((t) => t.trim()) : [];
const autoTag = args.includes("--auto-tag");
const enrich = args.includes("--enrich");

if (directories.length === 0) {
  console.log(`
╔══════════════════════════════════════════════════════════════════╗
║                     📚 BULK INGEST                               ║
╠══════════════════════════════════════════════════════════════════╣
║  Usage: ./scripts/bulk-ingest.ts <dir1> [dir2] [dir3] [options]  ║
║                                                                  ║
║  Options:                                                        ║
║    --tags tag1,tag2   Manual tags for all files                  ║
║    --auto-tag         Smart tagging via llama3.2:3b              ║
║    --enrich           Full enrichment (title, summary, tags)     ║
║                                                                  ║
║  Examples:                                                       ║
║    ./scripts/bulk-ingest.ts ~/books --tags "books"               ║
║    ./scripts/bulk-ingest.ts ~/papers ~/research --auto-tag       ║
║    ./scripts/bulk-ingest.ts ~/docs --enrich                      ║
╚══════════════════════════════════════════════════════════════════╝
`);
  process.exit(1);
}

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

function discoverFiles(dir: string): string[] {
  const files: string[] = [];
  try {
    for (const entry of readdirSync(dir)) {
      const fullPath = join(dir, entry);
      try {
        const stat = statSync(fullPath);
        if (stat.isDirectory()) {
          files.push(...discoverFiles(fullPath));
        } else if (stat.isFile()) {
          const ext = extname(entry).toLowerCase();
          if ([".pdf", ".md", ".markdown"].includes(ext)) {
            files.push(fullPath);
          }
        }
      } catch {
        /* skip */
      }
    }
  } catch {
    /* skip */
  }
  return files;
}

function formatDuration(ms: number): string {
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  const h = Math.floor(m / 60);
  if (h > 0) return `${h}h ${m % 60}m`;
  if (m > 0) return `${m}m ${s % 60}s`;
  return `${s}s`;
}

function truncate(str: string, len: number): string {
  return str.length > len ? str.slice(0, len - 1) + "…" : str;
}

function progressBar(current: number, total: number, width = 30): string {
  const pct = current / total;
  const filled = Math.round(width * pct);
  const empty = width - filled;
  return `[${"█".repeat(filled)}${"░".repeat(empty)}]`;
}

// ═══════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════

const program = Effect.gen(function* () {
  const mode = enrich ? "enrich" : autoTag ? "auto-tag" : "manual";

  console.log(`
╔══════════════════════════════════════════════════════════════════╗
║  📚 BULK INGEST                                                  ║
╠══════════════════════════════════════════════════════════════════╣
║  Directories: ${String(directories.length).padEnd(48)} ║
║  Mode:        ${mode.padEnd(48)} ║
║  Tags:        ${(manualTags.join(", ") || "(none)").slice(0, 48).padEnd(48)} ║
╚══════════════════════════════════════════════════════════════════╝
`);

  // Discover all files from all directories
  process.stdout.write("  🔍 Scanning directories...");
  const allFiles: string[] = [];
  for (const dir of directories) {
    const targetDir = dir.startsWith("/") ? dir : join(process.cwd(), dir);
    if (existsSync(targetDir)) {
      allFiles.push(...discoverFiles(targetDir));
    } else {
      console.log(`\n  ⚠️  Skipping (not found): ${dir}`);
    }
  }
  console.log(` found ${allFiles.length} files\n`);

  if (allFiles.length === 0) {
    console.log("  No PDF or Markdown files found.\n");
    return;
  }

  // Services
  const library = yield* PDFLibrary;
  const tagger = autoTag || enrich ? yield* AutoTagger : null;

  // Filter already ingested
  const existingDocs = yield* library.list();
  const existingPaths = new Set(existingDocs.map((d: Document) => d.path));
  const files = allFiles.filter((f) => !existingPaths.has(f));

  if (files.length < allFiles.length) {
    console.log(
      `  ⏭️  Skipping ${allFiles.length - files.length} already ingested\n`
    );
  }

  if (files.length === 0) {
    console.log("  ✅ All files already ingested!\n");
    return;
  }

  // Process
  const startTime = Date.now();
  let succeeded = 0;
  let failed = 0;
  const failures: Array<{ path: string; error: string }> = [];

  for (let i = 0; i < files.length; i++) {
    const filePath = files[i];
    const filename = basename(filePath);
    const elapsed = Date.now() - startTime;
    const avgTime = i > 0 ? elapsed / i : 3000;
    const eta = formatDuration((files.length - i) * avgTime);
    const pct = Math.round(((i + 1) / files.length) * 100);

    // Progress line
    process.stdout.write(
      `\r  ${progressBar(i + 1, files.length)} ${String(pct).padStart(
        3
      )}% │ ETA ${eta.padEnd(8)} │ ${truncate(filename, 35).padEnd(35)}`
    );

    // Build tags
    let fileTags = [...manualTags];
    let title: string | undefined;

    // Smart tagging
    if (tagger) {
      const ext = extname(filePath).toLowerCase();
      let content: string | undefined;

      if (ext === ".md" || ext === ".markdown") {
        const readResult = yield* Effect.either(
          Effect.tryPromise(() => Bun.file(filePath).text())
        );
        if (readResult._tag === "Right") {
          content = readResult.right;
        }
      }

      const tagResult = yield* Effect.either(
        Effect.gen(function* () {
          if (enrich && content) {
            const r = yield* tagger.enrich(filePath, content, {
              basePath: directories[0],
            });
            return { title: r.title, tags: r.tags };
          } else {
            const r = yield* tagger.generateTags(filePath, content, {
              heuristicsOnly: !content,
              basePath: directories[0],
            });
            return { title: undefined, tags: r.allTags };
          }
        })
      );

      if (tagResult._tag === "Right") {
        title = tagResult.right.title;
        fileTags = [...fileTags, ...tagResult.right.tags];
      }
    }

    // Add document
    const result = yield* Effect.either(
      library.add(
        filePath,
        new AddOptions({
          title,
          tags: fileTags.length > 0 ? fileTags : undefined,
        })
      )
    );

    if (result._tag === "Right") {
      succeeded++;
    } else {
      failed++;
      const error = result.left;
      const errorMsg =
        error && typeof error === "object" && "message" in error
          ? String((error as { message: string }).message)
          : String(error);
      failures.push({ path: filePath, error: errorMsg });
    }

    // Checkpoint every 25
    if ((i + 1) % 25 === 0) {
      yield* Effect.either(library.checkpoint());
    }
  }

  // Final checkpoint
  yield* Effect.either(library.checkpoint());

  // Clear progress line
  process.stdout.write("\r" + " ".repeat(100) + "\r");

  // Summary
  const totalTime = formatDuration(Date.now() - startTime);
  const stats = yield* library.stats();

  console.log(`
╔══════════════════════════════════════════════════════════════════╗
║  ✅ COMPLETE                                                     ║
╠══════════════════════════════════════════════════════════════════╣
║  Time:       ${totalTime.padEnd(49)} ║
║  Succeeded:  ${String(succeeded).padEnd(49)} ║
║  Failed:     ${String(failed).padEnd(49)} ║
╠══════════════════════════════════════════════════════════════════╣
║  📊 LIBRARY STATS                                                ║
║  Documents:  ${String(stats.documents).padEnd(49)} ║
║  Chunks:     ${String(stats.chunks).padEnd(49)} ║
║  Embeddings: ${String(stats.embeddings).padEnd(49)} ║
╚══════════════════════════════════════════════════════════════════╝
`);

  // Write failures
  if (failures.length > 0) {
    const failuresLog = join(process.cwd(), "ingest-failures.log");
    const logContent = failures
      .map((f) => `${f.path}\n  ${f.error}\n`)
      .join("\n");
    writeFileSync(
      failuresLog,
      `# Failures - ${new Date().toISOString()}\n\n${logContent}`
    );
    console.log(`  ⚠️  Failures logged to: ${failuresLog}\n`);
  }
});

// Run with Effect logging suppressed (we do our own progress output)
const AppLayer = PDFLibraryLive.pipe(Layer.provideMerge(AutoTaggerLive));
Effect.runPromise(
  program.pipe(
    Effect.provide(AppLayer),
    Logger.withMinimumLogLevel(LogLevel.None)
  )
).catch(console.error);
