#!/usr/bin/env -S bun run
/**
 * Bulk Ingest - Batch import with LLM-powered enrichment
 *
 * Uses taxonomy concepts for smart tagging via LLM.
 * Extracts text from PDFs before enrichment.
 */

import { Effect, Layer, Logger, LogLevel } from "effect";
import { existsSync, readdirSync, statSync } from "node:fs";
import { basename, extname, join } from "node:path";
import {
  AddOptions,
  type Document,
  PDFLibrary,
  PDFLibraryLive,
} from "../src/index.js";
import { AutoTagger, AutoTaggerLive } from "../src/services/AutoTagger.js";
import {
  PDFExtractor,
  PDFExtractorLive,
} from "../src/services/PDFExtractor.js";
import {
  MarkdownExtractor,
  MarkdownExtractorLive,
} from "../src/services/MarkdownExtractor.js";
import {
  TaxonomyService,
  TaxonomyServiceImpl,
} from "../src/services/TaxonomyService.js";
import { LibraryConfig } from "../src/types.js";

// ============================================================================
// CLI Parsing
// ============================================================================

const args = process.argv.slice(2);
const directories = args.filter((a) => !a.startsWith("--"));
const tagsIdx = args.indexOf("--tags");
const manualTags =
  tagsIdx !== -1 ? args[tagsIdx + 1]?.split(",").map((t) => t.trim()) : [];
const autoTag = args.includes("--auto-tag");
const enrich = args.includes("--enrich");
const verbose = args.includes("--verbose");

if (directories.length === 0) {
  console.log(`
┌─────────────────────────────────────────┐
│         📚 Bulk Ingest                  │
└─────────────────────────────────────────┘

Usage: ./scripts/bulk-ingest.ts <dir1> [dir2] [options]

Options:
  --tags tag1,tag2   Manual tags for all files
  --auto-tag         Smart tagging (heuristics + light LLM)
  --enrich           Full enrichment (LLM extracts title/summary/concepts)
  --verbose          Show detailed logging

Examples:
  ./scripts/bulk-ingest.ts ~/books --tags "books"
  ./scripts/bulk-ingest.ts ~/papers --auto-tag
  ./scripts/bulk-ingest.ts ~/docs --enrich
`);
  process.exit(1);
}

// ============================================================================
// Helpers
// ============================================================================

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
        /* skip unreadable */
      }
    }
  } catch {
    /* skip unreadable dir */
  }
  return files;
}

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + "…" : s;
}

function formatDuration(ms: number): string {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m ${s % 60}s`;
}

const box = {
  h: "─",
  v: "│",
  tl: "┌",
  tr: "┐",
  bl: "└",
  br: "┘",
  t: "├",
  b: "└",
};

// ============================================================================
// Main Program
// ============================================================================

const program = Effect.gen(function* () {
  const mode = enrich ? "enrich" : autoTag ? "auto-tag" : "manual";

  console.log(`
┌─────────────────────────────────────────┐
│         📚 Bulk Ingest                  │
├─────────────────────────────────────────┤
│  Mode: ${mode.padEnd(32)}│
│  Dirs: ${directories.length.toString().padEnd(32)}│
${
  manualTags.length
    ? `│  Tags: ${truncate(manualTags.join(", "), 31).padEnd(32)}│\n`
    : ""
}└─────────────────────────────────────────┘
`);

  // Discover files
  console.log("📂 Scanning directories...");
  const allFiles: string[] = [];
  for (const dir of directories) {
    const targetDir = dir.startsWith("/") ? dir : join(process.cwd(), dir);
    if (existsSync(targetDir)) {
      const found = discoverFiles(targetDir);
      console.log(`   ${box.t} ${basename(dir)}: ${found.length} files`);
      allFiles.push(...found);
    } else {
      console.log(`   ${box.t} ${basename(dir)}: ❌ NOT FOUND`);
    }
  }
  console.log(`   ${box.b} Total: ${allFiles.length} files\n`);

  if (allFiles.length === 0) {
    console.log("Nothing to do.");
    return;
  }

  const library = yield* PDFLibrary;
  const tagger = autoTag || enrich ? yield* AutoTagger : null;
  const pdfExtractor = yield* PDFExtractor;
  const _mdExtractor = yield* MarkdownExtractor;
  const taxonomy = yield* TaxonomyService;

  // Load taxonomy concepts for smart tagging
  let availableConcepts: Array<{
    id: string;
    prefLabel: string;
    altLabels: string[];
  }> = [];

  if (enrich) {
    console.log("🧠 Loading taxonomy concepts...");
    const concepts = yield* taxonomy.listConcepts();
    availableConcepts = concepts.map((c) => ({
      id: c.id,
      prefLabel: c.prefLabel,
      altLabels: c.altLabels,
    }));
    console.log(`   Found ${availableConcepts.length} concepts\n`);
  }

  // Filter already ingested
  const existingDocs = yield* library.list();
  const existingPaths = new Set(existingDocs.map((d: Document) => d.path));
  const files = allFiles.filter((f) => !existingPaths.has(f));

  if (allFiles.length - files.length > 0) {
    console.log(
      `⏭️  Skipping ${allFiles.length - files.length} already-ingested files`
    );
  }
  console.log(`📥 Processing ${files.length} files\n`);

  if (files.length === 0) {
    console.log("Nothing to do.");
    return;
  }

  let succeeded = 0;
  let failed = 0;
  const startTime = Date.now();

  for (let i = 0; i < files.length; i++) {
    const filePath = files[i];
    const filename = basename(filePath);
    const ext = extname(filePath).toLowerCase();
    const pct = Math.round(((i + 1) / files.length) * 100);
    const fileStart = Date.now();

    // Progress header
    console.log(`\n${box.tl}${"─".repeat(50)}${box.tr}`);
    console.log(
      `${box.v} [${i + 1}/${files.length}] ${pct}% ${truncate(
        filename,
        35
      ).padEnd(35)} ${box.v}`
    );
    console.log(`${box.bl}${"─".repeat(50)}${box.br}`);

    let fileTags = [...manualTags];
    let title: string | undefined;

    // Extract content for enrichment
    let content: string | undefined;

    if (ext === ".md" || ext === ".markdown") {
      // Markdown: read file directly
      const readResult = yield* Effect.either(
        Effect.tryPromise(() => Bun.file(filePath).text())
      );
      if (readResult._tag === "Right") {
        content = readResult.right;
      }
    } else if (ext === ".pdf") {
      // PDF: extract text using PDFExtractor
      if (enrich || autoTag) {
        console.log("   📄 Extracting PDF text...");
        const extractResult = yield* Effect.either(
          pdfExtractor.extract(filePath)
        );
        if (extractResult._tag === "Right") {
          // Combine first N pages of text for enrichment (don't need entire doc)
          const pages = extractResult.right.pages.slice(0, 10);
          content = pages.map((p) => p.text).join("\n\n");
          if (content.length > 8000) {
            content = content.slice(0, 8000);
          }
        } else {
          console.log("   ⚠️  PDF extraction failed, using filename only");
        }
      }
    }

    if (tagger && (enrich || autoTag)) {
      if (enrich && content) {
        // Full enrichment with LLM
        console.log("   🔍 Enriching with LLM...");
        const enrichResult = yield* Effect.either(
          tagger.enrich(filePath, content, {
            basePath: directories[0],
            availableConcepts,
          })
        );

        if (enrichResult._tag === "Right") {
          const r = enrichResult.right;
          title = r.title;
          fileTags = [...fileTags, ...r.tags];

          console.log(`   📝 Title:    ${truncate(r.title, 45)}`);
          if (r.author) console.log(`   👤 Author:   ${r.author}`);
          console.log(`   📁 Type:     ${r.documentType}`);
          console.log(`   📂 Category: ${r.category}`);
          console.log(
            `   🏷️  Tags:     ${r.tags.slice(0, 5).join(", ")}${
              r.tags.length > 5 ? ` (+${r.tags.length - 5})` : ""
            }`
          );
          if (r.concepts && r.concepts.length > 0) {
            console.log(
              `   🧠 Concepts: ${r.concepts.slice(0, 3).join(", ")}${
                r.concepts.length > 3 ? ` (+${r.concepts.length - 3})` : ""
              }`
            );
          }
          if (r.proposedConcepts && r.proposedConcepts.length > 0) {
            console.log(
              `   💡 Proposed: ${r.proposedConcepts
                .map((c) => c.prefLabel)
                .slice(0, 2)
                .join(", ")}`
            );
          }
          if (r.summary) {
            console.log(`   📄 Summary:  ${truncate(r.summary, 60)}`);
          }
          console.log(
            `   🤖 Provider: ${r.provider} (${Math.round(
              r.confidence * 100
            )}% confidence)`
          );
        } else {
          console.log("   ⚠️  Enrichment failed, falling back to heuristics");
          const tagResult = yield* Effect.either(
            tagger.generateTags(filePath, content, {
              heuristicsOnly: true,
              basePath: directories[0],
            })
          );
          if (tagResult._tag === "Right") {
            fileTags = [...fileTags, ...tagResult.right.allTags];
            console.log(`   🏷️  Tags:   ${fileTags.slice(0, 6).join(", ")}`);
          }
        }
      } else if (autoTag) {
        // Auto-tag only (lighter weight)
        const tagResult = yield* Effect.either(
          tagger.generateTags(filePath, content, {
            heuristicsOnly: !content,
            basePath: directories[0],
          })
        );

        if (tagResult._tag === "Right") {
          fileTags = [...fileTags, ...tagResult.right.allTags];
          if (tagResult.right.author) {
            console.log(`   👤 Author: ${tagResult.right.author}`);
          }
          console.log(
            `   🏷️  Tags:   ${fileTags.slice(0, 6).join(", ")}${
              fileTags.length > 6 ? ` (+${fileTags.length - 6})` : ""
            }`
          );
        } else {
          console.log("   ⚠️  Tag generation failed");
        }
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

    const elapsed = Date.now() - fileStart;

    if (result._tag === "Right") {
      succeeded++;
      const doc = result.right;
      console.log(
        `   ✅ Added: ${doc.id} (${doc.pageCount} pages, ${formatDuration(
          elapsed
        )})`
      );
    } else {
      failed++;
      const err = result.left;
      const msg =
        err && typeof err === "object" && "message" in err
          ? (err as { message: string }).message
          : String(err);
      console.log(`   ❌ Failed: ${truncate(msg, 50)}`);
    }

    // Checkpoint every 10
    if ((i + 1) % 10 === 0) {
      yield* Effect.either(library.checkpoint());
      const stats = yield* library.stats();
      console.log(
        `\n   💾 Checkpoint: ${stats.documents} docs, ${stats.embeddings} embeddings`
      );
    }
  }

  // Final checkpoint
  yield* Effect.either(library.checkpoint());

  const totalElapsed = Date.now() - startTime;
  const stats = yield* library.stats();

  console.log(`
┌─────────────────────────────────────────┐
│              ✨ Complete                │
├─────────────────────────────────────────┤
│  Time:       ${formatDuration(totalElapsed).padEnd(27)}│
│  Succeeded:  ${succeeded.toString().padEnd(27)}│
│  Failed:     ${failed.toString().padEnd(27)}│
├─────────────────────────────────────────┤
│  Documents:  ${stats.documents.toString().padEnd(27)}│
│  Chunks:     ${stats.chunks.toString().padEnd(27)}│
│  Embeddings: ${stats.embeddings.toString().padEnd(27)}│
└─────────────────────────────────────────┘
`);
});

// ============================================================================
// Layer Setup
// ============================================================================

const config = LibraryConfig.fromEnv();
const taxonomyLayer = TaxonomyServiceImpl.make({
  url: `file:${config.dbPath}`,
});

const AppLayer = PDFLibraryLive.pipe(
  Layer.provideMerge(AutoTaggerLive),
  Layer.provideMerge(PDFExtractorLive),
  Layer.provideMerge(MarkdownExtractorLive),
  Layer.provideMerge(taxonomyLayer)
);

// Suppress Effect logging unless verbose
const logLayer = verbose
  ? Logger.pretty
  : Logger.minimumLogLevel(LogLevel.Warning);

Effect.runPromise(
  program.pipe(Effect.provide(AppLayer), Effect.provide(logLayer))
).catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
