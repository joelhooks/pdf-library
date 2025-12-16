/**
 * Markdown Extraction Service
 */

import { Context, Effect, Layer, Schema } from "effect";
import { existsSync, readFileSync } from "node:fs";
import { LibraryConfig } from "../types.js";

// ============================================================================
// Custom Error Types
// ============================================================================

export class MarkdownNotFoundError extends Schema.TaggedError<MarkdownNotFoundError>()(
  "MarkdownNotFoundError",
  { path: Schema.String },
) {}

export class MarkdownExtractionError extends Schema.TaggedError<MarkdownExtractionError>()(
  "MarkdownExtractionError",
  { path: Schema.String, reason: Schema.String },
) {}

// ============================================================================
// Service Definition
// ============================================================================

export interface ExtractedSection {
  section: number;
  heading: string;
  text: string;
}

export interface ExtractedMarkdown {
  sections: ExtractedSection[];
  sectionCount: number;
}

export interface ProcessedChunk {
  page: number; // Using section number as "page" for consistency
  chunkIndex: number;
  content: string;
}

export class MarkdownExtractor extends Context.Tag("MarkdownExtractor")<
  MarkdownExtractor,
  {
    readonly extract: (
      path: string,
    ) => Effect.Effect<
      ExtractedMarkdown,
      MarkdownExtractionError | MarkdownNotFoundError
    >;
    readonly process: (
      path: string,
    ) => Effect.Effect<
      { pageCount: number; chunks: ProcessedChunk[] },
      MarkdownExtractionError | MarkdownNotFoundError
    >;
  }
>() {}

// ============================================================================
// Implementation
// ============================================================================

/**
 * Parse markdown into sections based on headings
 */
function parseMarkdown(content: string): ExtractedSection[] {
  const sections: ExtractedSection[] = [];
  let currentSection = 0;
  let currentHeading = "";
  let currentText = "";

  const lines = content.split("\n");
  
  for (const line of lines) {
    // Check if line is a heading (# or ##, etc.)
    const headingMatch = line.match(/^(#{1,6})\s+(.+)$/);
    
    if (headingMatch) {
      // Save previous section if it has content
      if (currentSection > 0 || currentText.trim()) {
        sections.push({
          section: currentSection || 1,
          heading: currentHeading,
          text: currentText.trim(),
        });
      }
      
      // Start new section
      currentSection = sections.length + 1;
      currentHeading = headingMatch[2];
      currentText = "";
    } else {
      // Add line to current section
      currentText += line + "\n";
    }
  }

  // Save final section
  if (currentText.trim() || currentHeading) {
    sections.push({
      section: currentSection || 1,
      heading: currentHeading,
      text: currentText.trim(),
    });
  }

  // If no headings found, treat entire document as one section
  if (sections.length === 0 && content.trim()) {
    sections.push({
      section: 1,
      heading: "",
      text: content.trim(),
    });
  }

  return sections;
}

/**
 * Chunk text with intelligent splitting
 * Preserves code blocks and handles markdown-specific structures
 */
function chunkText(
  text: string,
  chunkSize: number,
  chunkOverlap: number,
): string[] {
  const chunks: string[] = [];

  // Preserve code blocks - extract them first
  const codeBlocks: { placeholder: string; content: string }[] = [];
  const processedText = text.replace(
    /```[\s\S]*?```|`[^`]+`/g,
    (match) => {
      const placeholder = `__CODE_BLOCK_${codeBlocks.length}__`;
      codeBlocks.push({ placeholder, content: match });
      return placeholder;
    },
  );

  // Clean up text (but preserve double newlines for paragraphs)
  const cleaned = processedText
    .replace(/\s+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

  if (cleaned.length <= chunkSize) {
    // Restore code blocks
    let result = cleaned;
    codeBlocks.forEach(({ placeholder, content }) => {
      result = result.replace(placeholder, content);
    });
    return result ? [result] : [];
  }

  // Try to split on paragraph boundaries first
  const paragraphs = cleaned.split(/\n\n+/);
  let currentChunk = "";

  for (const para of paragraphs) {
    if (currentChunk.length + para.length + 2 <= chunkSize) {
      currentChunk += (currentChunk ? "\n\n" : "") + para;
    } else {
      if (currentChunk) {
        chunks.push(currentChunk);
      }

      // If paragraph itself is too long, split by sentences
      if (para.length > chunkSize) {
        const sentences = para.match(/[^.!?]+[.!?]+/g) || [para];
        currentChunk = "";

        for (const sentence of sentences) {
          if (currentChunk.length + sentence.length <= chunkSize) {
            currentChunk += sentence;
          } else {
            if (currentChunk) {
              chunks.push(currentChunk.trim());
            }
            // If sentence is still too long, hard split
            if (sentence.length > chunkSize) {
              for (
                let i = 0;
                i < sentence.length;
                i += chunkSize - chunkOverlap
              ) {
                chunks.push(sentence.slice(i, i + chunkSize).trim());
              }
              currentChunk = "";
            } else {
              currentChunk = sentence;
            }
          }
        }
      } else {
        currentChunk = para;
      }
    }
  }

  if (currentChunk) {
    chunks.push(currentChunk);
  }

  // Restore code blocks in all chunks
  const restoredChunks = chunks.map((chunk) => {
    let restored = chunk;
    codeBlocks.forEach(({ placeholder, content }) => {
      restored = restored.replace(placeholder, content);
    });
    return restored;
  });

  return restoredChunks.filter((c) => c.length > 20); // Filter tiny chunks
}

export const MarkdownExtractorLive = Layer.effect(
  MarkdownExtractor,
  Effect.gen(function* () {
    const config = LibraryConfig.fromEnv();

    return {
      extract: (path: string) =>
        Effect.gen(function* () {
          // Resolve path
          const resolvedPath = path.startsWith("~")
            ? path.replace("~", process.env.HOME || "")
            : path;

          if (!existsSync(resolvedPath)) {
            return yield* Effect.fail(
              new MarkdownNotFoundError({ path: resolvedPath }),
            );
          }

          const result = yield* Effect.try({
            try: () => {
              const content = readFileSync(resolvedPath, "utf-8");
              const sections = parseMarkdown(content);

              return {
                sections,
                sectionCount: sections.length,
              } as ExtractedMarkdown;
            },
            catch: (e) =>
              new MarkdownExtractionError({
                path: resolvedPath,
                reason: String(e),
              }),
          });

          return result;
        }),

      process: (path: string) =>
        Effect.gen(function* () {
          const resolvedPath = path.startsWith("~")
            ? path.replace("~", process.env.HOME || "")
            : path;

          if (!existsSync(resolvedPath)) {
            return yield* Effect.fail(
              new MarkdownNotFoundError({ path: resolvedPath }),
            );
          }

          const extracted = yield* Effect.try({
            try: () => {
              const content = readFileSync(resolvedPath, "utf-8");
              return parseMarkdown(content);
            },
            catch: (e) =>
              new MarkdownExtractionError({
                path: resolvedPath,
                reason: String(e),
              }),
          });

          const allChunks: ProcessedChunk[] = [];

          for (const { section, text } of extracted) {
            const sectionChunks = chunkText(
              text,
              config.chunkSize,
              config.chunkOverlap,
            );
            sectionChunks.forEach((content, chunkIndex) => {
              allChunks.push({
                page: section, // Use section number as "page"
                chunkIndex,
                content,
              });
            });
          }

          return {
            pageCount: extracted.length, // Section count as pseudo-pages
            chunks: allChunks,
          };
        }),
    };
  }),
);
