/**
 * Markdown Extraction Service
 *
 * Uses unified/remark ecosystem for proper AST-based markdown parsing.
 * Supports frontmatter extraction via gray-matter.
 */

import { Context, Effect, Layer, Schema } from "effect";
import { existsSync, readFileSync } from "node:fs";
import { unified } from "unified";
import remarkParse from "remark-parse";
import remarkFrontmatter from "remark-frontmatter";
import remarkGfm from "remark-gfm";
import { toString as mdastToString } from "mdast-util-to-string";
import matter from "gray-matter";
import type { Root, Heading, RootContent } from "mdast";
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
// Types
// ============================================================================

/**
 * Frontmatter data extracted from markdown
 */
export interface MarkdownFrontmatter {
  title?: string;
  description?: string;
  tags?: string[];
  [key: string]: unknown;
}

/**
 * A section of markdown content, typically delimited by headings
 */
export interface ExtractedSection {
  section: number;
  heading: string;
  headingLevel: number;
  text: string;
}

/**
 * Result of extracting markdown content
 */
export interface ExtractedMarkdown {
  frontmatter: MarkdownFrontmatter;
  sections: ExtractedSection[];
  sectionCount: number;
}

/**
 * A chunk of content ready for embedding
 */
export interface ProcessedChunk {
  page: number; // Using section number as "page" for consistency with PDF model
  chunkIndex: number;
  content: string;
}

// ============================================================================
// Service Definition
// ============================================================================

export class MarkdownExtractor extends Context.Tag("MarkdownExtractor")<
  MarkdownExtractor,
  {
    /**
     * Extract markdown into sections with frontmatter
     */
    readonly extract: (
      path: string,
    ) => Effect.Effect<
      ExtractedMarkdown,
      MarkdownExtractionError | MarkdownNotFoundError
    >;

    /**
     * Process markdown into chunks suitable for embedding
     */
    readonly process: (
      path: string,
    ) => Effect.Effect<
      {
        pageCount: number;
        chunks: ProcessedChunk[];
        frontmatter: MarkdownFrontmatter;
      },
      MarkdownExtractionError | MarkdownNotFoundError
    >;

    /**
     * Extract frontmatter only (fast path for title extraction)
     */
    readonly extractFrontmatter: (
      path: string,
    ) => Effect.Effect<
      MarkdownFrontmatter,
      MarkdownExtractionError | MarkdownNotFoundError
    >;
  }
>() {}

// ============================================================================
// Implementation
// ============================================================================

/**
 * Create unified processor with remark plugins
 */
function createProcessor() {
  return unified()
    .use(remarkParse)
    .use(remarkFrontmatter, ["yaml", "toml"])
    .use(remarkGfm);
}

/**
 * Check if a node is a frontmatter node (yaml or toml)
 */
function isFrontmatterNode(node: RootContent): boolean {
  return node.type === "yaml" || (node as { type: string }).type === "toml";
}

/**
 * Parse markdown content into AST and extract sections
 */
function parseMarkdownAST(content: string): ExtractedSection[] {
  const processor = createProcessor();
  const tree = processor.parse(content) as Root;

  const sections: ExtractedSection[] = [];
  let currentSection = 0;
  let currentHeading = "";
  let currentHeadingLevel = 0;
  let currentContent: RootContent[] = [];

  /**
   * Flush current section to results
   */
  function flushSection() {
    if (currentContent.length > 0 || currentHeading) {
      const text = currentContent
        .map((node) => mdastToString(node))
        .join("\n\n")
        .trim();

      if (text || currentHeading) {
        sections.push({
          section: currentSection || 1,
          heading: currentHeading,
          headingLevel: currentHeadingLevel,
          text,
        });
      }
    }
  }

  // Walk through top-level children
  for (const node of tree.children) {
    // Skip frontmatter nodes (handled separately by gray-matter)
    if (isFrontmatterNode(node)) {
      continue;
    }

    if (node.type === "heading") {
      // Flush previous section
      flushSection();

      // Start new section
      currentSection = sections.length + 1;
      currentHeading = mdastToString(node);
      currentHeadingLevel = (node as Heading).depth;
      currentContent = [];
    } else {
      // Add to current section content
      currentContent.push(node);
    }
  }

  // Flush final section
  flushSection();

  // If no sections found, treat entire document as one section
  if (sections.length === 0 && content.trim()) {
    // Remove frontmatter for the fallback case
    const { content: bodyContent } = matter(content);
    sections.push({
      section: 1,
      heading: "",
      headingLevel: 0,
      text: bodyContent.trim(),
    });
  }

  return sections;
}

/**
 * Extract frontmatter using gray-matter
 */
function extractFrontmatterData(content: string): MarkdownFrontmatter {
  try {
    const { data } = matter(content);
    return {
      title: typeof data.title === "string" ? data.title : undefined,
      description:
        typeof data.description === "string" ? data.description : undefined,
      tags: Array.isArray(data.tags)
        ? data.tags.filter((t): t is string => typeof t === "string")
        : undefined,
      ...data,
    };
  } catch {
    return {};
  }
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
  const processedText = text.replace(/```[\s\S]*?```|`[^`]+`/g, (match) => {
    const placeholder = `__CODE_BLOCK_${codeBlocks.length}__`;
    codeBlocks.push({ placeholder, content: match });
    return placeholder;
  });

  // Clean up excessive whitespace while preserving paragraph breaks
  const cleaned = processedText
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

  if (cleaned.length <= chunkSize) {
    // Restore code blocks
    let result = cleaned;
    for (const { placeholder, content } of codeBlocks) {
      result = result.replace(placeholder, content);
    }
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
            // If sentence is still too long, hard split with overlap
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
    for (const { placeholder, content } of codeBlocks) {
      restored = restored.replace(placeholder, content);
    }
    return restored;
  });

  // Filter tiny chunks (less than 20 chars)
  return restoredChunks.filter((c) => c.length > 20);
}

/**
 * Resolve path with home directory expansion
 */
function resolvePath(path: string): string {
  return path.startsWith("~")
    ? path.replace("~", process.env.HOME || "")
    : path;
}

// ============================================================================
// Service Layer
// ============================================================================

export const MarkdownExtractorLive = Layer.effect(
  MarkdownExtractor,
  Effect.gen(function* () {
    const config = LibraryConfig.fromEnv();

    return {
      extract: (path: string) =>
        Effect.gen(function* () {
          const resolvedPath = resolvePath(path);

          if (!existsSync(resolvedPath)) {
            return yield* Effect.fail(
              new MarkdownNotFoundError({ path: resolvedPath }),
            );
          }

          const result = yield* Effect.try({
            try: () => {
              const content = readFileSync(resolvedPath, "utf-8");
              const frontmatter = extractFrontmatterData(content);
              const sections = parseMarkdownAST(content);

              return {
                frontmatter,
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
          const resolvedPath = resolvePath(path);

          if (!existsSync(resolvedPath)) {
            return yield* Effect.fail(
              new MarkdownNotFoundError({ path: resolvedPath }),
            );
          }

          const { frontmatter, sections } = yield* Effect.try({
            try: () => {
              const content = readFileSync(resolvedPath, "utf-8");
              return {
                frontmatter: extractFrontmatterData(content),
                sections: parseMarkdownAST(content),
              };
            },
            catch: (e) =>
              new MarkdownExtractionError({
                path: resolvedPath,
                reason: String(e),
              }),
          });

          const allChunks: ProcessedChunk[] = [];

          for (const { section, heading, text } of sections) {
            // Include heading in chunk content for better context
            const sectionContent = heading ? `# ${heading}\n\n${text}` : text;
            const sectionChunks = chunkText(
              sectionContent,
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
            pageCount: sections.length, // Section count as pseudo-pages
            chunks: allChunks,
            frontmatter,
          };
        }),

      extractFrontmatter: (path: string) =>
        Effect.gen(function* () {
          const resolvedPath = resolvePath(path);

          if (!existsSync(resolvedPath)) {
            return yield* Effect.fail(
              new MarkdownNotFoundError({ path: resolvedPath }),
            );
          }

          const result = yield* Effect.try({
            try: () => {
              const content = readFileSync(resolvedPath, "utf-8");
              return extractFrontmatterData(content);
            },
            catch: (e) =>
              new MarkdownExtractionError({
                path: resolvedPath,
                reason: String(e),
              }),
          });

          return result;
        }),
    };
  }),
);
