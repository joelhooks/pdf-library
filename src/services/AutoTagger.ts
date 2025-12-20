/**
 * AutoTagger - Intelligent document enrichment
 *
 * Enriches documents with:
 * - Clean, properly formatted titles
 * - Author extraction
 * - Semantic tags and categories
 * - Brief summaries
 * - Document type classification
 *
 * Strategy: Local LLM first (Ollama), fallback to Anthropic Haiku
 */

import { anthropic } from "@ai-sdk/anthropic";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";
import { generateObject, generateText } from "ai";
import { Context, Effect, Layer } from "effect";
import { z } from "zod";

// ============================================================================
// Types
// ============================================================================

/** LLM provider options */
export type LLMProvider = "ollama" | "anthropic";

/** Document type classification */
export type DocumentType =
  | "book"
  | "paper"
  | "tutorial"
  | "reference"
  | "guide"
  | "article"
  | "report"
  | "presentation"
  | "notes"
  | "other";

/** Taxonomy concept (minimal interface for AutoTagger) */
export interface TaxonomyConcept {
  id: string;
  prefLabel: string;
  altLabels: string[];
}

/** Proposed new concept from LLM */
export interface ProposedConcept {
  id: string;
  prefLabel: string;
  altLabels?: string[];
  definition?: string;
}

/** Full enrichment result */
export interface EnrichmentResult {
  /** Clean, properly formatted title */
  title: string;
  /** Author name(s) if detected */
  author?: string;
  /** 2-3 sentence summary */
  summary: string;
  /** Document type classification */
  documentType: DocumentType;
  /** Primary category */
  category: string;
  /** Semantic tags (5-10) - DEPRECATED: use concepts instead */
  tags: string[];
  /** Matched concept IDs from taxonomy */
  concepts: string[];
  /** Proposed new concepts to add to taxonomy */
  proposedConcepts?: ProposedConcept[];
  /** Confidence score 0-1 */
  confidence: number;
  /** Which provider was used */
  provider: LLMProvider;
}

/** Lightweight tag-only result */
export interface TagResult {
  /** Tags extracted from path */
  pathTags: string[];
  /** Tags extracted from filename */
  filenameTags: string[];
  /** Tags from content analysis */
  contentTags: string[];
  /** Tags from LLM (if used) */
  llmTags: string[];
  /** All tags combined */
  allTags: string[];
  /** Author if detected */
  author?: string;
  /** Category if detected */
  category?: string;
}

/** Options for enrichment */
export interface EnrichmentOptions {
  /** Preferred LLM provider (default: ollama, falls back to anthropic) */
  provider?: LLMProvider;
  /** Specific model to use (overrides provider default) */
  model?: string;
  /** Skip LLM entirely, use heuristics only */
  heuristicsOnly?: boolean;
  /** Base path to strip from path-based tags */
  basePath?: string;
  /** Available taxonomy concepts for concept-based tagging */
  availableConcepts?: TaxonomyConcept[];
}

// ============================================================================
// Constants
// ============================================================================

/** Default models per provider */
const DEFAULT_MODELS: Record<LLMProvider, string> = {
  ollama: "llama3.2:3b",
  anthropic: "claude-3-5-haiku-latest",
};

/** Ollama base URL */
const OLLAMA_BASE_URL = process.env.OLLAMA_HOST || "http://localhost:11434";

/** Stop words for keyword extraction */
const STOP_WORDS = new Set([
  "a",
  "an",
  "the",
  "and",
  "or",
  "but",
  "in",
  "on",
  "at",
  "to",
  "for",
  "of",
  "with",
  "by",
  "from",
  "as",
  "is",
  "was",
  "are",
  "were",
  "been",
  "be",
  "have",
  "has",
  "had",
  "do",
  "does",
  "did",
  "will",
  "would",
  "could",
  "should",
  "may",
  "might",
  "must",
  "shall",
  "can",
  "this",
  "that",
  "these",
  "those",
  "i",
  "you",
  "he",
  "she",
  "it",
  "we",
  "they",
  "what",
  "which",
  "who",
  "where",
  "when",
  "why",
  "how",
  "all",
  "each",
  "every",
  "both",
  "few",
  "more",
  "most",
  "other",
  "some",
  "no",
  "not",
  "only",
  "same",
  "so",
  "than",
  "too",
  "very",
  "just",
  "also",
  "now",
  "here",
  "there",
  "then",
  "if",
  "about",
  "into",
  "through",
  "during",
  "before",
  "after",
  "above",
  "below",
  "up",
  "down",
  "out",
  "off",
  "over",
  "under",
  "again",
  "any",
  "pdf",
  "epub",
  "doc",
  "file",
  "document",
  "page",
  "pages",
  "chapter",
  "book",
  "ebook",
  "download",
  "copy",
  "version",
  "new",
  "first",
  "last",
  "good",
  "best",
  "free",
]);

/** Patterns to ignore in path segments */
const IGNORE_PATH_PATTERNS = [
  /^\d+$/, // Pure numbers
  /^[a-f0-9-]{36}$/i, // UUIDs
  /^(downloads?|documents?|files?|temp|tmp|cache)$/i,
  /^(users?|home|library|mobile documents)$/i,
  /^[._]/, // Hidden files/folders
  /^com\.[a-z]+\.[a-z]+$/i, // Bundle IDs
  /^3l68kqb4hg/i, // iCloud container IDs
];

/** Patterns to extract author from filename */
const AUTHOR_PATTERNS = [
  /[-–—]\s*([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?(?:[A-Z][a-z]+)?)\s*\.(?:pdf|epub|md)$/i,
  /by\s+([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?(?:[A-Z][a-z]+)?)/i,
  /\(([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?(?:[A-Z][a-z]+)?)\)\s*\.(?:pdf|epub|md)$/i,
];

// ============================================================================
// Schemas
// ============================================================================

/** Schema for proposed concepts */
const ProposedConceptSchema = z.object({
  id: z.string().describe("Suggested concept ID (e.g., 'programming/rust')"),
  prefLabel: z.string().describe("Preferred label"),
  altLabels: z
    .array(z.string())
    .optional()
    .describe("Alternative labels/aliases"),
  definition: z.string().optional().describe("Definition/description"),
});

/** Schema for full enrichment */
const EnrichmentSchema = z.object({
  title: z.string().describe("Clean, properly formatted document title"),
  author: z.string().optional().describe("Author name(s) if identifiable"),
  summary: z.string().describe("2-3 sentence summary of the document"),
  documentType: z
    .enum([
      "book",
      "paper",
      "tutorial",
      "reference",
      "guide",
      "article",
      "report",
      "presentation",
      "notes",
      "other",
    ])
    .describe("Type of document"),
  category: z
    .string()
    .describe("Primary category (e.g., programming, business, design)"),
  tags: z
    .array(z.string())
    .min(3)
    .max(10)
    .describe("5-10 descriptive tags (DEPRECATED: use concepts)"),
  concepts: z.array(z.string()).describe("Matched concept IDs from taxonomy"),
  proposedConcepts: z
    .array(ProposedConceptSchema)
    .optional()
    .describe("New concepts to add to taxonomy"),
});

/** Schema for lightweight tagging */
const TagSchema = z.object({
  tags: z.array(z.string()).min(3).max(7).describe("3-7 descriptive tags"),
  category: z.string().optional().describe("Primary category"),
  author: z.string().optional().describe("Author if identifiable"),
});

// ============================================================================
// Providers
// ============================================================================

/** Create Ollama provider */
const createOllamaProvider = () =>
  createOpenAICompatible({
    name: "ollama",
    baseURL: `${OLLAMA_BASE_URL}/v1`,
  });

/** Check if Ollama is available */
async function isOllamaAvailable(): Promise<boolean> {
  try {
    const response = await fetch(`${OLLAMA_BASE_URL}/api/tags`, {
      signal: AbortSignal.timeout(2000),
    });
    return response.ok;
  } catch {
    return false;
  }
}

/** Check if a specific model is available in Ollama */
async function isModelAvailable(modelName: string): Promise<boolean> {
  try {
    const response = await fetch(`${OLLAMA_BASE_URL}/api/tags`, {
      signal: AbortSignal.timeout(2000),
    });
    if (!response.ok) return false;
    const data = (await response.json()) as {
      models?: Array<{ name: string }>;
    };
    return (
      data.models?.some((m) => m.name.startsWith(modelName.split(":")[0])) ??
      false
    );
  } catch {
    return false;
  }
}

/** Get model for provider */
function getModel(provider: LLMProvider, modelName?: string) {
  const model = modelName || DEFAULT_MODELS[provider];

  if (provider === "ollama") {
    return createOllamaProvider()(model);
  }
  return anthropic(model);
}

// ============================================================================
// Heuristic Extraction (No LLM)
// ============================================================================

/**
 * Normalize a tag string
 */
function normalizeTag(tag: string): string {
  return tag
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

/**
 * Clean a filename into a proper title
 */
export function cleanTitle(filename: string): string {
  // Remove extension
  let title = filename.replace(/\.(pdf|epub|md|markdown|txt)$/i, "");

  // Remove common URL encoding artifacts
  title = decodeURIComponent(title);

  // Replace separators with spaces
  title = title.replace(/[-_+]+/g, " ");

  // Remove parenthetical content that looks like metadata
  title = title.replace(
    /\([^)]*(?:edition|ed\.|vol\.|volume|isbn)[^)]*\)/gi,
    ""
  );

  // Clean up whitespace
  title = title.replace(/\s+/g, " ").trim();

  // Title case (but preserve acronyms)
  title = title
    .split(" ")
    .map((word) => {
      if (word === word.toUpperCase() && word.length <= 4) return word; // Acronym
      if (word.length <= 2) return word.toLowerCase(); // Articles
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    })
    .join(" ");

  return title;
}

/**
 * Extract author from filename
 */
export function extractAuthor(filename: string): string | undefined {
  for (const pattern of AUTHOR_PATTERNS) {
    const match = filename.match(pattern);
    if (match) {
      return match[1].trim();
    }
  }
  return undefined;
}

/**
 * Extract tags from file path
 */
export function extractPathTags(filePath: string, basePath?: string): string[] {
  let path = filePath;

  if (basePath && path.startsWith(basePath)) {
    path = path.slice(basePath.length);
  }

  const segments = path
    .split("/")
    .filter((s) => s && !s.includes("."))
    .filter((s) => s.length >= 2)
    .filter((s) => !IGNORE_PATH_PATTERNS.some((p) => p.test(s)))
    .map(normalizeTag)
    .filter((s) => s.length >= 2);

  return [...new Set(segments)];
}

/**
 * Extract keywords from content using TF-IDF-like scoring
 */
export function extractContentKeywords(
  content: string,
  maxKeywords: number = 5
): string[] {
  const words = content
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((w) => w.length >= 4)
    .filter((w) => !STOP_WORDS.has(w))
    .filter((w) => !/^\d+$/.test(w));

  const freq = new Map<string, number>();
  for (const word of words) {
    freq.set(word, (freq.get(word) || 0) + 1);
  }

  const totalWords = words.length || 1;
  const scored = [...freq.entries()]
    .map(([word, count]) => ({
      word,
      score:
        count *
        (count / totalWords > 0.1 ? 0.5 : 1) *
        Math.min(word.length / 8, 1.5),
    }))
    .sort((a, b) => b.score - a.score);

  return scored
    .slice(0, maxKeywords)
    .map((s) => normalizeTag(s.word))
    .filter((w) => w.length >= 4);
}

/**
 * Extract tags from filename
 */
export function extractFilenameTags(filename: string): string[] {
  const name = filename.replace(/\.(pdf|epub|md|markdown|txt)$/i, "");

  const cleaned = name
    .replace(/[-_+]+/g, " ")
    .replace(/\([^)]*\)/g, " ")
    .replace(/\[[^\]]*\]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const words = cleaned
    .split(/\s+/)
    .map((w) => w.toLowerCase())
    .filter((w) => w.length >= 3)
    .filter((w) => !STOP_WORDS.has(w))
    .filter((w) => !/^\d+$/.test(w))
    .map(normalizeTag)
    .filter((w) => w.length >= 3);

  return [...new Set(words)].slice(0, 3);
}

// ============================================================================
// LLM-based Enrichment
// ============================================================================

/**
 * Parse JSON from LLM response text
 * Handles markdown code blocks, raw JSON, and common formatting issues
 */
function parseJSONFromText(text: string): unknown {
  // Try to extract JSON from markdown code block
  const codeBlockMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  const jsonText = codeBlockMatch ? codeBlockMatch[1].trim() : text;

  // Try to find JSON object in text
  const jsonMatch = jsonText.match(/\{[\s\S]*\}/);
  if (!jsonMatch) {
    throw new Error("No JSON found in response");
  }

  let cleaned = jsonMatch[0];

  // Fix common LLM JSON issues:
  // - Trailing commas before closing brackets
  cleaned = cleaned.replace(/,\s*([}\]])/g, "$1");
  // - Single quotes instead of double quotes
  cleaned = cleaned.replace(/'/g, '"');
  // - Unquoted keys
  cleaned = cleaned.replace(
    /(\{|,)\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:/g,
    '$1"$2":'
  );

  try {
    return JSON.parse(cleaned);
  } catch (e) {
    // Last resort: try to extract just the tags array
    const tagsMatch = cleaned.match(/"tags"\s*:\s*\[([\s\S]*?)\]/);
    if (tagsMatch) {
      const tags = tagsMatch[1]
        .split(",")
        .map((t) => t.trim().replace(/^["']|["']$/g, ""))
        .filter((t) => t.length > 0);
      return { tags };
    }
    throw new Error(`Failed to parse JSON: ${e}`);
  }
}

/**
 * Format concepts for LLM prompt
 */
function formatConceptsForPrompt(concepts: TaxonomyConcept[]): string {
  if (concepts.length === 0) {
    return "No taxonomy concepts available yet.";
  }

  const lines = concepts.map((c) => {
    const aliases =
      c.altLabels.length > 0 ? ` (aliases: ${c.altLabels.join(", ")})` : "";
    return `- ${c.id}: ${c.prefLabel}${aliases}`;
  });

  return `Available concepts (use these IDs when applicable):\n${lines.join(
    "\n"
  )}`;
}

/**
 * Generate full enrichment using LLM
 * Uses generateText for better compatibility with local models
 */
async function enrichWithLLM(
  filename: string,
  content: string,
  provider: LLMProvider,
  availableConcepts: TaxonomyConcept[] = [],
  model?: string
): Promise<Omit<EnrichmentResult, "provider" | "confidence">> {
  const truncatedContent = content.slice(0, 6000);
  const conceptsList = formatConceptsForPrompt(availableConcepts);

  // For Anthropic, use structured output
  if (provider === "anthropic") {
    const { object } = await generateObject({
      model: getModel(provider, model),
      schema: EnrichmentSchema,
      prompt: `Analyze this document and extract metadata for a personal knowledge library.

Filename: ${filename}

Content (excerpt):
${truncatedContent}

${conceptsList}

Extract:
- title, author (if present), summary (2-3 sentences)
- documentType (book/paper/tutorial/reference/guide/article/report/presentation/notes/other)
- category, tags (5-10 descriptive tags for backward compatibility)
- concepts: array of concept IDs from the available concepts above that match this document's topics
- proposedConcepts: if the document covers topics NOT in the taxonomy, suggest new concepts to add (with id, prefLabel, optional altLabels/definition)`,
    });

    return {
      title: object.title,
      author: object.author,
      summary: object.summary,
      documentType: object.documentType,
      category: normalizeTag(object.category),
      tags: object.tags.map(normalizeTag).filter((t) => t.length >= 2),
      concepts: object.concepts,
      proposedConcepts: object.proposedConcepts,
    };
  }

  // For local models, use generateText with JSON prompt
  const { text } = await generateText({
    model: getModel(provider, model),
    prompt: `Analyze this document and return a JSON object with metadata.

Filename: ${filename}

Content (excerpt):
${truncatedContent}

${conceptsList}

Return ONLY a JSON object with these fields:
{
  "title": "Clean document title",
  "author": "Author name or null",
  "summary": "2-3 sentence summary",
  "documentType": "book|paper|tutorial|reference|guide|article|report|presentation|notes|other",
  "category": "primary-category",
  "tags": ["tag1", "tag2", "tag3", "tag4", "tag5"],
  "concepts": ["concept-id-1", "concept-id-2"],
  "proposedConcepts": [{"id": "new/concept", "prefLabel": "New Concept", "altLabels": ["alias1"], "definition": "Description"}]
}

Rules:
- Use lowercase hyphenated tags. Be specific, avoid generic terms.
- Match existing concept IDs from the list above when applicable
- Propose new concepts if the document covers topics not in the taxonomy`,
  });

  const parsed = parseJSONFromText(text) as {
    title?: string;
    author?: string | null;
    summary?: string;
    documentType?: string;
    category?: string;
    tags?: string[];
    concepts?: string[];
    proposedConcepts?: ProposedConcept[];
  };

  return {
    title: parsed.title || cleanTitle(filename),
    author: parsed.author || undefined,
    summary: parsed.summary || "",
    documentType: (parsed.documentType as DocumentType) || "other",
    category: normalizeTag(parsed.category || "uncategorized"),
    tags: (parsed.tags || []).map(normalizeTag).filter((t) => t.length >= 2),
    concepts: parsed.concepts || [],
    proposedConcepts: parsed.proposedConcepts,
  };
}

/**
 * Generate tags only using LLM (lighter weight)
 */
async function tagWithLLM(
  filename: string,
  content: string,
  provider: LLMProvider,
  model?: string
): Promise<{ tags: string[]; category?: string; author?: string }> {
  const truncatedContent = content.slice(0, 4000);

  // For Anthropic, use structured output
  if (provider === "anthropic") {
    const { object } = await generateObject({
      model: getModel(provider, model),
      schema: TagSchema,
      prompt: `Generate tags for this document. Filename: ${filename}\n\nContent:\n${truncatedContent}`,
    });

    return {
      tags: object.tags.map(normalizeTag).filter((t) => t.length >= 2),
      category: object.category ? normalizeTag(object.category) : undefined,
      author: object.author,
    };
  }

  // For local models, use generateText with JSON prompt
  const { text } = await generateText({
    model: getModel(provider, model),
    prompt: `Generate tags for this document. Return ONLY a JSON object.

Filename: ${filename}

Content (excerpt):
${truncatedContent}

Return JSON:
{
  "tags": ["tag1", "tag2", "tag3", "tag4", "tag5"],
  "category": "primary-category",
  "author": "Author Name or null"
}

Rules:
- 3-7 specific tags, lowercase, hyphenated (e.g., "machine-learning")
- Focus on topics, technologies, domain
- Avoid generic tags like "book", "document"`,
  });

  const parsed = parseJSONFromText(text) as {
    tags?: string[];
    category?: string;
    author?: string | null;
  };

  return {
    tags: (parsed.tags || []).map(normalizeTag).filter((t) => t.length >= 2),
    category: parsed.category ? normalizeTag(parsed.category) : undefined,
    author: parsed.author || undefined,
  };
}

// ============================================================================
// Service Definition
// ============================================================================

/** Error for enrichment failures */
export class EnrichmentError {
  readonly _tag = "EnrichmentError";
  constructor(readonly message: string, readonly cause?: unknown) {}
}

/**
 * AutoTagger service interface
 */
export interface AutoTagger {
  /**
   * Full document enrichment (title, summary, tags, etc.)
   * Uses local LLM first, falls back to Anthropic
   */
  readonly enrich: (
    filePath: string,
    content: string,
    options?: EnrichmentOptions
  ) => Effect.Effect<EnrichmentResult, EnrichmentError>;

  /**
   * Lightweight tagging only
   * Combines heuristics with optional LLM enhancement
   */
  readonly generateTags: (
    filePath: string,
    content?: string,
    options?: EnrichmentOptions
  ) => Effect.Effect<TagResult, EnrichmentError>;

  /**
   * Check if local LLM (Ollama) is available
   */
  readonly isLocalAvailable: () => Effect.Effect<boolean>;
}

export const AutoTagger = Context.GenericTag<AutoTagger>("AutoTagger");

/**
 * Create the AutoTagger service
 */
export const AutoTaggerLive = Layer.succeed(
  AutoTagger,
  AutoTagger.of({
    enrich: (filePath: string, content: string, options?: EnrichmentOptions) =>
      Effect.gen(function* () {
        const filename = filePath.split("/").pop() || "";
        const opts = options || {};
        const availableConcepts = opts.availableConcepts || [];

        // If heuristics only, build from extraction
        if (opts.heuristicsOnly) {
          const pathTags = extractPathTags(filePath, opts.basePath);
          const filenameTags = extractFilenameTags(filename);
          const contentTags = extractContentKeywords(content, 5);

          return {
            title: cleanTitle(filename),
            author: extractAuthor(filename),
            summary: content.slice(0, 200).replace(/\s+/g, " ").trim() + "...",
            documentType: "other" as DocumentType,
            category: pathTags[0] || "uncategorized",
            tags: [
              ...new Set([...pathTags, ...filenameTags, ...contentTags]),
            ].slice(0, 10),
            concepts: [],
            confidence: 0.3,
            provider: "ollama" as LLMProvider, // Placeholder
          };
        }

        // Try local LLM first
        let provider: LLMProvider = opts.provider || "ollama";
        let model = opts.model;

        if (provider === "ollama") {
          const available = yield* Effect.promise(() => isOllamaAvailable());
          if (!available) {
            console.warn("Ollama not available, falling back to Anthropic");
            provider = "anthropic";
            model = undefined;
          } else if (!model) {
            // Check if default model is available
            const modelAvailable = yield* Effect.promise(() =>
              isModelAvailable(DEFAULT_MODELS.ollama)
            );
            if (!modelAvailable) {
              console.warn(
                `Model ${DEFAULT_MODELS.ollama} not available, falling back to Anthropic`
              );
              provider = "anthropic";
            }
          }
        }

        // Run enrichment with available concepts
        const result = yield* Effect.tryPromise({
          try: () =>
            enrichWithLLM(
              filename,
              content,
              provider,
              availableConcepts,
              model
            ),
          catch: (error) =>
            new EnrichmentError(
              `Enrichment failed: ${
                error instanceof Error ? error.message : String(error)
              }`,
              error
            ),
        });

        return {
          ...result,
          confidence: provider === "ollama" ? 0.7 : 0.9,
          provider,
        };
      }),

    generateTags: (
      filePath: string,
      content?: string,
      options?: EnrichmentOptions
    ) =>
      Effect.gen(function* () {
        const filename = filePath.split("/").pop() || "";
        const opts = options || {};

        // Always extract heuristic tags
        const pathTags = extractPathTags(filePath, opts.basePath);
        const filenameTags = extractFilenameTags(filename);
        const contentTags = content ? extractContentKeywords(content, 5) : [];
        const author = extractAuthor(filename);

        let llmTags: string[] = [];
        let category: string | undefined;
        let llmAuthor: string | undefined;

        // Add LLM tags if not heuristics-only and we have content
        if (!opts.heuristicsOnly && content) {
          let provider: LLMProvider = opts.provider || "ollama";
          let model = opts.model;

          if (provider === "ollama") {
            const available = yield* Effect.promise(() => isOllamaAvailable());
            if (!available) {
              provider = "anthropic";
              model = undefined;
            }
          }

          const llmResult = yield* Effect.tryPromise({
            try: () => tagWithLLM(filename, content, provider, model),
            catch: (error) =>
              new EnrichmentError(
                `LLM tagging failed: ${
                  error instanceof Error ? error.message : String(error)
                }`,
                error
              ),
          }).pipe(
            Effect.catchAll((error) => {
              console.warn(
                "LLM tagging failed, using heuristics only:",
                error.message
              );
              return Effect.succeed({
                tags: [],
                category: undefined,
                author: undefined,
              });
            })
          );

          llmTags = llmResult.tags;
          category = llmResult.category;
          llmAuthor = llmResult.author;
        }

        // Combine all tags (LLM first for priority)
        const allTags = [
          ...new Set([
            ...llmTags,
            ...pathTags,
            ...filenameTags,
            ...contentTags,
          ]),
        ]
          .filter((t) => t.length >= 2)
          .slice(0, 10);

        return {
          pathTags,
          filenameTags,
          contentTags,
          llmTags,
          allTags,
          author: llmAuthor || author,
          category: category || pathTags[0],
        };
      }),

    isLocalAvailable: () => Effect.promise(() => isOllamaAvailable()),
  })
);
