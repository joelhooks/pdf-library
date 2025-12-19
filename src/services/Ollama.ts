/**
 * Ollama Embedding Service
 */

import {
  Effect,
  Context,
  Layer,
  Schedule,
  Duration,
  Chunk,
  Stream,
} from "effect";
import { OllamaError, LibraryConfig } from "../types.js";

// ============================================================================
// Service Definition
// ============================================================================

export class Ollama extends Context.Tag("Ollama")<
  Ollama,
  {
    readonly embed: (text: string) => Effect.Effect<number[], OllamaError>;
    readonly embedBatch: (
      texts: string[],
      concurrency?: number
    ) => Effect.Effect<number[][], OllamaError>;
    readonly checkHealth: () => Effect.Effect<void, OllamaError>;
  }
>() {}

// ============================================================================
// Implementation
// ============================================================================

interface OllamaEmbeddingResponse {
  embedding: number[];
}

interface OllamaTagsResponse {
  models: Array<{ name: string }>;
}

/**
 * Expected embedding dimension for nomic-embed-text model
 */
const EXPECTED_EMBEDDING_DIMENSION = 1024;

/**
 * Validates embedding dimensions and values before database insert
 *
 * Prevents "different vector dimensions 1024 and 0" errors that cause
 * PGlite WAL accumulation and database corruption.
 *
 * @param embedding - The embedding vector to validate
 * @returns Effect that fails with OllamaError if invalid
 */
function validateEmbedding(
  embedding: number[]
): Effect.Effect<number[], OllamaError> {
  if (embedding.length === 0) {
    return Effect.fail(
      new OllamaError({
        reason: `Invalid embedding: dimension 0 (expected ${EXPECTED_EMBEDDING_DIMENSION})`,
      })
    );
  }

  if (embedding.length !== EXPECTED_EMBEDDING_DIMENSION) {
    return Effect.fail(
      new OllamaError({
        reason: `Invalid embedding: dimension ${embedding.length} (expected ${EXPECTED_EMBEDDING_DIMENSION})`,
      })
    );
  }

  if (embedding.some((v) => !Number.isFinite(v))) {
    return Effect.fail(
      new OllamaError({
        reason:
          "Invalid embedding: contains non-finite values (NaN or Infinity)",
      })
    );
  }

  return Effect.succeed(embedding);
}

export const OllamaLive = Layer.effect(
  Ollama,
  Effect.gen(function* () {
    const config = LibraryConfig.fromEnv();

    const embedSingle = (text: string): Effect.Effect<number[], OllamaError> =>
      Effect.gen(function* () {
        const response = yield* Effect.tryPromise({
          try: () =>
            fetch(`${config.ollamaHost}/api/embeddings`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                model: config.ollamaModel,
                prompt: text,
              }),
            }),
          catch: (e) => new OllamaError({ reason: `Connection failed: ${e}` }),
        });

        if (!response.ok) {
          const error = yield* Effect.tryPromise({
            try: () => response.text(),
            catch: () =>
              new OllamaError({ reason: "Failed to read error response" }),
          });
          return yield* Effect.fail(new OllamaError({ reason: error }));
        }

        const data = yield* Effect.tryPromise({
          try: () => response.json() as Promise<OllamaEmbeddingResponse>,
          catch: () => new OllamaError({ reason: "Invalid JSON response" }),
        });

        // Validate embedding before returning to prevent database corruption
        return yield* validateEmbedding(data.embedding);
      }).pipe(
        // Retry with exponential backoff on transient failures
        Effect.retry(
          Schedule.exponential(Duration.millis(100)).pipe(
            Schedule.compose(Schedule.recurs(3))
          )
        )
      );

    return {
      embed: embedSingle,

      embedBatch: (texts: string[], concurrency = 5) =>
        Stream.fromIterable(texts).pipe(
          Stream.mapEffect(embedSingle, { concurrency }),
          Stream.runCollect,
          Effect.map(Chunk.toArray)
        ),

      checkHealth: () =>
        Effect.gen(function* () {
          const response = yield* Effect.tryPromise({
            try: () => fetch(`${config.ollamaHost}/api/tags`),
            catch: () =>
              new OllamaError({
                reason: `Cannot connect to Ollama at ${config.ollamaHost}`,
              }),
          });

          if (!response.ok) {
            return yield* Effect.fail(
              new OllamaError({ reason: "Ollama not responding" })
            );
          }

          const data = yield* Effect.tryPromise({
            try: () => response.json() as Promise<OllamaTagsResponse>,
            catch: () =>
              new OllamaError({ reason: "Invalid response from Ollama" }),
          });

          const hasModel = data.models.some(
            (m) =>
              m.name === config.ollamaModel ||
              m.name.startsWith(`${config.ollamaModel}:`)
          );

          if (!hasModel) {
            return yield* Effect.fail(
              new OllamaError({
                reason: `Model ${config.ollamaModel} not found. Run: ollama pull ${config.ollamaModel}`,
              })
            );
          }
        }),
    };
  })
);
