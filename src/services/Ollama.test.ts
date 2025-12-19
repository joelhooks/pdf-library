/**
 * Ollama Service Unit Tests
 *
 * Focus: Embedding dimension validation
 * Context: PGlite WAL accumulation was triggered by "different vector dimensions 1024 and 0" error
 * Goal: Validate embeddings BEFORE they reach the database
 */

import { Effect, Either } from "effect";
import { describe, expect, test } from "bun:test";
import { OllamaError } from "../types.js";

// ============================================================================
// Embedding Dimension Validation Tests (TDD - RED phase)
// ============================================================================

describe("Ollama Embedding Validation", () => {
  const EXPECTED_DIMENSION = 1024; // nomic-embed-text model dimension

  /**
   * Validation function to be implemented in Ollama.ts
   * For now, defined here to make tests concrete
   */
  function validateEmbedding(
    embedding: number[]
  ): Either.Either<number[], OllamaError> {
    // This should be moved to Ollama.ts in GREEN phase
    // For now, we're testing the logic we want to implement

    if (embedding.length === 0) {
      return Either.left(
        new OllamaError({
          reason: `Invalid embedding: dimension 0 (expected ${EXPECTED_DIMENSION})`,
        })
      );
    }

    if (embedding.length !== EXPECTED_DIMENSION) {
      return Either.left(
        new OllamaError({
          reason: `Invalid embedding: dimension ${embedding.length} (expected ${EXPECTED_DIMENSION})`,
        })
      );
    }

    if (embedding.some((v) => !Number.isFinite(v))) {
      return Either.left(
        new OllamaError({
          reason:
            "Invalid embedding: contains non-finite values (NaN or Infinity)",
        })
      );
    }

    return Either.right(embedding);
  }

  test("rejects empty embedding (dimension 0)", () => {
    const emptyEmbedding: number[] = [];
    const result = validateEmbedding(emptyEmbedding);

    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect(result.left).toBeInstanceOf(OllamaError);
      expect(result.left.reason).toContain("dimension 0");
      expect(result.left.reason).toContain("expected 1024");
    }
  });

  test("rejects embedding with wrong dimension", () => {
    const wrongDimensionEmbedding = new Array(512).fill(0.1); // Wrong: 512 instead of 1024
    const result = validateEmbedding(wrongDimensionEmbedding);

    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect(result.left).toBeInstanceOf(OllamaError);
      expect(result.left.reason).toContain("dimension 512");
      expect(result.left.reason).toContain("expected 1024");
    }
  });

  test("accepts valid embedding with correct dimension", () => {
    const validEmbedding = new Array(EXPECTED_DIMENSION).fill(0.1);
    const result = validateEmbedding(validEmbedding);

    expect(Either.isRight(result)).toBe(true);
    if (Either.isRight(result)) {
      expect(result.right.length).toBe(EXPECTED_DIMENSION);
    }
  });

  test("validates embedding array contains numbers", () => {
    const nanEmbedding = new Array(EXPECTED_DIMENSION).fill(NaN);
    const result = validateEmbedding(nanEmbedding);

    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect(result.left).toBeInstanceOf(OllamaError);
      expect(result.left.reason).toContain("non-finite");
    }
  });

  test("rejects embedding with mixed NaN values", () => {
    const mixedEmbedding = new Array(EXPECTED_DIMENSION).fill(0.1);
    mixedEmbedding[100] = NaN; // Inject NaN
    const result = validateEmbedding(mixedEmbedding);

    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect(result.left.reason).toContain("non-finite");
    }
  });

  test("rejects embedding with Infinity values", () => {
    const infinityEmbedding = new Array(EXPECTED_DIMENSION).fill(0.1);
    infinityEmbedding[500] = Infinity; // Inject Infinity
    const result = validateEmbedding(infinityEmbedding);

    expect(Either.isLeft(result)).toBe(true);
    if (Either.isLeft(result)) {
      expect(result.left.reason).toContain("non-finite");
    }
  });
});
